#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "list.h"
#include "vector.h"
#include "address.h"
#include "sentinel.h"
#include "connection.h"
#include "redis.h"

#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <errno.h>

/* A redis backed sqlite3 virtual table implementation.
 * prefix.db.table:[rowid]      = hash of the row data.
 * prefix.db.table.rowid        = sequence from which rowids are generated.
 * prefix.db.table.index.rowid  = master index (zset) of rows in the table
 * prefix.db.table.indices      = master index (set) of indices on the table
 * prefix.db.table.index:x      = value index for column x
 * prefix.db.table.index:x:val  = rowid map for value val in column x */

/*
Index design.

prefix.db.table.indices
column_name


zset prefix.db.table.index:column_name
0 value_a
0 value_b
0 value_c

set prefix.db.table.index:column_name:value_a
rowid1
rowid2
... value_b, value_c


insert new row
for each indexed column 'column_name'

zadd prefix.db.table.index:column_name 0 column_value
sadd prefix.db.table.index:column_name:column_value rowid

delete row
srem prefix.db.table.index:column_name:column_value rowid
if(!exists prefix.db.table.index:column_name:column_value)
    zrem prefix.db.table.index:column_name column_value


update row
srem prefix.db.table.index:column_name:old_column_value rowid
if(!exists prefix.db.table.index:column_name:old_column_value)
    zrem prefix.db.table.index:column_name old_column_value

zadd prefix.db.table.index:column_name 0 column_value
sadd prefix.db.table.index:column_name:column_value rowid

*/

typedef struct redis_vtbl_vtab {
    sqlite3_vtab base;
    
    redis_vtbl_connection conn_spec;
    redisContext *c;
    char *key_base;
    
    vector_t columns;
} redis_vtbl_vtab;

static int redis_vtbl_create(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr);
static int redis_vtbl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr);
static int redis_vtbl_bestindex(sqlite3_vtab *pVTab, sqlite3_index_info *pIndexInfo);
static int redis_vtbl_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid);
static int redis_vtbl_disconnect(sqlite3_vtab *pVTab);
static int redis_vtbl_destroy(sqlite3_vtab *pVTab);

static void redis_vtbl_func_createindex(sqlite3_context *ctx, int argc, sqlite3_value **argv);

enum {
    CURSOR_INDEX_SCAN,
    
    CURSOR_INDEX_ROWID_EQ,
    CURSOR_INDEX_ROWID_GT,
    CURSOR_INDEX_ROWID_LT,
    CURSOR_INDEX_ROWID_GE,
    CURSOR_INDEX_ROWID_LE,
    
    CURSOR_INDEX_NAMED_EQ,
    CURSOR_INDEX_NAMED_GT,
    CURSOR_INDEX_NAMED_LT,
    CURSOR_INDEX_NAMED_GE,
    CURSOR_INDEX_NAMED_LE,
};

typedef struct redis_vtbl_cursor {
    sqlite3_vtab_cursor base;
    redis_vtbl_vtab *vtab;
    
    vector_t rows;
    sqlite3_int64 *current_row;
    
    list_t column_data;
    int column_data_valid;
} redis_vtbl_cursor;

static int redis_vtbl_cursor_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
static int redis_vtbl_cursor_close(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv);
static int redis_vtbl_cursor_next(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_eof(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int N);
static int redis_vtbl_cursor_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid);


/*-----------------------------------------------------------------------------
 * String helpers
 *----------------------------------------------------------------------------*/

static int string_append(char** str, const char* src) {
    char* s;
    size_t str_sz = *str ? strlen(*str) : 0;
    size_t src_sz = strlen(src);
    
    s = realloc(*str, str_sz+src_sz+1);
    if(!s) return 1;
    *str = s;
    strcpy(s + str_sz, src);
    s[str_sz+src_sz] = 0;
    return 0;
}

#define MAXI64STRLEN ((CHAR_BIT * sizeof(sqlite3_int64) - 1) / 3 + 2)
static char* i64tos(sqlite3_int64 i) {
    static char buf[MAXI64STRLEN];           /* not thread safe nor reentrant */
    snprintf(buf, MAXI64STRLEN, "%lld", i);
    return buf;
}

/*-----------------------------------------------------------------------------
 * Column definition
 *----------------------------------------------------------------------------*/

typedef struct redis_vtbl_column_spec {
    char *name;
    int data_type;
    int indexed;
} redis_vtbl_column_spec;

static int column_type_int_p(const char *data_type) {
    return !strncasecmp(data_type, "INT", 3);
}
static int column_type_text_p(const char *data_type) {
    return !strncasecmp(data_type, "CHAR", 4) || !strncasecmp(data_type, "CLOB", 4) || !strncasecmp(data_type, "TEXT", 4);
}
static int column_type_float_p(const char *data_type) {
    return !strncasecmp(data_type, "REAL", 4) || !strncasecmp(data_type, "FLOA", 4) || !strncasecmp(data_type, "DOUB", 4);
}

static int redis_vtbl_column_spec_init(redis_vtbl_column_spec *cspec, const char *column_def) {
    int err;
    list_t tok_list;
    const char *data_type;
    
    err = list_strtok(&tok_list, column_def, " \t\n");
    if(err) return 1;

    if(tok_list.size < 1) {
        list_free(&tok_list);
        return 1;
    }

    /* Column name */
    cspec->name = list_set(&tok_list, 0, 0);
    
    /* If the next token is not CONSTRAINT then it's a type name
     * Determine column type based on sqlite affinity rules.
     * Default case is TEXT where whereas the sqlite default is NUMERIC */
    cspec->data_type = SQLITE_TEXT;
    
    data_type = list_get(&tok_list, 1);
    if(tok_list.size > 1 && strcasecmp(data_type, "CONSTRAINT")) {

        if(column_type_int_p(data_type))
            cspec->data_type = SQLITE_INTEGER;
        
        else if(column_type_text_p(data_type))  
            cspec->data_type = SQLITE_TEXT;
        
        else if(column_type_float_p(data_type))
            cspec->data_type = SQLITE_FLOAT;
    }
    
    cspec->indexed = 0;
    
    list_free(&tok_list);
    return 0;
}

static void redis_vtbl_column_spec_free(redis_vtbl_column_spec *cspec) {
    free(cspec->name);
}


/*-----------------------------------------------------------------------------
 * Redis backed Virtual table
 *----------------------------------------------------------------------------*/

static int redis_vtbl_vtab_init(redis_vtbl_vtab *vtab, const char *conn_config, const char *db, const char *table, const char *prefix, char **pzErr);
static int redis_vtbl_vtab_update_indices(redis_vtbl_vtab *vtab);
static int redis_vtbl_vtab_generate_rowid(redis_vtbl_vtab *vtab, sqlite3_int64 *rowid);
static void redis_vtbl_vtab_free(redis_vtbl_vtab *vtab);

static int redis_vtbl_vtab_init(redis_vtbl_vtab *vtab, const char *conn_config, const char *db, const char *table, const char *prefix, char **pzErr) {
    int err;
    
    memset(&vtab->base, 0, sizeof(sqlite3_vtab));
    
    err = redis_vtbl_connection_init(&vtab->conn_spec, conn_config);
    if(err) {
        switch(err) {
            case CONNECTION_BAD_FORMAT:
                *pzErr = sqlite3_mprintf("Bad format; Expected ip[:port] | sentinel service ip[:port] [ip[:port]...], key_prefix, column_def0, ...column_defN");
                return SQLITE_ERROR;
            case CONNECTION_ENOMEM:
                return SQLITE_NOMEM;
        }
    }
    
    vtab->c = 0;
    vtab->key_base = 0;
    
    string_append(&vtab->key_base, prefix);
    string_append(&vtab->key_base, ".");
    string_append(&vtab->key_base, db);
    string_append(&vtab->key_base, ".");
    string_append(&vtab->key_base, table);
    
    if(!vtab->key_base) {
        redis_vtbl_connection_free(&vtab->conn_spec);
        return SQLITE_NOMEM;
    }
    
    vector_init(&vtab->columns, sizeof(redis_vtbl_column_spec), (void(*)(void*))redis_vtbl_column_spec_free);
    
    return SQLITE_OK;
}

static int redis_vtbl_vtab_update_indices(redis_vtbl_vtab *vtab) {
    int err;
    list_t indexes;
    redisReply *reply;
    
    /* retrieve indices from redis */
    reply = redisCommand(vtab->c, "SMEMBERS %s.indices", vtab->key_base);
    if(!reply) return 1;
    
    list_init(&indexes, free);
    err = redis_reply_string_list(&indexes, reply);
    
    /* TODO update column spec to identify that these columns are indexed */
    
    list_free(&indexes);
    
    freeReplyObject(reply);
    if(err)  return 1;
    return 0;
}

static int redis_vtbl_vtab_generate_rowid(redis_vtbl_vtab *vtab, sqlite3_int64 *rowid) {
    redisReply *reply;
    
    reply = redisCommand(vtab->c, "INCR %s.rowid", vtab->key_base);
    if(!reply) return 1;
    
    if(reply->type != REDIS_REPLY_INTEGER) {
        freeReplyObject(reply);
        return 1;
    }
    
    *rowid = reply->integer;
    freeReplyObject(reply);
    return 0;
}

static void redis_vtbl_vtab_free(redis_vtbl_vtab *vtab) {
    redis_vtbl_connection_free(&vtab->conn_spec);
    if(vtab->c) redisFree(vtab->c);
    free(vtab->key_base);
    vector_free(&vtab->columns);
}

/* argv[1]    - database name
 * argv[2]    - table name
 * argv[3]    - address of redis e.g. "127.0.0.1:6379"
 *  -or-      - address(es) of sentinel "sentinel service-name 127.0.0.1:6379;192.168.1.1"
 * argv[4]    - key prefix
 * argv[5...] - column defintions */
static int redis_vtbl_create(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVTab, char **pzErr) {
    int i;
    size_t n;
    int err;
    list_t column;
    redis_vtbl_vtab *vtab;
    redis_vtbl_column_spec cspec;
    
    (void)pAux; /* unused */
    
    if(argc < 6) {
        *pzErr = sqlite3_mprintf("Bad format; Expected ip[:port] | sentinel service ip[:port] [ip[:port]...], key_prefix, column_def0, ...column_defN");
        return SQLITE_ERROR;
    }

    const char *db_name = argv[1];
    const char *table = argv[2];
    const char *conn_config = argv[3];
    const char *prefix = argv[4];
    
    vtab = malloc(sizeof(redis_vtbl_vtab));
    if(!vtab) return SQLITE_NOMEM;
    
    /* Initialises structure parameters
     * Parses and validates configuration
     * returns an sqlite error code on failure */
    err = redis_vtbl_vtab_init(vtab, conn_config, db_name, table, prefix, pzErr);
    if(err) {
        /* error message is already set */
        free(vtab);
        return err;
    }
    
    /* Attempt to connect to redis.
     * Will either connect via sentinel or directly to redis depending on the configuration. */
    err = redis_vtbl_connection_connect(&vtab->conn_spec, &vtab->c);
    if(err) {
        if(vtab->conn_spec.service) {
            switch(err) {
                case SENTINEL_ERROR:
                    *pzErr = sqlite3_mprintf("Sentinel: Unknown error.");
                    break;
                case SENTINEL_MASTER_NAME_UNKNOWN:
                    *pzErr = sqlite3_mprintf("Sentinel: Master name unknown");
                    break;
                case SENTINEL_MASTER_UNKNOWN:
                    *pzErr = sqlite3_mprintf("Sentinel: Master Unknown");
                    break;
                case SENTINEL_UNREACHABLE:
                    *pzErr = sqlite3_mprintf("Sentinel: Unreachable");
                    break;
            }
        } else {
            *pzErr = sqlite3_mprintf("Redis: %s", vtab->conn_spec.errstr);
        }
        
        redis_vtbl_vtab_free(vtab);
        free(vtab);
        return SQLITE_ERROR;
    }
    
    list_init(&column, 0);
    for(i = 5; i < argc; ++i)
        list_push(&column, (char*)argv[i]);
    
    /* parse column names and types */
    for(n = 0; n < column.size; ++n) {
        err = redis_vtbl_column_spec_init(&cspec, list_get(&column, n));
        if(err) {
            *pzErr = sqlite3_mprintf("Unable to parse column definition '%s'", list_get(&column, n));
            
            list_free(&column);
            redis_vtbl_vtab_free(vtab);
            free(vtab);
            return SQLITE_ERROR;
        }
        
        vector_push(&vtab->columns, &cspec);
    }
    
    /* retrieve indices from redis */
    err = redis_vtbl_vtab_update_indices(vtab);
    if(err) {
        *pzErr = sqlite3_mprintf("Unable to retrieve indices from redis");
        
        list_free(&column);
        redis_vtbl_vtab_free(vtab);
        free(vtab);
        return SQLITE_ERROR;
    }
    
    /* Create table definition and pass to sqlite */
    char* s = 0;
    string_append(&s, "CREATE TABLE xxxx(");
    for(n = 0; n < column.size; ) {
        string_append(&s, list_get(&column, n));
        ++n;
        if(n < column.size)
            string_append(&s, ", ");
    }
    string_append(&s, ")");
    if(!s) return SQLITE_NOMEM;
    
    err = sqlite3_declare_vtab(db, s);
    if(err) return err;
    free(s);
    
    list_free(&column);
    
    *ppVTab = (sqlite3_vtab*)vtab;
    return SQLITE_OK;
}

static int redis_vtbl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr) {
    return redis_vtbl_create(db, pAux, argc, argv, ppVTab, pzErr);
}

static int redis_vtbl_bestindex(sqlite3_vtab *pVTab, sqlite3_index_info *pIndexInfo) {
    redis_vtbl_vtab *vtab;
    int i;
    
    vtab = (redis_vtbl_vtab*)pVTab;
    
    /* Explanation of cost constants
     * 10000.0  Guess at cost of full table scan
     * 1.0      Lookup by rowid is O(1)
     * 2500.0   Lookup by relative rowid is fairly cheap
     * 10.0     Lookup by index incurs some lookup overhead
     * 5000.0   Lookup by relative index requires multiple ops */
    pIndexInfo->idxNum = CURSOR_INDEX_SCAN;
    pIndexInfo->estimatedCost = 10000.0;
    
    for(i = 0; i < pIndexInfo->nConstraint; ++i) {
        struct sqlite3_index_constraint *constraint = &pIndexInfo->aConstraint[i];
        if(!constraint->usable) continue;
        
        if(constraint->iColumn == /* rowid */ -1) {
            pIndexInfo->aConstraintUsage[i].argvIndex = 1;
            pIndexInfo->estimatedCost = constraint->op == SQLITE_INDEX_CONSTRAINT_EQ ? 1.0 : 2500.0;
            
            switch(constraint->op) {
                case SQLITE_INDEX_CONSTRAINT_EQ:
                    pIndexInfo->idxNum = CURSOR_INDEX_ROWID_EQ;
                    break;
                case SQLITE_INDEX_CONSTRAINT_GT:
                    pIndexInfo->idxNum = CURSOR_INDEX_ROWID_GT;
                    break;
                case SQLITE_INDEX_CONSTRAINT_LE:
                    pIndexInfo->idxNum = CURSOR_INDEX_ROWID_LE;
                    break;
                case SQLITE_INDEX_CONSTRAINT_LT:
                    pIndexInfo->idxNum = CURSOR_INDEX_ROWID_LT;
                    break;
                case SQLITE_INDEX_CONSTRAINT_GE:
                    pIndexInfo->idxNum = CURSOR_INDEX_ROWID_GE;
                    break;
            }
            break;
        } else {
            redis_vtbl_column_spec *cspec;
        
            /* lookup the column by id
             * Determine if that column is indexed */
            cspec = vector_get(&vtab->columns, constraint->iColumn);
            if(!cspec) return SQLITE_ERROR;
            
            if(cspec->indexed) {
                pIndexInfo->aConstraintUsage[i].argvIndex = 1;
                pIndexInfo->estimatedCost = constraint->op == SQLITE_INDEX_CONSTRAINT_EQ ? 10.0 : 5000.0;
                pIndexInfo->idxStr = cspec->name;
                
                switch(constraint->op) {
                    case SQLITE_INDEX_CONSTRAINT_EQ:
                        pIndexInfo->idxNum = CURSOR_INDEX_NAMED_EQ;
                        break;
                    case SQLITE_INDEX_CONSTRAINT_GT:
                        pIndexInfo->idxNum = CURSOR_INDEX_NAMED_GT;
                        break;
                    case SQLITE_INDEX_CONSTRAINT_LE:
                        pIndexInfo->idxNum = CURSOR_INDEX_NAMED_LE;
                        break;
                    case SQLITE_INDEX_CONSTRAINT_LT:
                        pIndexInfo->idxNum = CURSOR_INDEX_NAMED_LT;
                        break;
                    case SQLITE_INDEX_CONSTRAINT_GE:
                        pIndexInfo->idxNum = CURSOR_INDEX_NAMED_GE;
                        break;
                }
                break;
            }
        }
    }
    
    return SQLITE_OK;
}

static int redis_vtbl_findfunction(sqlite3_vtab *pVtab, int nArg, const char *zName, void (**pxFunc)(sqlite3_context*,int,sqlite3_value**), void **ppArg) {
    redis_vtbl_vtab *vtab;
    
    vtab = (redis_vtbl_vtab*)pVtab;
    if(nArg == 1 && !strcmp(zName, "redis_create_index")) {
        *pxFunc = redis_vtbl_func_createindex;
        *ppArg = vtab;          /* may need to pass some extra data through the context. */
        return 1;
    }
    
    return 0;
}

static int redis_vtbl_exec_insert(redis_vtbl_vtab *vtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid);
static int redis_vtbl_exec_update(redis_vtbl_vtab *vtab, int argc, sqlite3_value **argv);
static int redis_vtbl_exec_delete(redis_vtbl_vtab *vtab, sqlite3_int64 row_id);

static int redis_vtbl_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid) {
    int err;
    redis_vtbl_vtab *vtab;
    
    vtab = (redis_vtbl_vtab*)pVTab;
    
    if(argc == 1) {                                                     /* delete */
        sqlite3_int64 row_id;
        
        row_id = sqlite3_value_int64(argv[0]);
        err = redis_vtbl_exec_delete(vtab, row_id);
    
    } else if(sqlite3_value_type(argv[0]) == SQLITE_NULL) {             /* insert */
        err = redis_vtbl_exec_insert(vtab, argc, argv, pRowid);
    
    } else if(argv[0] == argv[1]) {                                     /* update */
        err = redis_vtbl_exec_update(vtab, argc, argv);
    
    } else {
        pVTab->zErrMsg = sqlite3_mprintf("User provided rowid disallowed.");
        return SQLITE_ERROR;        /* attempt to update rowid disallowed */
    }
    
    return err;
}

static int redis_vtbl_exec_insert(redis_vtbl_vtab *vtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid) {
    int err;
    sqlite3_int64 row_id;
    list_t args;
    char *str;
    size_t i;
    redis_vtbl_column_spec *cspec;
    list_t replies;
    
    if(sqlite3_value_type(argv[1]) != SQLITE_NULL) {
        vtab->base.zErrMsg = sqlite3_mprintf("User provided rowid disallowed.");
        return SQLITE_ERROR;            /* attempt to specify manual rowid disallowed */
    }
    
    if((unsigned)argc != vtab->columns.size + 2) {
        return SQLITE_ERROR;            /* correct number of columns not provided */
    }
    
    err = redis_vtbl_vtab_generate_rowid(vtab, &row_id);
    if(err) {
        vtab->base.zErrMsg = sqlite3_mprintf("Unable to generate new rowid");
        return SQLITE_ERROR;
    }
    *pRowid = row_id;
    
    redisAppendCommand(vtab->c, "MULTI");
    
    /* HMSET key column0 value0 ...columnN valueN */                                                    /* create object */
    list_init(&args, free);
    
    list_push(&args, strdup("HMSET"));
    
    str = 0;
    string_append(&str, vtab->key_base);
    string_append(&str, ":");
    string_append(&str, i64tos(row_id));
    list_push(&args, str);
    
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        list_push(&args, strdup(cspec->name));
        list_push(&args, strdup((const char*)sqlite3_value_text(argv[2 + i])));
    }
    
    redisAppendCommandArgv(vtab->c, args.size, (const char**)args.data, 0);
    list_free(&args);
    redisAppendCommand(vtab->c, "ZADD %s.index.rowid %lld %lld", vtab->key_base, row_id, row_id);       /* add to rowids */
    /* todo */                                                                                          /* add to indexes */
    /*
    for each indexed column {
        zadd prefix.db.table.index:column_name 0 column_value
        sadd prefix.db.table.index:column_name:column_value rowid
    }
    */
    redisAppendCommand(vtab->c, "EXEC");
    
    list_init(&replies, freeReplyObject);
    redis_n_replies(vtab->c, 4, &replies);
    
    err = redis_check_expected(&replies, 
        /* MULTI */ redis_status_reply_p,
        /* HMSET */ redis_status_queued_reply_p,
        /* ZADD */  redis_status_queued_reply_p,
        /* EXEC */  redis_bulk_reply_p);
    if(err) {
        list_free(&replies);
        return SQLITE_ERROR;
    
    } else {
        redisReply *exec_reply;
        exec_reply = list_get(&replies, 3);
        err = redis_check_expected_bulk(exec_reply,
            /* HMSET */ redis_status_reply_p,
            /* ZADD */  redis_integer_reply_p);
        if(err) {
            list_free(&replies);
            return SQLITE_ERROR;
        }
    }
    list_free(&replies);

    return SQLITE_OK;
}
static int redis_vtbl_exec_update(redis_vtbl_vtab *vtab, int argc, sqlite3_value **argv) {
    int err;
    sqlite3_int64 row_id;
    list_t args;
    char *str;
    size_t i;
    redis_vtbl_column_spec *cspec;
    list_t replies;
    
    if((unsigned)argc != vtab->columns.size + 2) {
        return SQLITE_ERROR;            /* correct number of columns not provided */
    }
    
    row_id = sqlite3_value_int64(argv[0]);
    redisAppendCommand(vtab->c, "WATCH %s:%lld", vtab->key_base, row_id);                               /* WATCH key */
    redisAppendCommand(vtab->c, "MULTI");
    
    /* HMSET key column0 value0 ...columnN valueN */                                                    /* update object */
    list_init(&args, free);
    
    list_push(&args, strdup("HMSET"));
    
    str = 0;
    string_append(&str, vtab->key_base);
    string_append(&str, ":");
    string_append(&str, i64tos(row_id));
    list_push(&args, str);
    
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        list_push(&args, strdup(cspec->name));
        list_push(&args, strdup((const char*)sqlite3_value_text(argv[2 + i])));
    }
    
    redisAppendCommandArgv(vtab->c, args.size, (const char**)args.data, 0);
    list_free(&args);
    /* todo */                                                                                          /* update indexes */
    redisAppendCommand(vtab->c, "EXEC");
    
    list_init(&replies, freeReplyObject);
    redis_n_replies(vtab->c, 4, &replies);
    
    err = redis_check_expected(&replies, 
        /* WATCH */ redis_status_reply_p,
        /* MULTI */ redis_status_reply_p,
        /* HMSET */ redis_status_queued_reply_p,
        /* EXEC */  redis_bulk_reply_p);
    if(err) {
        list_free(&replies);
        return SQLITE_ERROR;
    
    } else {
        redisReply *exec_reply;
        exec_reply = list_get(&replies, 3);
        err = redis_check_expected_bulk(exec_reply,
            /* HMSET */ redis_status_reply_p);
        
        /* null response to EXEC means that the transaction was aborted. */
        if(exec_reply->elements == 0)
            err = 1;
        
        if(err) {
            list_free(&replies);
            return SQLITE_ERROR;
        }
    }
    list_free(&replies);
    
    return SQLITE_OK;
}
static int redis_vtbl_exec_delete(redis_vtbl_vtab *vtab, sqlite3_int64 row_id) {
    int err;
    list_t replies;
    
    redisAppendCommand(vtab->c, "MULTI");
    redisAppendCommand(vtab->c, "DEL %s:%lld", vtab->key_base, row_id);                 /* erase object */
    redisAppendCommand(vtab->c, "ZREM %s.index.rowid %lld", vtab->key_base, row_id);    /* erase from rowids */
    /* todo */                                                                          /* erase from indexes */
    redisAppendCommand(vtab->c, "EXEC");

    list_init(&replies, freeReplyObject);
    redis_n_replies(vtab->c, 4, &replies);
    
    err = redis_check_expected(&replies, 
        /* MULTI */ redis_status_reply_p,
        /* DEL */   redis_status_queued_reply_p,
        /* ZREM */  redis_status_queued_reply_p,
        /* EXEC */  redis_bulk_reply_p);
    if(err) {
        list_free(&replies);
        return SQLITE_ERROR;
    
    } else {
        redisReply *exec_reply;
        exec_reply = list_get(&replies, 3);
        err = redis_check_expected_bulk(exec_reply,
            /* DEL */   redis_integer_reply_p,
            /* ZREM */  redis_integer_reply_p);
        if(err) {
            list_free(&replies);
            return SQLITE_ERROR;
        }
    }
    list_free(&replies);
    
    return SQLITE_OK;
}

static int redis_vtbl_disconnect(sqlite3_vtab *pVTab) {
    return redis_vtbl_destroy(pVTab);
}

static int redis_vtbl_destroy(sqlite3_vtab *pVTab) {
    redis_vtbl_vtab *vtab;
    
    vtab = (redis_vtbl_vtab*)pVTab;
    redis_vtbl_vtab_free(vtab);
    free(vtab);
    return SQLITE_OK;
}

/*-----------------------------------------------------------------------------
 * Utility function to define indexes
 *----------------------------------------------------------------------------*/

/* column name */
static void redis_vtbl_func_createindex(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    redis_vtbl_vtab *vtab;
    
    vtab = sqlite3_user_data(ctx);
    
    (void)vtab; /* pending impl */
    (void)argc; /* pending impl */
    (void)argv; /* pending impl */
    
    /* todo create the index 
     * Seems it may be necessary to pass some data through the context. */
    
    sqlite3_result_int(ctx, 1);
}

/*-----------------------------------------------------------------------------
 * Redis backed virtual table cursor
 *----------------------------------------------------------------------------*/

static int redis_vtbl_cursor_init(redis_vtbl_cursor *cur, redis_vtbl_vtab *vtab) {
    memset(&cur->base, 0, sizeof(sqlite3_vtab_cursor));

    cur->vtab = vtab;
    
    vector_init(&cur->rows, sizeof(int64_t), 0);
    cur->current_row = 0;
    
    list_init(&cur->column_data, free);
    cur->column_data_valid = 0;
    
    return SQLITE_OK;
}

/* retrieve column_data for current_row */
static void redis_vtbl_cursor_get(redis_vtbl_cursor *cur) {
    int err;
    int eof;
    redis_vtbl_vtab *vtab;
    list_t args;
    redisReply *reply;
    char *str;
    size_t i;
    redis_vtbl_column_spec *cspec;

    eof = cur->current_row == vector_end(&cur->rows);
    if(eof) return;
    
    vtab = cur->vtab;
    list_clear(&cur->column_data);

    /* HMGET key_base.current_row column0 ...columnN */
    list_init(&args, free);
    
    list_push(&args, strdup("HMGET"));
    
    str = 0;
    string_append(&str, vtab->key_base);
    string_append(&str, ":");
    string_append(&str, i64tos(*cur->current_row));
    if(!str) {
        list_free(&args);
        return;
    }
    list_push(&args, str);
    
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        list_push(&args, strdup(cspec->name));
    }
    
    reply = redisCommandArgv(vtab->c, args.size, (const char**)args.data, 0);
    list_free(&args);
    if(!reply) return;
    
    /* note: This implementation differs from the prototype.
     * In the original if a record was concurrently removed,
     * the next record was automatically retrieved.
     * This version will return a row full of null values.
     * It is uncertain at this time which approach is better. */
    err = redis_reply_string_list(&cur->column_data, reply);
    freeReplyObject(reply);
    if(err) return;
    cur->column_data_valid = 1;
}

static void redis_vtbl_cursor_free(redis_vtbl_cursor *cur) {
    vector_free(&cur->rows);
    list_free(&cur->column_data);
}

static int redis_vtbl_cursor_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_cursor *cursor;

    vtab = (redis_vtbl_vtab*)pVTab;

    cursor = malloc(sizeof(redis_vtbl_cursor));
    if(!cursor) return SQLITE_NOMEM;
    
    err = redis_vtbl_cursor_init(cursor, vtab);
    if(err) {
        free(cursor);
        return err;
    }
    
    *ppCursor = (sqlite3_vtab_cursor*)cursor;
    return SQLITE_OK;
}

static int redis_vtbl_cursor_close(sqlite3_vtab_cursor *pCursor) {
    redis_vtbl_cursor *cursor;
    
    cursor = (redis_vtbl_cursor*)pCursor;
    redis_vtbl_cursor_free(cursor);
    free(cursor);
    
    return SQLITE_OK;
}

static int redis_vtbl_cursor_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_cursor *cursor;
    redisReply *reply;
    sqlite3_int64 row_id;
    
    cursor = (redis_vtbl_cursor*)pCursor;
    vtab = cursor->vtab;
    
    vector_clear(&cursor->rows);
    
    switch(idxNum) {
        case CURSOR_INDEX_ROWID_EQ:
        case CURSOR_INDEX_ROWID_GT:
        case CURSOR_INDEX_ROWID_LT:
        case CURSOR_INDEX_ROWID_GE:
        case CURSOR_INDEX_ROWID_LE:
            if(argc == 0) return SQLITE_ERROR;
            row_id = sqlite3_value_int64(argv[0]);
            break;
    }
    
    switch(idxNum) {
        case CURSOR_INDEX_SCAN:
            reply = redisCommand(vtab->c, "ZRANGE %s.index.rowid 0 -1", vtab->key_base);
            break;
        
        case CURSOR_INDEX_ROWID_EQ:
            reply = redisCommand(vtab->c, "EXISTS %s:%lld", vtab->key_base, row_id);
            break;
        case CURSOR_INDEX_ROWID_GT:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index.rowid (%lld +inf", vtab->key_base, row_id);
            break;
        case CURSOR_INDEX_ROWID_LT:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index.rowid -inf (%lld", vtab->key_base, row_id);
            break;
        case CURSOR_INDEX_ROWID_GE:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index.rowid %lld +inf", vtab->key_base, row_id);
            break;
        case CURSOR_INDEX_ROWID_LE:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index.rowid -inf %lld", vtab->key_base, row_id);
            break;
        
        /* todo: implement indexes */
        case CURSOR_INDEX_NAMED_EQ:
            /* special */
            break;
        case CURSOR_INDEX_NAMED_GT:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index:%s (%lld +inf", vtab->key_base, idxStr);
            break;
        case CURSOR_INDEX_NAMED_LT:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index:%s -inf (%lld", vtab->key_base, idxStr);
            break;
        case CURSOR_INDEX_NAMED_GE:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index:%s %lld +inf", vtab->key_base, idxStr);
            break;
        case CURSOR_INDEX_NAMED_LE:
            reply = redisCommand(vtab->c, "ZRANGEBYSCORE %s.index:%s -inf %lld", vtab->key_base, idxStr);
            break;
    }

    if(idxNum == CURSOR_INDEX_ROWID_EQ) {
        if(!reply) return SQLITE_ERROR;
        
        if(reply->type == REDIS_REPLY_INTEGER) {
            if(reply->integer) vector_push(&cursor->rows, &row_id);
            
        } else {
            freeReplyObject(reply);
            return SQLITE_ERROR;
        }
        freeReplyObject(reply);
           
    } else if(idxNum == CURSOR_INDEX_NAMED_EQ) {
        return SQLITE_ERROR;            /* todo implement indexes */
    
    } else {
        if(!reply) return SQLITE_ERROR;
        
        err = redis_reply_numeric_array(&cursor->rows, reply);
        freeReplyObject(reply);
        if(err) return SQLITE_ERROR;
    }

    cursor->current_row = vector_begin(&cursor->rows);
    cursor->column_data_valid = 0;
    
    return SQLITE_OK;
}

static int redis_vtbl_cursor_next(sqlite3_vtab_cursor *pCursor) {
    redis_vtbl_cursor *cursor;
    
    cursor = (redis_vtbl_cursor*)pCursor;
    ++cursor->current_row;
    cursor->column_data_valid = 0;
    
    return SQLITE_OK;
}

static int redis_vtbl_cursor_eof(sqlite3_vtab_cursor *pCursor) {
    redis_vtbl_cursor *cursor;
    cursor = (redis_vtbl_cursor*)pCursor;
    
    return cursor->current_row == vector_end(&cursor->rows);
}

static int redis_vtbl_cursor_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int N) {
    redis_vtbl_cursor *cursor;
    redis_vtbl_vtab *vtab;
    redis_vtbl_column_spec *cspec;
    const char *value;
    char *end;
    sqlite3_int64 i;
    double f;
    
    cursor = (redis_vtbl_cursor*)pCursor;
    vtab = cursor->vtab;

    cspec = vector_get(&vtab->columns, N);
    if(!cspec) return SQLITE_ERROR;

    if(!cursor->column_data_valid) {
        redis_vtbl_cursor_get(cursor);
    }
    
    value = list_get(&cursor->column_data, N);
    if(!value) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    
    switch(cspec->data_type) {
        case SQLITE_INTEGER:
            errno = 0;
            i = strtoll(value, &end, 10);
            if(errno || *end)      /* out-of-range | rubbish characters */
                break;
            sqlite3_result_int64(ctx, i);
            break;
            
        case SQLITE_TEXT:
            sqlite3_result_text(ctx, value, -1, SQLITE_TRANSIENT);
            break;
            
        case SQLITE_FLOAT:
            errno = 0;
            f = strtod(value, &end);
            if(errno || *end)      /* out-of-range | rubbish characters */
                break;
            sqlite3_result_double(ctx, f);
            break;
    }
    
    return SQLITE_OK;
}

static int redis_vtbl_cursor_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
    redis_vtbl_cursor *cursor;
    cursor = (redis_vtbl_cursor*)pCursor;
    
    if(!cursor->current_row) return SQLITE_ERROR;

    *pRowid = *cursor->current_row;
    return SQLITE_OK;
}

/*-----------------------------------------------------------------------------
 * sqlite3 extension machinery
 *----------------------------------------------------------------------------*/

static const sqlite3_module redis_vtbl_Module = {
    .iVersion     = 1,
    
    .xCreate       = redis_vtbl_create,
    .xConnect      = redis_vtbl_connect,
    .xBestIndex    = redis_vtbl_bestindex,
    .xFindFunction = redis_vtbl_findfunction,
    .xUpdate       = redis_vtbl_update,
    .xDisconnect   = redis_vtbl_disconnect,
    .xDestroy      = redis_vtbl_destroy,
    
    .xOpen         = redis_vtbl_cursor_open,
    .xClose        = redis_vtbl_cursor_close,
    .xFilter       = redis_vtbl_cursor_filter,
    .xNext         = redis_vtbl_cursor_next,
    .xEof          = redis_vtbl_cursor_eof,
    .xColumn       = redis_vtbl_cursor_column,
    .xRowid        = redis_vtbl_cursor_rowid
};

int sqlite3_redisvtbl_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    int rc;
    (void)pzErrMsg; /* unused */
    
    rc = sqlite3_create_module(db, "redis", &redis_vtbl_Module, 0);
    if(rc != SQLITE_OK)
        return rc;
    
    rc = sqlite3_overload_function(db, "redis_create_index", 1);
    return rc;
}

#if !SQLITE_CORE
int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    return sqlite3_redisvtbl_init(db, pzErrMsg, pApi);
}
#endif

