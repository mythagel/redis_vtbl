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
 * prefix.db.table.index:x      = value zset index for column x
 * prefix.db.table.index:x:val  = rowid map for value val in column x */

typedef struct redis_vtbl_vtab {
    sqlite3_vtab base;
    
    redis_vtbl_connection conn;
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

static int redis_vtbl_column_spec_name_cmp(const char *l, const redis_vtbl_column_spec *r) {
    return strcmp(l, r->name);
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
    
    err = redis_vtbl_connection_init(&vtab->conn, conn_config);
    if(err) {
        switch(err) {
            case CONNECTION_BAD_FORMAT:
                *pzErr = sqlite3_mprintf("Bad format; Expected ip[:port] | sentinel service ip[:port] [ip[:port]...], key_prefix, column_def0, ...column_defN");
                return SQLITE_ERROR;
            case CONNECTION_ENOMEM:
                return SQLITE_NOMEM;
        }
    }
    
    vtab->key_base = 0;
    
    string_append(&vtab->key_base, prefix);
    string_append(&vtab->key_base, ".");
    string_append(&vtab->key_base, db);
    string_append(&vtab->key_base, ".");
    string_append(&vtab->key_base, table);
    
    if(!vtab->key_base) {
        redis_vtbl_connection_free(&vtab->conn);
        return SQLITE_NOMEM;
    }
    
    vector_init(&vtab->columns, sizeof(redis_vtbl_column_spec), (void(*)(void*))redis_vtbl_column_spec_free);
    
    return SQLITE_OK;
}

static int redis_vtbl_vtab_update_indices(redis_vtbl_vtab *vtab) {
    int err;
    redis_vtbl_command cmd;
    list_t indexes;
    redisReply *reply;
    size_t i;
    redis_vtbl_column_spec *cspec;
    void *indexed;
    
    /* retrieve indices from redis */
    redis_vtbl_command_init_arg(&cmd, "SMEMBERS");
    redis_vtbl_command_arg_fmt(&cmd, "%s.indices", vtab->key_base);
    reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
    if(!reply) return 1;
    
    list_init(&indexes, free);
    err = redis_reply_string_list(&indexes, reply);
    freeReplyObject(reply);
    
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        indexed = list_find(&indexes, cspec->name, (int(*)(const void*, const void*))strcmp);
        cspec->indexed = indexed ? 1 : 0;
    }
    
    list_free(&indexes);
    
    if(err)  return 1;
    return 0;
}

static int redis_vtbl_vtab_generate_rowid(redis_vtbl_vtab *vtab, sqlite3_int64 *rowid) {
    redis_vtbl_command cmd;
    redisReply *reply;
    
    redis_vtbl_command_init_arg(&cmd, "INCR");
    redis_vtbl_command_arg_fmt(&cmd, "%s.rowid", vtab->key_base);
    reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
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
    redis_vtbl_connection_free(&vtab->conn);
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
    err = redis_vtbl_connection_connect(&vtab->conn);
    if(err) {
        if(vtab->conn.service) {
            *pzErr = sqlite3_mprintf("Sentinel: %s", vtab->conn.errstr);
        } else {
            *pzErr = sqlite3_mprintf("Redis: %s", vtab->conn.errstr);
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
    
    /* todo overload other functions to improve performance.
     * min and max are simple examples that atm require a table scan. */
    
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
    redis_vtbl_command cmd;
    size_t i;
    redis_vtbl_column_spec *cspec;
    list_t replies;
    list_t expected;
    list_t expected_exec;
    
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
    
    list_init(&expected, 0);
    list_init(&expected_exec, 0);
    
    redis_vtbl_command_init_arg(&cmd, "MULTI");
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_reply_p);
    
    /* HMSET key column0 value0 ...columnN valueN */                                                    /* create object */
    redis_vtbl_command_init_arg(&cmd, "HMSET");
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        redis_vtbl_command_arg(&cmd, cspec->name);
        redis_vtbl_command_arg(&cmd, (const char*)sqlite3_value_text(argv[2 + i]));
    }
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_status_reply_p);

    redis_vtbl_command_init_arg(&cmd, "ZADD");                                                          /* add to rowids */
    redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
    redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
    redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_integer_reply_p);
    
    for(i = 0; i < vtab->columns.size; ++i) {                                                           /* add to indexes */
        cspec = vector_get(&vtab->columns, i);
        if(!cspec->indexed) continue;
        
        if(cspec->data_type == SQLITE_INTEGER) {
            sqlite3_int64 value;
            value = sqlite3_value_int64(argv[2 + i]);
            
            redis_vtbl_command_init_arg(&cmd, "ZADD");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, cspec->name);
            redis_vtbl_command_arg_fmt(&cmd, "%lld", value);
            redis_vtbl_command_arg_fmt(&cmd, "%lld", value);
            redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
            list_push(&expected, redis_status_queued_reply_p);
            list_push(&expected_exec, redis_integer_reply_p);
        } else if(cspec->data_type == SQLITE_FLOAT) {
            double value;
            value = sqlite3_value_double(argv[2 + i]);

            redis_vtbl_command_init_arg(&cmd, "ZADD");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, cspec->name);
            redis_vtbl_command_arg_fmt(&cmd, "%f", value);
            redis_vtbl_command_arg_fmt(&cmd, "%f", value);
            redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
            list_push(&expected, redis_status_queued_reply_p);
            list_push(&expected_exec, redis_integer_reply_p);
        } else {
            const char *value;
            value = (const char*)sqlite3_value_text(argv[2 + i]);
            
            redis_vtbl_command_init_arg(&cmd, "ZADD");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, cspec->name);
            redis_vtbl_command_arg(&cmd, "0");
            redis_vtbl_command_arg_fmt(&cmd, "%s", value);
            redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
            list_push(&expected, redis_status_queued_reply_p);
            list_push(&expected_exec, redis_integer_reply_p);
        }
        
        redis_vtbl_command_init_arg(&cmd, "SADD");
        redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s:%s", vtab->key_base, cspec->name, (const char*)sqlite3_value_text(argv[2 + i]));
        redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
        redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
        list_push(&expected, redis_status_queued_reply_p);
        list_push(&expected_exec, redis_integer_reply_p);
    }
    
    redis_vtbl_command_init_arg(&cmd, "EXEC");
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_bulk_reply_p);
    
    list_init(&replies, freeReplyObject);
    redis_vtbl_connection_read_queued(&vtab->conn, &replies);
    
    err = redis_check_expected_list(&replies, &expected);
    list_free(&expected);
    
    if(err) {
        list_free(&replies);
        list_free(&expected_exec);
        return SQLITE_ERROR;
    
    } else {
        redisReply *exec_reply;
        exec_reply = list_get(&replies, replies.size-1);
        err = redis_check_expected_bulk_list(exec_reply, &expected_exec);
        list_free(&expected_exec);
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
    redis_vtbl_command cmd;
    size_t i;
    redis_vtbl_column_spec *cspec;
    list_t replies;
    list_t expected;
    list_t expected_exec;
    
    if((unsigned)argc != vtab->columns.size + 2) {
        return SQLITE_ERROR;            /* correct number of columns not provided */
    }
    
    list_init(&expected, 0);
    list_init(&expected_exec, 0);
    
    row_id = sqlite3_value_int64(argv[0]);
    redis_vtbl_command_init_arg(&cmd, "WATCH");                                                         /* WATCH key */
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_reply_p);
    
    redis_vtbl_command_init_arg(&cmd, "MULTI");
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_reply_p);
    
    redis_vtbl_command_init_arg(&cmd, "EVAL");                                                          /* update indexes (a) */
    redis_vtbl_command_arg(&cmd, "\
        local key_base = ARGV[1];\n\
        local row_id = ARGV[2];\n\
        \n\
        local indexed_columns = redis.call('SMEMBERS', key_base..'.indices');\n\
        for _,column_name in ipairs(indexed_columns) do\n\
            local column_value = redis.call('HGET', key_base..':'..row_id, column_name);\n\
            local value_index = key_base..'.index:'..column_name..':'..column_value;\n\
            redis.call('SREM', value_index, row_id);\n\
            if(redis.call('EXISTS', value_index) == 0) then\n\
                redis.call('ZREM', key_base..'.index:'..column_name, column_value);\n\
            end\n\
        end\n\
        return 1;\n");
    redis_vtbl_command_arg(&cmd, "2");
    redis_vtbl_command_arg_fmt(&cmd, "%s.indices", vtab->key_base);
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
    redis_vtbl_command_arg(&cmd, vtab->key_base);
    redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_integer_reply_p);
    
    /* HMSET key column0 value0 ...columnN valueN */                                                    /* update object */
    redis_vtbl_command_init_arg(&cmd, "HMSET");
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
    
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        redis_vtbl_command_arg(&cmd, cspec->name);
        redis_vtbl_command_arg(&cmd, (const char*)sqlite3_value_text(argv[2 + i]));
    }
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_status_reply_p);
    
    for(i = 0; i < vtab->columns.size; ++i) {                                                           /* update indexes (b) */
        cspec = vector_get(&vtab->columns, i);
        if(!cspec->indexed) continue;
        
        if(cspec->data_type == SQLITE_INTEGER) {
            sqlite3_int64 value;
            value = sqlite3_value_int64(argv[2 + i]);
            
            redis_vtbl_command_init_arg(&cmd, "ZADD");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, cspec->name);
            redis_vtbl_command_arg_fmt(&cmd, "%lld", value);
            redis_vtbl_command_arg_fmt(&cmd, "%lld", value);
            redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
            list_push(&expected, redis_status_queued_reply_p);
            list_push(&expected_exec, redis_integer_reply_p);
        } else if(cspec->data_type == SQLITE_FLOAT) {
            double value;
            value = sqlite3_value_double(argv[2 + i]);

            redis_vtbl_command_init_arg(&cmd, "ZADD");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, cspec->name);
            redis_vtbl_command_arg_fmt(&cmd, "%f", value);
            redis_vtbl_command_arg_fmt(&cmd, "%f", value);
            redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
            list_push(&expected, redis_status_queued_reply_p);
            list_push(&expected_exec, redis_integer_reply_p);
        } else {
            const char *value;
            value = (const char*)sqlite3_value_text(argv[2 + i]);
            
            redis_vtbl_command_init_arg(&cmd, "ZADD");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, cspec->name);
            redis_vtbl_command_arg(&cmd, "0");
            redis_vtbl_command_arg_fmt(&cmd, "%s", value);
            redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
            list_push(&expected, redis_status_queued_reply_p);
            list_push(&expected_exec, redis_integer_reply_p);
        }
        
        redis_vtbl_command_init_arg(&cmd, "SADD");
        redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s:%s", vtab->key_base, cspec->name, (const char*)sqlite3_value_text(argv[2 + i]));
        redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
        redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
        list_push(&expected, redis_status_queued_reply_p);
        list_push(&expected_exec, redis_integer_reply_p);
    }
    
    redis_vtbl_command_init_arg(&cmd, "EXEC");
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_bulk_reply_p);
    
    list_init(&replies, freeReplyObject);
    redis_vtbl_connection_read_queued(&vtab->conn, &replies);
    
    err = redis_check_expected_list(&replies, &expected);
    list_free(&expected);
    
    if(err) {
        list_free(&replies);
        list_free(&expected_exec);
        return SQLITE_ERROR;
    
    } else {
        redisReply *exec_reply;
        exec_reply = list_get(&replies, replies.size-1);
        err = redis_check_expected_bulk_list(exec_reply, &expected_exec);
        list_free(&expected_exec);
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
    redis_vtbl_command cmd;
    list_t replies;
    list_t expected;
    list_t expected_exec;
    
    list_init(&expected, 0);
    list_init(&expected_exec, 0);
    
    redis_vtbl_command_init_arg(&cmd, "MULTI");
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_reply_p);
    
    redis_vtbl_command_init_arg(&cmd, "EVAL");                                                          /* erase from indexes */
    redis_vtbl_command_arg(&cmd, "\
        local key_base = ARGV[1];\n\
        local row_id = ARGV[2];\n\
        \n\
        local indexed_columns = redis.call('SMEMBERS', key_base..'.indices');\n\
        for _,column_name in ipairs(indexed_columns) do\n\
            local column_value = redis.call('HGET', key_base..':'..row_id, column_name);\n\
            local value_index = key_base..'.index:'..column_name..':'..column_value;\n\
            redis.call('SREM', value_index, row_id);\n\
            if(redis.call('EXISTS', value_index) == 0) then\n\
                redis.call('ZREM', key_base..'.index:'..column_name, column_value);\n\
            end\n\
        end\n\
        return 1;\n");
    redis_vtbl_command_arg(&cmd, "2");
    redis_vtbl_command_arg_fmt(&cmd, "%s.indices", vtab->key_base);
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
    redis_vtbl_command_arg(&cmd, vtab->key_base);
    redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_integer_reply_p);
    
    redis_vtbl_command_init_arg(&cmd, "DEL");                                                           /* erase object */
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_integer_reply_p);
    
    redis_vtbl_command_init_arg(&cmd, "ZREM");                                                          /* erase from rowids */
    redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
    redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_status_queued_reply_p);
    list_push(&expected_exec, redis_integer_reply_p);
    
    redis_vtbl_command_init_arg(&cmd, "EXEC");
    redis_vtbl_connection_command_enqueue(&vtab->conn, &cmd);
    list_push(&expected, redis_bulk_reply_p);

    list_init(&replies, freeReplyObject);
    redis_vtbl_connection_read_queued(&vtab->conn, &replies);
    
    err = redis_check_expected_list(&replies, &expected);
    list_free(&expected);
    
    if(err) {
        list_free(&replies);
        list_free(&expected_exec);
        return SQLITE_ERROR;
    
    } else {
        redisReply *exec_reply;
        exec_reply = list_get(&replies, replies.size-1);
        err = redis_check_expected_bulk_list(exec_reply, &expected_exec);
        list_free(&expected_exec);
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

/*
Thoughts for cursor get:
It would be good to retrieve batched row data.
i.e. first get retrieves one row
next 2
next 4, etc (n * 1.618)

Additionally it would be good to not retrieve the entire row,
but only the subset of columns that are going to be requested.
*/
/* retrieve column_data for current_row */
static void redis_vtbl_cursor_get(redis_vtbl_cursor *cur) {
    int err;
    int eof;
    redis_vtbl_vtab *vtab;
    redis_vtbl_command cmd;
    redisReply *reply;
    size_t i;
    redis_vtbl_column_spec *cspec;

    eof = cur->current_row == vector_end(&cur->rows);
    if(eof) return;
    
    vtab = cur->vtab;
    list_clear(&cur->column_data);

    /* HMGET key_base.current_row column0 ...columnN */
    redis_vtbl_command_init_arg(&cmd, "HMGET");
    redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, *cur->current_row);
    
    for(i = 0; i < vtab->columns.size; ++i) {
        cspec = vector_get(&vtab->columns, i);
        redis_vtbl_command_arg(&cmd, cspec->name);
    }
    
    reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
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

static int redis_vtbl_cursor_filter_scan(redis_vtbl_cursor *cursor);
static int redis_vtbl_cursor_filter_rowid(redis_vtbl_cursor *cursor, sqlite3_int64 row_id, int idxNum);
static int redis_vtbl_cursor_filter_index(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, sqlite3_value *value);
static int redis_vtbl_cursor_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv) {
    int err;
    redis_vtbl_cursor *cursor;
    sqlite3_int64 row_id;
    
    cursor = (redis_vtbl_cursor*)pCursor;
    vector_clear(&cursor->rows);
    err = SQLITE_ERROR;

    switch(idxNum) {
        case CURSOR_INDEX_SCAN:
            err = redis_vtbl_cursor_filter_scan(cursor);
            break;
        case CURSOR_INDEX_ROWID_EQ:
        case CURSOR_INDEX_ROWID_GT:
        case CURSOR_INDEX_ROWID_LT:
        case CURSOR_INDEX_ROWID_GE:
        case CURSOR_INDEX_ROWID_LE:
            if(argc == 0) return SQLITE_ERROR;          /* Internal error. row_id not passed after request in bestindex */
            row_id = sqlite3_value_int64(argv[0]);
            err = redis_vtbl_cursor_filter_rowid(cursor, row_id, idxNum);
            break;
        case CURSOR_INDEX_NAMED_EQ:
        case CURSOR_INDEX_NAMED_GT:
        case CURSOR_INDEX_NAMED_LT:
        case CURSOR_INDEX_NAMED_GE:
        case CURSOR_INDEX_NAMED_LE:
            if(argc == 0) return SQLITE_ERROR;          /* Internal error. value not passed after request in bestindex */
            err = redis_vtbl_cursor_filter_index(cursor, idxNum, idxStr, argv[0]);
            break;
    }
    
    if(!err) {
        cursor->current_row = vector_begin(&cursor->rows);
        cursor->column_data_valid = 0;
    }
    return err;
}
static int redis_vtbl_cursor_filter_scan(redis_vtbl_cursor *cursor) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_command cmd;
    redisReply *reply;
    
    vtab = cursor->vtab;
    
    redis_vtbl_command_init_arg(&cmd, "ZRANGE");
    redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
    redis_vtbl_command_arg(&cmd, "0");
    redis_vtbl_command_arg(&cmd, "-1");
    reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
    if(!reply) return SQLITE_ERROR;
    
    err = redis_reply_numeric_array(&cursor->rows, reply);
    freeReplyObject(reply);
    
    if(err) return SQLITE_ERROR;
    return SQLITE_OK;
}
static int redis_vtbl_cursor_filter_rowid(redis_vtbl_cursor *cursor, sqlite3_int64 row_id, int idxNum) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_command cmd;
    redisReply *reply;

    vtab = cursor->vtab;

    switch(idxNum) {
        case CURSOR_INDEX_ROWID_EQ:
            redis_vtbl_command_init_arg(&cmd, "EXISTS");
            redis_vtbl_command_arg_fmt(&cmd, "%s:%lld", vtab->key_base, row_id);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_ROWID_GT:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
            redis_vtbl_command_arg_fmt(&cmd, "(%lld", row_id);
            redis_vtbl_command_arg(&cmd, "+inf");
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_ROWID_LT:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
            redis_vtbl_command_arg(&cmd, "-inf");
            redis_vtbl_command_arg_fmt(&cmd, "(%lld", row_id);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_ROWID_GE:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
            redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
            redis_vtbl_command_arg(&cmd, "+inf");
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_ROWID_LE:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index.rowid", vtab->key_base);
            redis_vtbl_command_arg(&cmd, "-inf");
            redis_vtbl_command_arg_fmt(&cmd, "%lld", row_id);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
    }

    if(!reply) return SQLITE_ERROR;
    
    if(idxNum == CURSOR_INDEX_ROWID_EQ) {
        if(reply->type == REDIS_REPLY_INTEGER) {
            if(reply->integer) vector_push(&cursor->rows, &row_id);
            
        } else {
            freeReplyObject(reply);
            return SQLITE_ERROR;
        }
        freeReplyObject(reply);
    
    } else {
        err = redis_reply_numeric_array(&cursor->rows, reply);
        freeReplyObject(reply);
        if(err) return SQLITE_ERROR;
    }
    
    return SQLITE_OK;
}
static int redis_vtbl_cursor_filter_index_text(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, const char *value);
static int redis_vtbl_cursor_filter_index_integer(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, sqlite3_int64 value);
static int redis_vtbl_cursor_filter_index_float(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, double value);
static int redis_vtbl_cursor_filter_index(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, sqlite3_value *value) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_column_spec *cspec;
    
    vtab = cursor->vtab;
    
    cspec = vector_find(&vtab->columns, idxStr, (int (*)(const void *, const void *))redis_vtbl_column_spec_name_cmp);
    if(!cspec) return SQLITE_ERROR;

    err = SQLITE_ERROR;

    switch(cspec->data_type) {
        case SQLITE_INTEGER: {
            sqlite3_int64 ivalue;
            
            ivalue = sqlite3_value_int64(value);
            err = redis_vtbl_cursor_filter_index_integer(cursor, idxNum, idxStr, ivalue);
            break;
        }
        
        case SQLITE_TEXT: {
            const char *cvalue;
            
            cvalue = (const char*)sqlite3_value_text(value);
            err = redis_vtbl_cursor_filter_index_text(cursor, idxNum, idxStr, cvalue);
            break;
        }
        
        case SQLITE_FLOAT: {
            double fvalue;
            
            fvalue = sqlite3_value_double(value);
            err = redis_vtbl_cursor_filter_index_float(cursor, idxNum, idxStr, fvalue);
            break;
        }
    }
    
    return err;
}
static int redis_vtbl_cursor_filter_index_text(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, const char *value) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_command cmd;
    redisReply *reply;
    long long rank;
    
    vtab = cursor->vtab;
    
    redis_vtbl_command_init_arg(&cmd, "ZRANK");
    redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
    redis_vtbl_command_arg_fmt(&cmd, "%s", value);
    reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
    if(reply->type == REDIS_REPLY_INTEGER) {
        rank = reply->integer;
    
    } else if(reply->type == REDIS_REPLY_NIL) {
        rank = -1;
    
    } else {
        freeReplyObject(reply);
        return SQLITE_ERROR;
    }
    freeReplyObject(reply);

    if(idxNum == CURSOR_INDEX_NAMED_EQ) {
        if(rank == -1) return SQLITE_OK;    /* rank indicates item does not exist. */
        redis_vtbl_command_init_arg(&cmd, "SMEMBERS");
        redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s:%s", vtab->key_base, idxStr, value);
        reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
    
    } else if(rank == -1) {
        /* todo fallback to a scan if the value does not exist in the index.
         * This can be improved by determining the approximate rank of the item
         * and then using zrange more efficiently. */
        redis_vtbl_command_init_arg(&cmd, "ZRANGE");
        redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
        redis_vtbl_command_arg(&cmd, "0");
        redis_vtbl_command_arg(&cmd, "-1");
        reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
    
    } else {
        switch(idxNum) {
            case CURSOR_INDEX_NAMED_GT:
                redis_vtbl_command_init_arg(&cmd, "ZRANGE");
                redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
                redis_vtbl_command_arg_fmt(&cmd, "%lld", rank);
                redis_vtbl_command_arg(&cmd, "-1");
                reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
                break;
            case CURSOR_INDEX_NAMED_LT:
                redis_vtbl_command_init_arg(&cmd, "ZRANGE");
                redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
                redis_vtbl_command_arg(&cmd, "0");
                redis_vtbl_command_arg_fmt(&cmd, "%lld", rank);
                reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
                break;
            case CURSOR_INDEX_NAMED_GE:
                redis_vtbl_command_init_arg(&cmd, "ZRANGE");
                redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
                redis_vtbl_command_arg_fmt(&cmd, "%lld", rank);
                redis_vtbl_command_arg(&cmd, "-1");
                reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
                break;
            case CURSOR_INDEX_NAMED_LE:
                redis_vtbl_command_init_arg(&cmd, "ZRANGE");
                redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
                redis_vtbl_command_arg(&cmd, "0");
                redis_vtbl_command_arg_fmt(&cmd, "%lld", rank);
                reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
                break;
        }
    }

    if(!reply) return SQLITE_ERROR;

    err = redis_reply_numeric_array(&cursor->rows, reply);
    freeReplyObject(reply);
    if(err) return SQLITE_ERROR;
    
    return SQLITE_OK;
}
static int redis_vtbl_cursor_filter_index_integer(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, sqlite3_int64 value) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_command cmd;
    redisReply *reply;
    
    vtab = cursor->vtab;

    switch(idxNum) {
        case CURSOR_INDEX_NAMED_EQ:
            redis_vtbl_command_init_arg(&cmd, "SMEMBERS");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s:%lld", vtab->key_base, idxStr, value);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        
        case CURSOR_INDEX_NAMED_GT:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg_fmt(&cmd, "(%lld", value);
            redis_vtbl_command_arg(&cmd, "+inf");
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_NAMED_LT:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg(&cmd, "-inf");
            redis_vtbl_command_arg_fmt(&cmd, "(%lld", value);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_NAMED_GE:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg_fmt(&cmd, "%lld", value);
            redis_vtbl_command_arg(&cmd, "+inf");
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_NAMED_LE:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg(&cmd, "-inf");
            redis_vtbl_command_arg_fmt(&cmd, "%lld", value);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
    }
    
    if(!reply) return SQLITE_ERROR;
    
    err = redis_reply_numeric_array(&cursor->rows, reply);
    freeReplyObject(reply);
    if(err) return SQLITE_ERROR;
    
    return SQLITE_OK;
}
static int redis_vtbl_cursor_filter_index_float(redis_vtbl_cursor *cursor, int idxNum, const char *idxStr, double value) {
    int err;
    redis_vtbl_vtab *vtab;
    redis_vtbl_command cmd;
    redisReply *reply;
    
    vtab = cursor->vtab;

    switch(idxNum) {
        case CURSOR_INDEX_NAMED_EQ:
            redis_vtbl_command_init_arg(&cmd, "SMEMBERS");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s:%f", vtab->key_base, idxStr, value);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        
        case CURSOR_INDEX_NAMED_GT:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg_fmt(&cmd, "(%f", value);
            redis_vtbl_command_arg(&cmd, "+inf");
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_NAMED_LT:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg(&cmd, "-inf");
            redis_vtbl_command_arg_fmt(&cmd, "(%f", value);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_NAMED_GE:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg_fmt(&cmd, "%f", value);
            redis_vtbl_command_arg(&cmd, "+inf");
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
        case CURSOR_INDEX_NAMED_LE:
            redis_vtbl_command_init_arg(&cmd, "ZRANGEBYSCORE");
            redis_vtbl_command_arg_fmt(&cmd, "%s.index:%s", vtab->key_base, idxStr);
            redis_vtbl_command_arg(&cmd, "-inf");
            redis_vtbl_command_arg_fmt(&cmd, "%f", value);
            reply = redis_vtbl_connection_command(&vtab->conn, &cmd);
            break;
    }
    
    if(!reply) return SQLITE_ERROR;
    
    err = redis_reply_numeric_array(&cursor->rows, reply);
    freeReplyObject(reply);
    if(err) return SQLITE_ERROR;
    
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

