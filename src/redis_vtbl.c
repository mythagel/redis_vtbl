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

/*

NULL
INTEGER
REAL
TEXT
BLOB

map relational data to redis.

each row is a hash in redis
column names are keys in the hash

when creating a table, need
argv[1] - database name
argv[2] - table name
argv[3] - address of redis e.g. "127.0.0.1:6379 192.168.1.1 192.168.1.2"
argv[4] - key prefix


base key in redis will be 
prefix.db.table


key for a row
prefix.db.table.{{rowid}}
rowid is retrieved from
integer key prefix.db.table.rowid

KEYS is slow and SCAN is not implemented yet.

Use a SET of keys as the primary index.

lookup by rowid does not need to lookup in the set.

any iteration needs to lookup in the set.

*/

/*struct column_spec*/
/*{*/
/*    std::string name;*/
/*    enum class Type*/
/*    {*/
/*        INTEGER = SQLITE_INTEGER,*/
/*        FLOAT = SQLITE_FLOAT,*/
/*        BLOB = SQLITE_BLOB,*/
/*        TEXT = SQLITE_TEXT,*/
/*    } type;*/
/*    int min;*/
/*    int max;*/
/*};*/

/*struct cursor : sqlite3_vtab_cursor*/
/*{*/
/*    vtab& vtable;*/
/*    std::set<sqlite3_int64> rows;*/
/*    std::set<sqlite3_int64>::const_iterator row_it;*/
/*    std::map<std::string, std::string> record;*/
/*    void get()*/
/*    {*/
/*        using namespace hiredis::commands;*/
/*        */
/*        while(row_it != rows.end())*/
/*        {*/
/*            // Try to retrieve this record.*/
/*            // If it has concurrently been removed, try*/
/*            // the next records until one is found.*/
/*            record = hash::get(vtable.c, vtable.key(*row_it));*/
/*            if(!record.empty())*/
/*                break;*/
/*            */
/*            ++row_it;*/
/*        }*/
/*    }*/
/*    */
/*    */
/*    Retrieve row ids from redis.*/
/*    Set row_id to first returned record.*/
/*    retrieve the first record.*/
/*    */
/*    bool filter()*/
/*    {*/
/*        using namespace hiredis::commands;*/
/*        */
/*            // could retrieve a subset of key ids based on filters...*/
/*            auto key_ids = set::members(vtable.c, vtable.key("rows"));*/
/*            */
/*            rows.clear();*/
/*            for(auto& k : key_ids)*/
/*                rows.insert(std::strtoll(k.c_str(), nullptr, 10));*/
/*            */
/*            row_it = rows.begin();*/
/*            */
/*            if(!eof())*/
/*                get();*/
/*        return true;*/
/*    }*/
/*    */
/*    void next()*/
/*    {*/
/*        ++row_it;*/
/*        get();*/
/*    }*/
/*    */
/*    sqlite3_int64 row_id() const*/
/*    {*/
/*        return *row_it;*/
/*    }*/
/*    */
/*    bool eof() const*/
/*    {*/
/*        return row_it == rows.end();*/
/*    }*/
/*};*/

/*int redisNext(sqlite3_vtab_cursor *cur)*/
/*{*/
/*    auto pCur = static_cast<cursor*>(cur);*/
/*    pCur->next();*/
/*    return SQLITE_OK;*/
/*}*/

/*int redisColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i)*/
/*{*/
/*    auto pCur = static_cast<cursor*>(cur);*/
/*    auto& vtable = pCur->vtable;*/
/*    */
/*    if(static_cast<unsigned>(i) >= vtable.columns.size())*/
/*        return SQLITE_ERROR;*/
/*    */
/*    auto key = vtable.columns[i];*/
/*    auto it = pCur->record.find(key);*/
/*    */
/*    if(it != end(pCur->record))*/
/*        sqlite3_result_text(ctx, it->second.c_str(), it->second.size(), SQLITE_TRANSIENT);*/
/*    else*/
/*        sqlite3_result_null(ctx);*/
/*    */
/*    return SQLITE_OK;*/
/*}*/

/*int redisRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)*/
/*{*/
/*    auto pCur = static_cast<cursor*>(cur);*/
/*    *pRowid = pCur->row_id();*/
/*    return SQLITE_OK;*/
/*}*/

/*int redisEof(sqlite3_vtab_cursor *cur)*/
/*{*/
/*    auto pCur = static_cast<cursor*>(cur);*/
/*    return pCur->eof();*/
/*}*/

/*int redisFilter(sqlite3_vtab_cursor *cur, int , const char *, int , sqlite3_value **)*/
/*{*/
/*    auto pCur = static_cast<cursor*>(cur);*/
/*    pCur->filter(TODO);*/
/*    return SQLITE_OK;*/
/*}*/

/*int redisUpdate(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite_int64 *pRowid)*/
/*{*/
/*    auto vtable = static_cast<vtab*>(pVTab);*/
/*    auto& c = vtable->c;*/
/*    using namespace hiredis::commands;*/
/*    */
/*    if(argc == 1)*/
/*    {*/
/*        // DELETE*/
/*        sqlite3_int64 row_id = sqlite3_value_int64(argv[0]);*/
/*        */
/*        try*/
/*        {*/
/*            // TODO pipeline.*/
/*            key::del(c, vtable->key(row_id));*/
/*            set::rem(c, vtable->key("rows"), std::to_string(row_id));*/
/*        }*/
/*        catch(const hiredis::error& ex)*/
/*        {*/
/*            return SQLITE_ERROR;*/
/*        }*/
/*    }*/
/*    else if(argc > 1 && sqlite3_value_type(argv[0])==SQLITE_NULL)*/
/*    {*/
/*        // INSERT*/
/*        try*/
/*        {*/
/*            sqlite3_int64 row_id = sqlite3_value_int64(argv[0]);*/
/*            if(sqlite3_value_type(argv[1]) == SQLITE_NULL)*/
/*                row_id = vtable->generate_id();*/
/*            else*/
/*                row_id = sqlite3_value_int64(argv[0]);*/
/*        */
/*            std::map<std::string, std::string> record;*/
/*            int i = 0;*/
/*            for(int vidx = 2; vidx < argc; ++i, ++vidx)*/
/*            {*/
/*                if(sqlite3_value_type(argv[vidx]) != SQLITE_NULL)*/
/*                {*/
/*                    std::string v = reinterpret_cast<const char*>(sqlite3_value_text(argv[vidx]));*/
/*                    record.insert(std::make_pair(vtable->columns[i], v));*/
/*                }*/
/*            }*/
/*            */
/*            // TODO pipeline.*/
/*            hash::set(c, vtable->key(row_id), record);*/
/*            set::add(c, vtable->key("rows"), std::to_string(row_id));*/
/*            *pRowid = row_id;*/
/*        }*/
/*        catch(const hiredis::error& ex)*/
/*        {*/
/*            return SQLITE_ERROR;*/
/*        }*/
/*    }*/
/*    else if(argc > 1 && argv[0] && argv[0] == argv[1])*/
/*    {*/
/*        // UPDATE*/
/*        try*/
/*        {*/
/*            sqlite3_int64 row_id = sqlite3_value_int64(argv[0]);*/
/*        */
/*            std::map<std::string, std::string> record;*/
/*            int i = 0;*/
/*            for(int vidx = 2; vidx < argc; ++i, ++vidx)*/
/*            {*/
/*                if(sqlite3_value_type(argv[vidx]) != SQLITE_NULL)*/
/*                {*/
/*                    std::string v = reinterpret_cast<const char*>(sqlite3_value_text(argv[vidx]));*/
/*                    record.insert(std::make_pair(vtable->columns[i], v));*/
/*                }*/
/*            }*/
/*            */
/*            // TODO pipeline.*/
/*            hash::set(c, vtable->key(row_id), record);*/
/*            set::add(c, vtable->key("rows"), std::to_string(row_id));*/
/*        }*/
/*        catch(const hiredis::error& ex)*/
/*        {*/
/*            return SQLITE_ERROR;*/
/*        }*/
/*    }*/
/*    else if(argc > 1 && argv[0] && argv[0] != argv[1])*/
/*    {*/
/*//        sqlite3_int64 old_row_id = sqlite3_value_int64(argv[0]);*/
/*//        sqlite3_int64 row_id = sqlite3_value_int64(argv[1]);*/
/*        */
/*        // unhandled.*/
/*        return SQLITE_ERROR;*/
/*    }*/
/*    else*/
/*    {*/
/*        // eh? unhandled.*/
/*        return SQLITE_ERROR;*/
/*    }*/

/*    return SQLITE_OK;*/
/*}*/

/* A redis backed sqlite3 virtual table implementation.
 * prefix.db.table.[rowid]      = hash of the row data.
 * prefix.db.table.rowid        = sequence from which rowids are generated.
 * prefix.db.table.indices      = master index (set) of indices on the table
 * prefix.db.table.index.rowid  = master index (set) of rows in the table
 * prefix.db.table.index.x      = additional set(rowid) index(s) for column x*/

static int redis_vtbl_create(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr);
static int redis_vtbl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr);
static int redis_vtbl_bestindex(sqlite3_vtab *pVTab, sqlite3_index_info *pIndexInfo);
static int redis_vtbl_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid);
static int redis_vtbl_disconnect(sqlite3_vtab *pVTab);
static int redis_vtbl_destroy(sqlite3_vtab *pVTab);

static void redis_vtbl_func_createindex(sqlite3_context *ctx, int argc, sqlite3_value **argv);

static int redis_vtbl_cursor_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
static int redis_vtbl_cursor_close(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv);
static int redis_vtbl_cursor_next(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_eof(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int N);
static int redis_vtbl_cursor_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid);


/*-----------------------------------------------------------------------------
 * Redis backed Virtual table
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

const char* trim_ws(const char *str) {
    while(*str && isspace(*str))
        ++str;
    return str;
}

/* parse the column name out of an sql definition */
static int parse_column_name(const char *column_def, char **column_name) {
    size_t len;
    
    column_def = trim_ws(column_def);
    if(!*column_def) return 1;
    
    len = strcspn(column_def, " \t\n");
    *column_name = strndup(column_def, len);
    
    if(!*column_name) return 1;
    return 0;
}

enum {
    CURSOR_INDEX_LINEAR,
    CURSOR_INDEX_ROWID,
    CURSOR_INDEX_NAMED
};

typedef struct redis_vtbl_vtab {
    sqlite3_vtab base;
    
    redis_vtbl_connection conn_spec;
    redisContext *c;
    char *key_base;
    char *rowid_key;
    list_t columns;
    vector_t index;
} redis_vtbl_vtab;

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
    
    vtab->rowid_key = strdup(vtab->key_base);
    if(!vtab->rowid_key) {
        free(vtab->key_base);
        redis_vtbl_connection_free(&vtab->conn_spec);
        return SQLITE_NOMEM;
    }
    string_append(&vtab->rowid_key, ".");
    string_append(&vtab->rowid_key, "rowid");
    
    list_init(&vtab->columns, free);
    /* todo push the column names onto the columns list */
    
    vector_init(&vtab->index, sizeof(char*), free);
    /* todo retrieve indices from redis */
    
    return SQLITE_OK;
}

static int redis_vtbl_vtab_generate_rowid(redis_vtbl_vtab *vtab, sqlite3_int64 *rowid) {
    int err;
    int64_t old;
    
    err = redis_incr(vtab->c, vtab->rowid_key, &old);
    if(err) return err;
    
    *rowid = old;
    return 0;
}

static void redis_vtbl_vtab_free(redis_vtbl_vtab *vtab) {
    redis_vtbl_connection_free(&vtab->conn_spec);
    if(vtab->c) redisFree(vtab->c);
    free(vtab->key_base);
    free(vtab->rowid_key);
    list_free(&vtab->columns);
    vector_free(&vtab->index);
}

/* argv[1]    - database name
 * argv[2]    - table name
 * argv[3]    - address of redis e.g. "127.0.0.1:6379"
 * argv[3]    - address(es) of sentinel "sentinel service-name 127.0.0.1:6379;192.168.1.1"
 * argv[4]    - key prefix
 * argv[5...] - column defintions */
static int redis_vtbl_create(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVTab, char **pzErr) {
    int i;
    size_t n;
    int err;
    list_t column;
    char* column_name;
    redis_vtbl_vtab *vtab;
    
    if(argc < 6) {
        *pzErr = sqlite3_mprintf("Bad format; Expected ip[:port] | sentinel service ip[:port] [ip[:port]...], key_prefix, column_def0, ...column_defN");
        return SQLITE_ERROR;
    }

    /* Tasks:
     * create vtab structure
     * Parse arguments
     * Connect to redis
     * Declare table to sqlite
     */

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
        free(vtab);
        return err;
    }
    
    /* Attempt to connect to redis.
     * Will either connect via sentinel
     * or directly based on the configuration. */
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
    
    /* fill column names (todo and types) */
    for(n = 0; n < column.size; ++n) {
        err = parse_column_name(list_get(&column, n), &column_name);
        if(err) {
            redis_vtbl_vtab_free(vtab);
            free(vtab);
            return SQLITE_ERROR;
        }
        
        list_push(&vtab->columns, column_name);
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
    
    sqlite3_declare_vtab(db, s);
    free(s);
    
    list_free(&column);
    
    /* todo: retrieve indices from redis */
    
    *ppVTab = (sqlite3_vtab*)vtab;
    return SQLITE_OK;
}

static int redis_vtbl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr) {
    return redis_vtbl_create(db, pAux, argc, argv, ppVTab, pzErr);
}

static int eq_rowid_p(const struct sqlite3_index_constraint *constraint) {
    return constraint->op == SQLITE_INDEX_CONSTRAINT_EQ && constraint->iColumn == -1;
}

static int redis_vtbl_bestindex(sqlite3_vtab *pVTab, sqlite3_index_info *pIndexInfo) {
    redis_vtbl_vtab *vtab;
    int i;
    int cons_idx = 0;
    
    vtab = (redis_vtbl_vtab*)pVTab;
    
    pIndexInfo->idxNum = CURSOR_INDEX_LINEAR;
    pIndexInfo->estimatedCost = 1000.0;         /* todo retrieve row estimate from redis on connect. */
    
    for(i = 0; i < pIndexInfo->nConstraint; ++i) {
        const char *column_name;
        const char *index_name;
        struct sqlite3_index_constraint *constraint = &pIndexInfo->aConstraint[i];
        if(!constraint->usable) continue;
        
        if(eq_rowid_p(constraint)) {
            pIndexInfo->idxNum = CURSOR_INDEX_ROWID;
            pIndexInfo->estimatedCost = 1.0;
            break;
        }
        
        if(constraint->iColumn == -1) {
            // other constraints on the rowid
        } else {
        
            /* lookup the column name by id
             * Determine if that column is indexed */
            column_name = list_get(&vtab->columns, constraint->iColumn);
            if(!column_name) return SQLITE_ERROR;
            
            index_name = vector_bsearch(&vtab->index, column_name, strcmp);
            
            /*pIndexInfo->aConstraintUsage[x].argvIndex = ++cons_idx;*/
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

static int redis_vtbl_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid) {
    return SQLITE_ERROR;
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
    
    /* todo create the index 
     * Seems it may be necessary to pass some data through the context. */
    
    sqlite3_result_int(ctx, 1);
}

/*-----------------------------------------------------------------------------
 * Redis backed Virtual table cursor
 *----------------------------------------------------------------------------*/

typedef struct redis_vtbl_cursor {
    sqlite3_vtab_cursor base;
    redis_vtbl_vtab *vtab;
    
    vector_t rows;
    sqlite3_int64 *current_row;
    
    list_t columns;
} redis_vtbl_cursor;

static int redis_vtbl_cursor_init(redis_vtbl_cursor *cur, redis_vtbl_vtab *vtab) {
    memset(&cur->base, 0, sizeof(sqlite3_vtab_cursor));
    cur->vtab = vtab;
    vector_init(&cur->rows, sizeof(int64_t), 0);
    cur->current_row = 0;
    
    list_init(&cur->columns, free);
    
    return SQLITE_OK;
}

static void redis_vtbl_cursor_free(redis_vtbl_cursor *cur) {
    vector_free(&cur->rows);
    list_free(&cur->columns);
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
    return SQLITE_ERROR;
}

static int redis_vtbl_cursor_next(sqlite3_vtab_cursor *pCursor) {
    return SQLITE_ERROR;
}

static int redis_vtbl_cursor_eof(sqlite3_vtab_cursor *pCursor) {
    return SQLITE_ERROR;
}

static int redis_vtbl_cursor_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int N) {
    return SQLITE_ERROR;
}

static int redis_vtbl_cursor_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
    return SQLITE_ERROR;
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

