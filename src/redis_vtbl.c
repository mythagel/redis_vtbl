#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "list.h"
#include "vector.h"
#include "address.h"
#include "sentinel.h"

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

/*struct vtab : sqlite3_vtab*/
/*{*/
/*    hiredis::context c;*/
/*    std::string key_base;*/
/*    std::vector<std::string> columns;*/
/*    */
/*    vtab(const std::string& ip, int port, const std::string& prefix, const std::string& db, const std::string& table, const std::vector<std::string>& columns)*/
/*     : sqlite3_vtab(), c(ip, port), key_base(prefix + "." + db + "." + table), columns(columns)*/
/*    {*/
/*    }*/
/*    */
/*    sqlite3_int64 generate_id()*/
/*    {*/
/*        using namespace hiredis::commands;*/
/*        return string::incr(c, key("rowid"));*/
/*    }*/
/*    */
/*    std::string key(sqlite3_int64 row_id) const*/
/*    {*/
/*        return key_base + "." + std::to_string(row_id);*/
/*    }*/
/*    std::string key(const std::string& name) const*/
/*    {*/
/*        return key_base + "." + name;*/
/*    }*/
/*};*/

/*struct cursor : sqlite3_vtab_cursor*/
/*{*/
/*    vtab& vtable;*/
/*    std::set<sqlite3_int64> rows;*/
/*    std::set<sqlite3_int64>::const_iterator row_it;*/
/*    std::map<std::string, std::string> record;*/
/*    */
/*    cursor(vtab& vtable)*/
/*     : sqlite3_vtab_cursor(), vtable(vtable), row_it(rows.end())*/
/*    {*/
/*    }*/
/*    */
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
/*        try*/
/*        {*/
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
/*        }*/
/*        catch(const hiredis::error& ex)*/
/*        {*/
/*            // TODO*/
/*            return false;*/
/*        }*/
/*        catch(const hiredis::context::error& ex)*/
/*        {*/
/*            // not recoverable.*/
/*            return false;*/
/*        }*/
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

/*int redisConnect(sqlite3 *db, void *, int argc, const char *const* argv, sqlite3_vtab **ppVtab, char **pzErr)*/
/*{*/
/*    if(argc < 4)*/
/*    {*/
/*        *pzErr = sqlite3_mprintf("Redis address missing.");*/
/*        return SQLITE_ERROR;*/
/*    }*/

/*    try*/
/*    {*/
/*        std::string database_name = argv[1];*/
/*        std::string table_name = argv[2];*/
/*        std::string redis_address = argv[3];*/
/*        std::string prefix = argv[4];*/
/*        std::vector<std::string> column_def{argv+5, argv+argc};*/
/*        std::vector<std::string> columns = column_def;*/

/*        if(column_def.empty())*/
/*        {*/
/*            *pzErr = sqlite3_mprintf("Table with zero columns");*/
/*            return SQLITE_ERROR;*/
/*        }*/

/*        std::string ip = redis_address;*/
/*        int port = 6379;*/
/*        auto pos = redis_address.find(':');*/
/*        if(pos != std::string::npos)*/
/*        {*/
/*            ip = redis_address.substr(0, pos);*/
/*            port = atoi(redis_address.substr(pos+1, std::string::npos).c_str());*/
/*        }*/

/*        try*/
/*        {*/
/*            auto vtable = new vtab(ip, port, prefix, database_name, table_name, columns);*/
/*            *ppVtab = vtable;*/
/*    */
/*            std::stringstream s;*/
/*            s << "CREATE TABLE xxxx(";*/
/*            for(auto it = begin(column_def); it != end(column_def);)*/
/*            {*/
/*                s << *it;*/
/*                ++it;*/
/*                if(it != end(column_def))*/
/*                    s << ", ";*/
/*            }*/
/*            s << ")";*/
/*    */
/*            sqlite3_declare_vtab(db, s.str().c_str());*/
/*        }*/
/*        catch(const hiredis::context::error& ex)*/
/*        {*/
/*            *pzErr = sqlite3_mprintf(ex.what());*/
/*            return SQLITE_ERROR;*/
/*        }*/
/*    }*/
/*    catch(const std::bad_alloc& ex)*/
/*    {*/
/*        return SQLITE_NOMEM;*/
/*    }*/
/*    */
/*    return SQLITE_OK;*/
/*}*/

/*int redisDisconnect(sqlite3_vtab *pVtab)*/
/*{*/
/*    auto vtable = static_cast<vtab*>(pVtab);*/
/*    delete vtable;*/
/*    return SQLITE_OK;*/
/*}*/

/*int redisOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)*/
/*{*/
/*    auto vtable = static_cast<vtab*>(p);*/
/*    */
/*    try*/
/*    {*/
/*        auto pCur = new cursor(*vtable);*/
/*        *ppCursor = pCur;*/
/*    }*/
/*    catch(const std::bad_alloc& ex)*/
/*    {*/
/*        return SQLITE_NOMEM;*/
/*    }*/
/*    return SQLITE_OK;*/
/*}*/
/*int redisClose(sqlite3_vtab_cursor *cur)*/
/*{*/
/*    auto pCur = static_cast<cursor*>(cur);*/
/*    delete pCur;*/
/*    return SQLITE_OK;*/
/*}*/
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

/*int redisBestIndex(sqlite3_vtab *, sqlite3_index_info *)*/
/*{*/
/*    // TODO*/
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

enum {
    CONNECTION_OK,
    CONNECTION_BAD_FORMAT
};

typedef struct redis_vtbl_connection {
    char *service;              /* sentinel service name; optional */
    vector_t addresses;             /* list of redis/sentinel addresses */
} redis_vtbl_connection;

/* Initialise a new connection object from the given configuration.
 *
 * address[:port]
 * A connection to a single redis instance
 *
 * sentinel service-name address[:port][ address[:port]...]
 * A connection to the redis sentinel service at the given addresses. */
static int redis_vtbl_connection_init(redis_vtbl_connection *conn, const char *config) {
    
    while(config && isspace(*config))
        ++config;

    if(!config)
        return CONNECTION_BAD_FORMAT;
    
    conn->service = 0;
    
    /* Validate format:
     * sentinel service-name address[:port][; address[:port]...] */
    if(!strncmp(config, "sentinel ", /* strlen("sentinel ") */9)) {
        int err;
        size_t i;
        list_t tok_list;
        
        err = list_strtok(&tok_list, config, " \t\n");
        if(err) return CONNECTION_BAD_FORMAT;

        if(tok_list.size < 3) {
            list_free(&tok_list);
            return CONNECTION_BAD_FORMAT;
        }

        if(strcmp(list_get(&tok_list, 0), "sentinel")) {
            list_free(&tok_list);
            return CONNECTION_BAD_FORMAT;
        }

        conn->service = list_set(&tok_list, 1, 0);
        vector_init(&conn->addresses, sizeof(address_t), (void(*)(void*))address_free);

        for(i = 2; i < tok_list.size; ++i) {
            address_t address;
            
            err = address_parse(&address, list_get(&tok_list, i), DEFAULT_SENTINEL_PORT);
            if(err) {
                list_free(&tok_list);
                free(conn->service);
                vector_free(&conn->addresses);
                return CONNECTION_BAD_FORMAT;
            }
            vector_push(&conn->addresses, &address);
        }
        list_free(&tok_list);
        
    } else {
        int err;
        address_t address;
        
        err = address_parse(&address, config, DEFAULT_REDIS_PORT);
        if(err)
            return err;
        
        vector_init(&conn->addresses, sizeof(address_t), (void(*)(void*))address_free);
        vector_push(&conn->addresses, &address);
    }
    return CONNECTION_OK;
}

static void redis_vtbl_connection_free(redis_vtbl_connection *conn) {
    free(conn->service);
    vector_free(&conn->addresses);
}

/* A redis backed sqlite3 virtual table implementation.
 * prefix.db.table.[rowid]      = hash of the row data.
 * prefix.db.table.rowid        = sequence from which rowids are generated.
 * prefix.db.table.index.rowid  = master index (set) of rows in the table
 * prefix.db.table.index.x      = additional set(rowid) index(s) for column x*/

static int redis_vtbl_create(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr);
static int redis_vtbl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr);
static int redis_vtbl_bestindex(sqlite3_vtab *pVTab, sqlite3_index_info *pIndexInfo);
static int redis_vtbl_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid);
static int redis_vtbl_disconnect(sqlite3_vtab *pVTab);
static int redis_vtbl_destroy(sqlite3_vtab *pVTab);

static int redis_vtbl_cursor_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor);
static int redis_vtbl_cursor_close(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_filter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv);
static int redis_vtbl_cursor_next(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_eof(sqlite3_vtab_cursor *pCursor);
static int redis_vtbl_cursor_column(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int N);
static int redis_vtbl_cursor_rowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid);

static void redis_vtbl_func_createindex(sqlite3_context *ctx, int argc, sqlite3_value **argv);


/*-----------------------------------------------------------------------------
 * Redis backed Virtual table
 *----------------------------------------------------------------------------*/

struct redis_vtbl_vtab {
    sqlite3_vtab base;
    
    redisContext *c;
    redis_vtbl_connection conn_spec;
    char *key_base;
    list_t columns;
};

int string_append(char** str, const char* src) {
    char* s;
    size_t str_sz = *str ? strlen(*str) : 0;
    size_t src_sz = strlen(src);
    
    s = realloc(*str, str_sz+src_sz+1);
    if(!s) return 1;
    *str = s;
    strcpy(*str + str_sz, src);
    *str[str_sz+src_sz] = 0;
    return 0;
}

/* argv[1]    - database name
 * argv[2]    - table name
 * argv[3]    - address of redis e.g. "127.0.0.1:6379"
 * argv[3]    - address(es) of sentinel "sentinel service-name 127.0.0.1:6379;192.168.1.1"
 * argv[4]    - key prefix
 * argv[5...] - column defintions */
static int redis_vtbl_create(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVTab, char **pzErr) {
    int i;
    list_t column;
    
    if(argc < 6) {
        *pzErr = sqlite3_mprintf("Expected ip:port | sentinel service ip:port [ip:port...], prefix, column0, ...columnN");
        return SQLITE_ERROR;
    }

    const char* db_name = argv[1];
    const char* table = argv[2];
    const char* conn_config = argv[3];
    const char* prefix = argv[4];
    
    list_init(&column, 0);
    for(i = 5; i < argc; ++i)
        list_push(&column, (char*)argv[i]);
    
    char* s = 0;
    string_append(&s, "CREATE TABLE xxxx(");
    for(i = 0; i < column.size; ) {
        string_append(&s, list_get(&column, i));
        ++i;
        if(i < column.size)
            string_append(&s, ", ");
    }
    string_append(&s, ")");
    sqlite3_declare_vtab(db, s);
    free(s);
    
    list_free(&column);
    return SQLITE_ERROR;
}
static int redis_vtbl_connect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr) {
    return redis_vtbl_create(db, pAux, argc, argv, ppVTab, pzErr);
}
static int redis_vtbl_bestindex(sqlite3_vtab *pVTab, sqlite3_index_info *pIndexInfo) {
    return SQLITE_ERROR;
}
static int redis_vtbl_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid) {
    return SQLITE_ERROR;
}
static int redis_vtbl_disconnect(sqlite3_vtab *pVTab) {
    return redis_vtbl_destroy(pVTab);
}
static int redis_vtbl_destroy(sqlite3_vtab *pVTab) {
    return SQLITE_ERROR;
}


/*-----------------------------------------------------------------------------
 * Redis backed Virtual table cursor
 *----------------------------------------------------------------------------*/

struct redis_vtbl_cursor {
    sqlite3_vtab_cursor base;
    
    unsigned int rows;
    sqlite3_int64* row;
    sqlite3_int64* current_row;
    
    list_t columns;
};

static int redis_vtbl_cursor_open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {
    return SQLITE_ERROR;
}
static int redis_vtbl_cursor_close(sqlite3_vtab_cursor *pCursor) {
    return SQLITE_ERROR;
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
 * Utility function to define indexes
 *----------------------------------------------------------------------------*/

static void redis_vtbl_func_createindex(sqlite3_context *ctx, int argc, sqlite3_value **argv) {

}


/*-----------------------------------------------------------------------------
 * sqlite3 extension machinery
 *----------------------------------------------------------------------------*/

static const sqlite3_module redis_vtbl_Module = {
    .iVersion     = 1,
    
    .xCreate      = redis_vtbl_create,
    .xConnect     = redis_vtbl_connect,
    .xBestIndex   = redis_vtbl_bestindex,
    .xUpdate      = redis_vtbl_update,
    .xDisconnect  = redis_vtbl_disconnect,
    .xDestroy     = redis_vtbl_destroy,
    
    .xOpen        = redis_vtbl_cursor_open,
    .xClose       = redis_vtbl_cursor_close,
    .xFilter      = redis_vtbl_cursor_filter,
    .xNext        = redis_vtbl_cursor_next,
    .xEof         = redis_vtbl_cursor_eof,
    .xColumn      = redis_vtbl_cursor_column,
    .xRowid       = redis_vtbl_cursor_rowid
};

int sqlite3_redisvtbl_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    int rc;
    rc = sqlite3_create_module(db, "redis", &redis_vtbl_Module, 0);
    if(rc != SQLITE_OK)
        return rc;
    
    rc = sqlite3_create_function(db, "redis_create_index", 2, SQLITE_ANY, 0, redis_vtbl_func_createindex, 0, 0);
    return rc;
}

#if !SQLITE_CORE
int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    return sqlite3_redisvtbl_init(db, pzErrMsg, pApi);
}
#endif

