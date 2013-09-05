#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h> 

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
    int i;
    (void)NotUsed;
    
    for(i=0; i < argc; ++i)
        printf("%-20s: %-20s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    printf("\n");
    return 0;
}

int exec(sqlite3 *db, const char* sql) {
    int rc;
    char *zErrMsg = 0;
    
    printf("exec: %s\n", sql);
    rc = sqlite3_exec(db, sql, callback, NULL, &zErrMsg);
    if( rc != SQLITE_OK ) {
        fprintf(stderr, "-ERR %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    } else {
        printf("+OK\n");
        return 0;
    }
}

int quiet_exec(sqlite3 *db, const char* sql) {
    int rc;
    char *zErrMsg = 0;
    
    rc = sqlite3_exec(db, sql, NULL, NULL, &zErrMsg);
    if( rc != SQLITE_OK ) {
        fprintf(stderr, "-ERR %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
        return 1;
    } else {
        return 0;
    }
}

int perf_test(sqlite3 *db, int virt) {
    char buf[4096];
    unsigned int i;
    
    if(virt) {
        snprintf(buf, sizeof(buf), 
        "CREATE VIRTUAL TABLE perf USING redis (localhost:6379, prefix,\n"
        "   timestamp          INTEGER,\n"
        "   idx          INTEGER\n"
        ");\n");

    } else {
        snprintf(buf, sizeof(buf), 
        "CREATE TABLE perf (\n"
        "   timestamp          INTEGER,\n"
        "   idx          INTEGER\n"
        ");\n");

    }
    if(exec(db, buf)) return 1;
    
    /* level the playing field - data persists in redis even after drop table */
    exec(db, "delete from perf");
    
    
    for(i = 0; i < 10000; ++i) {
        snprintf(buf, sizeof(buf), 
        "insert into perf "
        "(timestamp, idx) "
        "values (strftime('%%s','now'), %u)", i);
        if(quiet_exec(db, buf)) return 1;
    }
    
    exec(db, "select * from perf where idx = 50");
    exec(db, "select * from perf where idx < 50 limit 10");
    exec(db, "select * from perf where idx <= 50 limit 10");
    
    if(virt)
        exec(db, "select max(timestamp) - min(timestamp) as virt_duration from perf");
    else
        exec(db, "select max(timestamp) - min(timestamp) as sql_duration from perf");
    exec(db, "DROP TABLE perf");
    
    return 0;
}

int main() {
    sqlite3 *db;
    char *zErrMsg = 0;
    int  rc;
    char buf[4096] = {0};

    /* Open database */
    rc = sqlite3_open("test.db", &db);
    if( rc ) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        exit(1);
    } else {
        fprintf(stdout, "Opened database successfully\n");
    }

    sqlite3_enable_load_extension(db, 1);

    rc = sqlite3_load_extension(db, "../src/libredis_vtbl.so", 0, &zErrMsg);
    if( rc ) {
        fprintf(stderr, "Can't load module: %s\n", zErrMsg);
        exit(1);
    } else {
        fprintf(stdout, "Loaded module successfully\n");
    }

    exec(db, "PRAGMA synchronous = OFF");

    /* test0 - sqlite table */
    snprintf(buf, sizeof(buf), 
        "CREATE TABLE IF NOT EXISTS test0 (\n"
        "   blah          VARCHAR(255),\n"
        "   blah2          INTEGER,\n"
        "   blah3     VARCHAR(6)\n"
        ");\n");
    if(exec(db, buf)) goto error;

    /* test1 - sqlite redis backed table */
    snprintf(buf, sizeof(buf), 
        "CREATE VIRTUAL TABLE test1 USING redis (localhost:6379, prefix,\n"
        "   blah          VARCHAR(255),\n"
        "   blah2          INTEGER,\n"
        "   blah3     VARCHAR(6)\n"
        ");\n");
    if(exec(db, buf)) goto error;

    exec(db, "select redis_create_index(blah) from test1");

    snprintf(buf, sizeof(buf), 
        "insert into test0 "
        "(blah, blah2, blah3) "
        "values ('%s','%d', '%s')", 
        "42ea2b19af3a4678b1b71a335cf5a9ce", 1377670786, "1008");
    if(exec(db, buf)) goto error;

    snprintf(buf, sizeof(buf), 
        "insert into test1 "
        "(blah, blah2, blah3) "
        "values ('%s','%d', '%s')", 
        "42ea2b19af3a4678b1b71a335cf5a9ce", 1377670786, "1008");
    if(exec(db, buf)) goto error;

    exec(db, "select * from test0");
    exec(db, "select * from test1");

    exec(db, "DELETE from test0");
    exec(db, "DELETE from test1");


    if(perf_test(db, 0)) goto error;
    if(perf_test(db, 1)) goto error;

// cleanup
    exec(db, "DROP TABLE test0");
    exec(db, "DROP TABLE test1");
    sqlite3_close(db);
    sqlite3_shutdown();
    return 0;
error:
    exec(db, "DROP TABLE test0");
    exec(db, "DROP TABLE test1");
    sqlite3_close(db);
    sqlite3_shutdown();
    return 1;
}

