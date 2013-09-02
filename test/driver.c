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

