#include "connection.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>


int main() {
    int err;
    redis_vtbl_connection conn;
    
    err = redis_vtbl_connection_init(&conn, "127.0.0.1");
    if(err) {
        return 1;
    }
    redis_vtbl_connection_free(&conn);

    err = redis_vtbl_connection_init(&conn, "sentinel db 127.0.0.1 192.168.1.1 192.168.1.2:26379");
    if(err) {
        return 1;
    }
    redis_vtbl_connection_free(&conn);

    err = redis_vtbl_connection_init(&conn, "127.0.0.1");
    if(err) {
        return 1;
    }
    
    err = redis_vtbl_connection_connect(&conn);
    if(err) {
        printf("Unable to connect to redis.\n");
        redis_vtbl_connection_free(&conn);
        return 1;
    } else {
        printf("connected to redis.\n");
    }
    redis_vtbl_connection_free(&conn);
    
    return 0;
}
