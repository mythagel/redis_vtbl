/*
 * connection.c
 *
 *  Created on: 2013-09-02
 *      Author: nicholas
 */

#include "connection.h"
#include "sentinel.h"
#include "list.h"
#include "address.h"
#include <ctype.h>
#include <string.h>
#include <stdlib.h>

/* Initialise a new connection object from the given configuration.
 *
 * address[:port]
 * A connection to a single redis instance
 *
 * sentinel service-name address[:port][ address[:port]...]
 * A connection to the redis sentinel service at the given addresses. */
int redis_vtbl_connection_init(redis_vtbl_connection *conn, const char *config) {
    
    while(*config && isspace(*config))
        ++config;

    if(!*config)
        return CONNECTION_BAD_FORMAT;
    
    conn->service = 0;
    conn->errstr[0] = 0;
    
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
            err = vector_push(&conn->addresses, &address);
            if(err) {
                list_free(&tok_list);
                free(conn->service);
                vector_free(&conn->addresses);
                return CONNECTION_ENOMEM;
            }
        }
        list_free(&tok_list);
        
    } else {
        int err;
        address_t address;
        
        err = address_parse(&address, config, DEFAULT_REDIS_PORT);
        if(err) {
            return CONNECTION_BAD_FORMAT;
        }
        
        vector_init(&conn->addresses, sizeof(address_t), (void(*)(void*))address_free);
        err = vector_push(&conn->addresses, &address);
        if(err) {
            vector_free(&conn->addresses);
            return CONNECTION_ENOMEM;
        }
    }
    return CONNECTION_OK;
}

int redis_vtbl_connection_connect(redis_vtbl_connection *conn, redisContext **c) {
    int err;
    
    if(conn->service) {     /* sentinel */
        err = redisSentinelConnect(&conn->addresses, conn->service, c);
        return err;
    
    } else {                /* redis */
        address_t *redis;
        
        redis = vector_get(&conn->addresses, 0);
        if(redis) {
            *c = redisConnect(redis->host, redis->port);
            if(!*c) return 1;
            if((*c)->err) {
                strcpy(conn->errstr, (*c)->errstr);
                redisFree(*c);
                *c = 0;
                return 1;
            }
            return 0;
        } else {
            return 1;   /* logic error */
        }
    }
}

void redis_vtbl_connection_free(redis_vtbl_connection *conn) {
    free(conn->service);
    vector_free(&conn->addresses);
}

