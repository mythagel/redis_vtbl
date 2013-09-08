/*
 * connection.h
 *
 *  Created on: 2013-09-02
 *      Author: nicholas
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_
#include "vector.h"
#include "list.h"
#include <hiredis/hiredis.h>

enum {
    CONNECTION_OK,
    CONNECTION_ERROR,
    CONNECTION_BAD_FORMAT,
    CONNECTION_ENOMEM
};

typedef struct redis_vtbl_connection {
    char *service;                  /* sentinel service name; optional */
    vector_t addresses;             /* list of redis/sentinel addresses */
    char errstr[128];               /* error string from redis */
    redisContext *c;
} redis_vtbl_connection;

typedef struct redis_vtbl_command {
    list_t args;
} redis_vtbl_command;

int  redis_vtbl_command_init(redis_vtbl_command *cmd);
int  redis_vtbl_command_append(redis_vtbl_command *cmd, const char *arg);
int  redis_vtbl_command_append_fmt(redis_vtbl_command *cmd, const char *fmt, ...);
void redis_vtbl_command_free(redis_vtbl_command *cmd);

/* Initialise a new connection object from the given configuration.
 *
 * address[:port]
 * A connection to a single redis instance
 *
 * sentinel service-name address[:port][ address[:port]...]
 * A connection to the redis sentinel service at the given addresses. */
int  redis_vtbl_connection_init(redis_vtbl_connection *conn, const char *config);

/* Attempt to connect to the specified redis / sentinel host(s)
 * Returns error code from redisSentinelConnect if conn->service
 * otherwise 0 on success & nonzero on error. conn->errstr is filled if 
 * provided by redis. */
int  redis_vtbl_connection_connect(redis_vtbl_connection *conn);

/* both of these commands take ownership of the cmd object */
redisReply* redis_vtbl_connection_command(redis_vtbl_connection *conn, redis_vtbl_command *cmd);
int  redis_vtbl_connection_command_enqueue(redis_vtbl_connection *conn, redis_vtbl_command *cmd);

void redis_vtbl_connection_free(redis_vtbl_connection *conn);

#endif /* CONNECTION_H_ */
