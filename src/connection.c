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
#include "redis.h"
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

static const char* trim_ws(const char *str) {
    while(*str && isspace(*str))
        ++str;
    return str;
}

int redis_vtbl_command_init(redis_vtbl_command *cmd) {
    list_init(&cmd->args, free);
    return CONNECTION_OK;
}
int redis_vtbl_command_init_arg(redis_vtbl_command *cmd, const char *arg) {
    int err;
    
    err = redis_vtbl_command_init(cmd);
    if(err) return err;
    
    err = redis_vtbl_command_arg(cmd, arg);
    if(err) {
        redis_vtbl_command_free(cmd);
        return err;
    }
    return CONNECTION_OK;
}
int redis_vtbl_command_arg(redis_vtbl_command *cmd, const char *arg) {
    int err;
    char *str;
    str = strdup(arg);
    if(!str) return CONNECTION_ENOMEM;
    err = list_push(&cmd->args, str);
    if(err) {
        free(str);
        return CONNECTION_ENOMEM;
    }
    return CONNECTION_OK;
}
int redis_vtbl_command_arg_fmt(redis_vtbl_command *cmd, const char *arg, ...) {
    int err;
    char *str;
    va_list args;
    size_t size;
    
    size = strlen(arg) * 1.618;
    str = malloc(size);
    if(!str) return CONNECTION_ENOMEM;
    
    va_start(args, arg);
    err = vsnprintf(str, size, arg, args);
    va_end(args);
    if(err < 0) {
        free(str);
        return CONNECTION_ERROR;
    }
    
    if((unsigned)err >= size) {
        char *s;
        size = err+1;
        s = realloc(str, size);
        if(!s) {
            free(str);
            return CONNECTION_ENOMEM;
        }
        str = s;
        
        va_start(args, arg);
        err = vsnprintf(str, size, arg, args);
        va_end(args);
        if(err < 0) {
            free(str);
            return CONNECTION_ERROR;
        }
    }
    
    err = list_push(&cmd->args, str);
    if(err) {
        free(str);
        return CONNECTION_ENOMEM;
    }
    
    return CONNECTION_OK;
}
void redis_vtbl_command_free(redis_vtbl_command *cmd) {
    list_free(&cmd->args);
}

/* Initialise a new connection object from the given configuration.
 *
 * address[:port]
 * A connection to a single redis instance
 *
 * sentinel service-name address[:port][ address[:port]...]
 * A connection to the redis sentinel service at the given addresses. */
int redis_vtbl_connection_init(redis_vtbl_connection *conn, const char *config) {
    
    config = trim_ws(config);

    if(!*config) return CONNECTION_BAD_FORMAT;
    
    conn->service = 0;
    conn->errstr[0] = 0;
    conn->c = 0;
    
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
                address_free(&address);
                vector_free(&conn->addresses);
                free(conn->service);
                list_free(&tok_list);
                return CONNECTION_ENOMEM;
            }
        }
        list_free(&tok_list);
        
    } else {
        int err;
        address_t address;
        
        err = address_parse(&address, config, DEFAULT_REDIS_PORT);
        if(err) return CONNECTION_BAD_FORMAT;
        
        vector_init(&conn->addresses, sizeof(address_t), (void(*)(void*))address_free);
        err = vector_push(&conn->addresses, &address);
        if(err) {
            address_free(&address);
            vector_free(&conn->addresses);
            return CONNECTION_ENOMEM;
        }
    }
    
    vector_init(&conn->cmd_queue, sizeof(redis_vtbl_command), (void (*)(void *))redis_vtbl_command_free);
    
    return CONNECTION_OK;
}

int redis_vtbl_connection_connect(redis_vtbl_connection *conn) {
    int err;
    
    conn->errstr[0] = 0;
    
    if(conn->c) redisFree(conn->c);
    conn->c = 0;
    
    if(conn->service) {     /* sentinel */
        err = redisSentinelConnect(&conn->addresses, conn->service, &conn->c);
        if(err) {
            switch(err) {
                case SENTINEL_ERROR:
                    strcpy(conn->errstr, "Unknown error");
                    break;
                case SENTINEL_MASTER_NAME_UNKNOWN:
                    strcpy(conn->errstr, "Master name unknown");
                    break;
                case SENTINEL_MASTER_UNKNOWN:
                    strcpy(conn->errstr, "Master Unknown");
                    break;
                case SENTINEL_UNREACHABLE:
                    strcpy(conn->errstr, "Unreachable");
                    break;
            }
        }
        return err;
    
    } else {                /* redis */
        address_t *redis;
        
        redis = vector_get(&conn->addresses, 0);
        if(!redis) return CONNECTION_ERROR;   /* logic error */
        
#ifndef QUIET
        fprintf(stderr, "redis_vtbl: Connecting to redis %s:%d... ", redis->host, redis->port);
#endif
        conn->c = redisConnect(redis->host, redis->port);
        if(!conn->c) return CONNECTION_ENOMEM;
        if(conn->c->err) {
#ifndef QUIET
            fprintf(stderr, "-NOK %s\n", conn->c->errstr);
#endif
            strcpy(conn->errstr, conn->c->errstr);
            redisFree(conn->c);
            conn->c = 0;
            return CONNECTION_ERROR;
        }
#ifndef QUIET
        fprintf(stderr, "+OK\n");
#endif
        return CONNECTION_OK;
    }
}

static redisReply* redis_vtbl_connection_command_impl(redis_vtbl_connection *conn, redis_vtbl_command *cmd, int retries) {
    int err;
    redisReply *reply;
    
    reply = redisCommandArgv(conn->c, cmd->args.size, (const char**)cmd->args.data, 0);
    if(!reply) {
#ifndef QUIET
        fprintf(stderr, "-ERR %s\n", conn->c->errstr);
#endif
        if(retries == 0) {
            /* free the command, we were unable to reconnect. */
            redis_vtbl_command_free(cmd);
            return 0;
        }
        
        /* reconnect / resend */
        err = redis_vtbl_connection_connect(conn);
        if(err) {
#ifndef QUIET
            if(conn->service) {
                fprintf(stderr, "Sentinel: %s", conn->errstr);
            } else {
                fprintf(stderr, "Redis: %s", conn->errstr);
            }
#endif
            /* free the command, we were unable to reconnect. */
            redis_vtbl_command_free(cmd);
            return 0;
        }
        
        return redis_vtbl_connection_command_impl(conn, cmd, --retries);
    }
    
    redis_vtbl_command_free(cmd);
    return reply;
}

redisReply* redis_vtbl_connection_command(redis_vtbl_connection *conn, redis_vtbl_command *cmd) {
    return redis_vtbl_connection_command_impl(conn, cmd, 3);
}

void redis_vtbl_connection_command_enqueue(redis_vtbl_connection *conn, redis_vtbl_command *cmd) {
    /* optimistically pass the message to hiredis... */
    redisAppendCommandArgv(conn->c, cmd->args.size, (const char**)cmd->args.data, 0);
    /* ... but queue it incase it needs to be resent */
    vector_push(&conn->cmd_queue, cmd);
}

static int redis_vtbl_connection_read_queued_impl(redis_vtbl_connection *conn, list_t *replies, int retries) {
    int err;
    size_t i;
    
    if(conn->cmd_queue.size == 0) return CONNECTION_ERROR;
    
    err = redis_n_replies(conn->c, conn->cmd_queue.size, replies);
    if(err) {
#ifndef QUIET
        fprintf(stderr, "-ERR %s\n", conn->c->errstr);
#endif
        if(retries == 0) {
            /* clear the command queue, we were unable to reconnect. */
            vector_clear(&conn->cmd_queue);
            return CONNECTION_ERROR;
        }
        
        /* reconnect / resend / reread */
        err = redis_vtbl_connection_connect(conn);
        if(err) {
#ifndef QUIET
            if(conn->service) {
                fprintf(stderr, "Sentinel: %s", conn->errstr);
            } else {
                fprintf(stderr, "Redis: %s", conn->errstr);
            }
#endif
            /* clear the command queue, we were unable to reconnect. */
            vector_clear(&conn->cmd_queue);
            return CONNECTION_ERROR;
        }
        
        /* resend the queued commands on the new connection. */
        for(i = 0; i < conn->cmd_queue.size; ++i) {
            redis_vtbl_command *cmd;
            
            cmd = vector_get(&conn->cmd_queue, i);
            redisAppendCommandArgv(conn->c, cmd->args.size, (const char**)cmd->args.data, 0);
        }
        
        return redis_vtbl_connection_read_queued_impl(conn, replies, --retries);
    }
    
    vector_clear(&conn->cmd_queue);
    return CONNECTION_OK;
}

int redis_vtbl_connection_read_queued(redis_vtbl_connection *conn, list_t *replies) {
    return redis_vtbl_connection_read_queued_impl(conn, replies, 3);
}

void redis_vtbl_connection_free(redis_vtbl_connection *conn) {
    free(conn->service);
    vector_free(&conn->addresses);
    if(conn->c) redisFree(conn->c);
    vector_free(&conn->cmd_queue);
}

