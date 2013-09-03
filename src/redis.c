/*
 * redis.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "redis.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>

/*-----------------------------------------------------------------------------
 * Redis helpers
 *----------------------------------------------------------------------------*/

int redis_reply_numeric_array(vector_t *vector, redisReply *reply) {
    int err;
    size_t i;
    int64_t n;
    char *end;
    
    if(reply->type != REDIS_REPLY_ARRAY)
        return 1;
    
    vector_reserve(vector, reply->elements);
    for(i = 0; i < reply->elements; ++i) {
        if(reply->type == REDIS_REPLY_STRING) {
            errno = 0;
            n = strtoll(reply->str, &end, 10);
            if(errno || *end) {      /* out-of-range | rubbish characters */
                continue;
            }

            err = vector_push(vector, &n);
            if(err) return 1;
        } else {
            return 1;
        }
    }
    return 0;
}

int redis_reply_string_array(vector_t *vector, redisReply *reply) {
    int err;
    size_t i;
    char *str;
    
    if(reply->type != REDIS_REPLY_ARRAY)
        return 1;
    
    vector_reserve(vector, reply->elements);
    for(i = 0; i < reply->elements; ++i) {
        if(reply->type == REDIS_REPLY_STRING) {

            str = strdup(reply->str);
            if(!str) return 1;
            
            err = vector_push(vector, &str);
            if(err) return 1;
        } else if(reply->type == REDIS_REPLY_NIL) {
            str = 0;
            err = vector_push(vector, &str);
            if(err) return 1;
        } else {
            return 1;
        }
    }
    return 0;
}

int redis_incr(redisContext *c, const char *key, int64_t *old) {
    redisReply *reply;
    
    reply = redisCommand(c, "INCR %s", key);
    if(!reply) return 1;
    if(reply->type != REDIS_REPLY_INTEGER) {
        freeReplyObject(reply);
        return 1;
    }
    *old = reply->integer;
    freeReplyObject(reply);
    return 0;
}

