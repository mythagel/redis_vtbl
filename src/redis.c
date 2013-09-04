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
    
    if(reply->type != REDIS_REPLY_ARRAY) return 1;
    
    vector_reserve(vector, reply->elements);
    for(i = 0; i < reply->elements; ++i) {
        redisReply *element;
        element = reply->element[i];
        
        if(element->type != REDIS_REPLY_STRING) return 1;
        
        errno = 0;
        n = strtoll(element->str, &end, 10);
        if(errno || *end) {      /* out-of-range | rubbish characters */
            continue;
        }

        err = vector_push(vector, &n);
        if(err) return 1;
    }
    return 0;
}

int redis_reply_string_list(list_t *list, redisReply *reply) {
    size_t i;
    
    if(reply->type != REDIS_REPLY_ARRAY) return 1;
    
    for(i = 0; i < reply->elements; ++i) {
        redisReply *element;
        element = reply->element[i];
        
        if(element->type == REDIS_REPLY_STRING) {
            list_push(list, strdup(element->str));

        } else if(element->type == REDIS_REPLY_NIL) {
            list_push(list, 0);

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

void redis_n_replies(redisContext *c, size_t n, list_t *replies) {
    redisReply *reply;
    size_t i;
    
    for(i = 0; i < n; ++i) {
        redisGetReply(c, (void**)&reply);
        list_push(replies, reply);
    }
}

int redis_check_expected(list_t *replies, ...)
{
    va_list args;
    size_t i;
    
    va_start(args, replies);
    for (i = 0; i < replies->size; ++i) {
        redisReply *reply;
        redis_reply_predicate_t pred;
        
        reply = replies->data[i];
        pred = va_arg(args, redis_reply_predicate_t);
        
        if(!pred(reply)) {
            va_end(args);
            return 1;
        }
    }
    
    va_end(args);
    return 0;
}

int redis_check_expected_bulk(redisReply *bulk_reply, ...) {
    va_list args;
    size_t i;
    
    va_start(args, bulk_reply);
    for (i = 0; i < bulk_reply->elements; ++i) {
        redisReply *reply;
        redis_reply_predicate_t pred;
        
        reply = bulk_reply->element[i];
        pred = va_arg(args, redis_reply_predicate_t);
        
        if(!pred(reply)) {
            va_end(args);
            return 1;
        }
    }
    
    va_end(args);
    return 0;
}

int redis_status_reply_p(redisReply *reply) {
    return reply->type == REDIS_REPLY_STATUS;
}
int redis_status_queued_reply_p(redisReply *reply) {
    return reply->type == REDIS_REPLY_STATUS && !strcmp(reply->str, "QUEUED");
}
int redis_integer_reply_p(redisReply *reply) {
    return reply->type == REDIS_REPLY_INTEGER;
}
int redis_bulk_reply_p(redisReply *reply) {
    return reply->type == REDIS_REPLY_ARRAY;
}

