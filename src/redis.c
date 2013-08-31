/*
 * redis.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "redis.h"
#include <stdlib.h>
#include <errno.h>

/*-----------------------------------------------------------------------------
 * Redis helpers
 *----------------------------------------------------------------------------*/

int redis_reply_numeric_array(vector_t *vector, redisReply *reply) {
    size_t i;
    int64_t n;
    char *end;
    
    if(reply->type != REDIS_REPLY_ARRAY)
        return 1;
    
    vector_init(vector, sizeof(int64_t), 0);
    vector_reserve(vector, reply->elements);
    for(i = 0; i < reply->elements; ++i) {
        if(reply->type == REDIS_REPLY_STRING) {
            errno = 0;
            n = strtoll(reply->str, &end, 10);
            if(errno || *end) {      /* out-of-range | rubbish characters */
                vector_free(vector);
                return 1;
            }
            vector_push(vector, &n);
        } else {
            vector_free(vector);
            return 1;
        }
    }
    return 0;
}