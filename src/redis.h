/*
 * redis.h
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#ifndef REDIS_H_
#define REDIS_H_
#include <stdint.h>
#include "vector.h"
#include <hiredis/hiredis.h>

/*-----------------------------------------------------------------------------
 * Redis helpers
 *----------------------------------------------------------------------------*/

/* vector must already be inited for int64_t members */
int redis_reply_numeric_array(vector_t *vector, redisReply *reply);

int redis_incr(redisContext *c, const char *key, int64_t *old);

#endif /* REDIS_H_ */
