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
#include "list.h"
#include <hiredis/hiredis.h>

/*-----------------------------------------------------------------------------
 * Redis helpers
 *----------------------------------------------------------------------------*/

/* vector must already be inited for int64_t members */
int redis_reply_numeric_array(vector_t *vector, redisReply *reply);

/* vector must already be inited for char* members with free */
int redis_reply_string_list(list_t *list, redisReply *reply);

int redis_incr(redisContext *c, const char *key, int64_t *old);

void redis_n_replies(redisContext *c, size_t n, list_t *replies);

typedef int (*redis_reply_predicate_t)(redisReply *);
int redis_check_expected(list_t *replies, ...);
int redis_check_expected_bulk(redisReply *bulk_reply, ...);

int redis_status_reply_p(redisReply *reply);
int redis_status_queued_reply_p(redisReply *reply);
int redis_integer_reply_p(redisReply *reply);
int redis_bulk_reply_p(redisReply *reply);

#endif /* REDIS_H_ */
