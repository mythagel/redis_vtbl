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

/* list must already be inited with free */
int redis_reply_string_list(list_t *list, redisReply *reply);

int redis_incr(redisContext *c, const char *key, int64_t *old);

/* read n replies from context and store in replies. 
 * list must already be inited with freeReplyObject */
void redis_n_replies(redisContext *c, size_t n, list_t *replies);

/* Check that the replies provided match the given predicates.
 * predicates return 1 for match and 0 for not matched */
typedef int (*redis_reply_predicate_t)(redisReply *);
int redis_check_expected(list_t *replies, ...);
int redis_check_expected_bulk(redisReply *bulk_reply, ...);

/* any valid status */
int redis_status_reply_p(redisReply *reply);
/* +QUEUED status */
int redis_status_queued_reply_p(redisReply *reply);
/* any integer value */
int redis_integer_reply_p(redisReply *reply);
/* any array (bulk) value */
int redis_bulk_reply_p(redisReply *reply);

#endif /* REDIS_H_ */
