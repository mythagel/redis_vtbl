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
#include <string.h>

/*-----------------------------------------------------------------------------
 * Redis helpers
 *----------------------------------------------------------------------------*/

int redis_reply_numeric_array(vector_t *vector, redisReply *reply);

#endif /* REDIS_H_ */
