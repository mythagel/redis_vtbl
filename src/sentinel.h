/*
 * sentinel.h
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#ifndef SENTINEL_H_
#define SENTINEL_H_
#include "vector.h"
#include <hiredis/hiredis.h>

#define DEFAULT_REDIS_PORT 6379
#define DEFAULT_SENTINEL_PORT 26379

enum {
    SENTINEL_OK,
    SENTINEL_ERROR,
    SENTINEL_MASTER_NAME_UNKNOWN,
    SENTINEL_MASTER_UNKNOWN
};

redisContext* redisSentinelConnect(vector_t *sentinels, const char *service);

#endif /* SENTINEL_H_ */
