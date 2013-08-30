/*
 * sentinel.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "sentinel.h"
#include "address.h"
#include <string.h>

static int redisSentinelGetMasterAddress(redisContext *cs, const char *service, address_t* address);
static int redisSentinelRefreshSentinelList(redisContext *cs, const char *service, vector_t *sentinels);

/* Sentinel client algorithm
 * track:
 *   no sentinel reachable -> error seninel down
 *   all null responses    -> master unknown
 *
 * for each sentinel address
 *   redisConnectWithTimeout(ip, port, short tv);
 *     fail -> continue
 *   SENTINEL get-master-addr-by-name master-name
 *     fail -> continue
 *   master ip:port received
 *   connect to master
 *     fail -> continue
 *   if sentinel not at head of list
 *     swap head, current
 *   return connected master context
 * return appropriate error code*/

int redisSentinelConnect(vector_t *sentinels, const char *service, redisContext **master_context) {
    redisContext *c = 0;
    
    size_t i;
    struct timeval tv;
    
    int sentinels_reached = 0;
    int name_unknown = 0;
    int master_unknown = 0;
    
    tv.tv_sec = 0;
    tv.tv_usec = 250000;        /* 250ms */
    
    *master_context = 0;
    
    for(i = 0; i < sentinels->size; ++i) {
        int err;
        address_t *sentinel = vector_get(sentinels, i);
        redisContext *cs = 0;
        address_t master;
        
        cs = redisConnectWithTimeout(sentinel->host, sentinel->port, tv);
        if(!cs)
            continue;       /* todo: Is there any point continuing if we're oom here? */
        if(cs->err) {
            redisFree(cs);
            continue;
        }
        
        err = redisSentinelGetMasterAddress(cs, service, &master);
        if(err) {
            switch(err) {
                case SENTINEL_ERROR:
                    break;
                
                case SENTINEL_MASTER_NAME_UNKNOWN:
                    ++name_unknown;
                    break;
                
                case SENTINEL_MASTER_UNKNOWN:
                    ++master_unknown;
                    break;
            }
            redisFree(cs);
            continue;
        }
        
        /* Refresh the sentinel list from the answering sentinel.
         * This is safe because it only appends to the end of the list
         * and no pointers to the list are maintained (only the index) */
        redisSentinelRefreshSentinelList(cs, service, sentinels);
        
        redisFree(cs);
        ++sentinels_reached;
        
        c = redisConnect(master.host, master.port);
        address_free(&master);
        
        if(!c)
            continue;       /* todo: Is there any point continuing if we're oom here? */
        if(c->err) {
            redisFree(c);
            c = 0;
            continue;
        }
        
        /* we are connected to master. */
        *master_context = c;
        break;
    }
    
    if(*master_context)    return SENTINEL_OK;
    if(!sentinels_reached) return SENTINEL_UNREACHABLE;
    if(master_unknown)     return SENTINEL_MASTER_UNKNOWN;
    if(name_unknown)       return SENTINEL_MASTER_NAME_UNKNOWN;
    /* ... */              return SENTINEL_ERROR;
}

static int redisSentinelGetMasterAddress(redisContext *cs, const char *service, address_t* address) {
    redisReply* reply;
    int err = SENTINEL_ERROR;
    
    reply = redisCommand(cs, "SENTINEL get-master-addr-by-name %s", service);
    if(!reply)
        return SENTINEL_ERROR;
    
    if(reply->type == REDIS_REPLY_STRING) {
        err = address_parse(address, reply->str, DEFAULT_REDIS_PORT);
        err = err ? SENTINEL_ERROR : SENTINEL_OK;
        
    } else if (reply->type == REDIS_REPLY_NIL) {
        err = SENTINEL_MASTER_NAME_UNKNOWN;
        
    } else if (reply->type == REDIS_REPLY_ERROR) {
        if(!strcmp(reply->str, "-IDONTKNOW"))
            err = SENTINEL_MASTER_UNKNOWN;
    }
    
    freeReplyObject(reply);
    return err;
}

static int redisSentinelRefreshSentinelList(redisContext *cs, const char *service, vector_t *sentinels) {
    redisReply* reply;
    int err = SENTINEL_ERROR;
    
    reply = redisCommand(cs, "SENTINEL sentinels %s", service);
    if(!reply)
        return SENTINEL_ERROR;
    
    if(reply->type == REDIS_REPLY_ARRAY) {
        size_t i;
        for(i = 0; i < reply->elements; ++i) {
            if(reply->element[i]->type == REDIS_REPLY_STRING) {
                address_t *exists;
                address_t sentinel;
                
                err = address_parse(&sentinel, reply->str, DEFAULT_REDIS_PORT);
                if(err) continue;
                
                exists = vector_find(sentinels, &sentinel, (int (*)(void *, void *))address_cmp);
                
                if(!exists) vector_push(sentinels, &sentinel);
            }
        }
    }
    
    freeReplyObject(reply);
    return err;
}

