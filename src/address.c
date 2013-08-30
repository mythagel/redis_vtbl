/*
 * address.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "address.h"
#include <string.h>
#include <errno.h>
#include <stdlib.h>

/*-----------------------------------------------------------------------------
 * IP Address / port type
 *----------------------------------------------------------------------------*/

int address_init(address_t *addr, const char *host, int port) {
    if(strlen(host) >= ADDRESS_MAX_HOST_LEN) return ADDRESS_BAD_FORMAT;
    
    strcpy(addr->host, host);
    addr->port = port;
    return ADDRESS_OK;
}

int address_cmp(const address_t *l, const address_t *r) {
    int diff = strcmp(l->host, r->host);
    if(diff) return diff;
    if(l->port < r->port) return -1;
    if(l->port > r->port) return 1;
    return 0;
}

/* Validate address format:
 * address | address:port */
int address_parse(address_t *addr, const char *address_spec, int default_port) {
    char* pos;
    
    pos = strchr(address_spec, ':');
    if(pos) {
        int port;
        char *end;
        size_t addr_len;
        
        addr_len = pos - address_spec;
        ++pos;
        if(*pos == 0)
            return ADDRESS_BAD_FORMAT;
        
        errno = 0;
        port = strtol(pos, &end, 10);
        if(errno || !port || *end)      /* out-of-range | zero | rubbish characters */
            return ADDRESS_BAD_FORMAT;
        
        if(addr_len >= ADDRESS_MAX_HOST_LEN) return ADDRESS_BAD_FORMAT;
        strncpy(addr->host, address_spec, addr_len);
        addr->host[addr_len] = 0;
        addr->port = port;
        return ADDRESS_OK;
    } else {
        return address_init(addr, address_spec, default_port);
    }
}

void address_free(address_t *address) {
    (void)address;
    /*noop*/
}

