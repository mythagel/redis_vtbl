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

static const size_t MAX_STRING_SIZE = 2048;

/*-----------------------------------------------------------------------------
 * IP Address / port type
 *----------------------------------------------------------------------------*/

int address_init(address_t *addr, const char *host, int port) {
    addr->host = strndup(host, MAX_STRING_SIZE);
    if(!addr->host)
        return ADDRESS_ENOMEM;
    
    addr->port = port;
    return ADDRESS_OK;
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
        
        addr->host = strndup(address_spec, addr_len > MAX_STRING_SIZE ? MAX_STRING_SIZE : addr_len);
        if(!addr->host)
            return ADDRESS_ENOMEM;
        addr->port = port;
        return ADDRESS_OK;
    } else {
        return address_init(addr, address_spec, default_port);
    }
}

void address_free(address_t *address) {
    free(address->host);
}

