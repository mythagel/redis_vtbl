/*
 * address.h
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#ifndef ADDRESS_H_
#define ADDRESS_H_

/*-----------------------------------------------------------------------------
 * IP Address / port type
 *----------------------------------------------------------------------------*/

#define ADDRESS_MAX_HOST_LEN 256     /* IPv4 15; IPv6 45; DNS 253 */

enum {
    ADDRESS_OK = 0,
    ADDRESS_BAD_FORMAT
};

typedef struct address_t {
    char host[ADDRESS_MAX_HOST_LEN];
    int port;
} address_t;

int  address_init(address_t *addr, const char *host, int port);
int  address_cmp(const address_t *l, const address_t *r);
int  address_parse(address_t *addr, const char *address_spec, int default_port);
void address_free(address_t *address);

#endif /* ADDRESS_H_ */
