#include "address.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>

int main() {
    address_t addr;
    
    if(address_init(&addr, "192.168.1.1", 80) == ADDRESS_OK) {
        printf("host: %s:%d\n", addr.host, addr.port);
        address_free(&addr);
    } else {
        assert(false);
    }
    
    if(address_init(&addr, "dns.domain.name.com", 80) == ADDRESS_OK) {
        printf("host: %s:%d\n", addr.host, addr.port);
        address_free(&addr);
    } else {
        assert(false);
    }

    if(address_init(&addr, "toooooooooooooooooooooooooooooooooooooooooooooooooooolongdns.domain.name.comddfsdksjdfksjdfjkshdfjsdfjhsdjkfhsjkdfhsjklhdfkjshdfjkshdfjksdfjkhsdjkfhsjkdhfjksdhfjkshdfjkhsdkfjhsdjkfhksdhfjksdhsudgskdghsjkdghksdghksjdghjkasdhgkjshdgkjshadgjkhasdkgjhsjkadghasjkdghsjkadgjdjfhdjkfhsk", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_init(&addr, "verylongsillybutactuallyoklengthdnsdomainnamethaticantimagineanyonewouldevertypein.com", 80) == ADDRESS_OK) {
        printf("host: %s:%d\n", addr.host, addr.port);
        address_free(&addr);
    } else {
        assert(false);
    }

    if(address_parse(&addr, "192.168.1.1:80", 80) == ADDRESS_OK) {
        printf("host: %s:%d\n", addr.host, addr.port);
        address_free(&addr);
    } else {
        assert(false);
    }

    if(address_parse(&addr, "192.168.1.1", 80) == ADDRESS_OK) {
        printf("host: %s:%d\n", addr.host, addr.port);
        address_free(&addr);
    } else {
        assert(false);
    }

    if(address_parse(&addr, "toooooooooooooooooooooooooooooooooooooooooooooooooooolongdns.domain.name.comddfsdksjdfksjdfjkshdfjsdfjhsdjkfhsjkdfhsjklhdfkjshdfjkshdfjksdfjkhsdjkfhsjkdhfjksdhfjkshdfjkhsdkfjhsdjkfhksdhfjksdhsudgskdghsjkdghksdghksjdghjkasdhgkjshdgkjshadgjkhasdkgjhsjkadghasjkdghsjkadgjdjfhdjkfhsk", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_parse(&addr, "192.168.1.1:", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_parse(&addr, "192.168.1.1:hello", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_parse(&addr, "192.168.1.1:0", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_parse(&addr, "192.168.1.1:80hello", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_parse(&addr, "192.168.1.1:hello80", 80) == ADDRESS_OK) {
        assert(false);
    } else {
        printf("+expected fail\n");
    }

    if(address_parse(&addr, "192.168.1.1:080", 80) == ADDRESS_OK) {
        printf("host: %s:%d\n", addr.host, addr.port);      /* base10 80 _not_ octal 080 */
        address_free(&addr);
    } else {
        assert(false);
    }

    return 0;
}


