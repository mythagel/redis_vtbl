#include "list.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main() {
    size_t i;
    list_t l;
    char* c;
    
    list_init(&l, free);
    
    for(i = 0; i < 100; ++i) {
        list_push(&l, strdup("hello"));
    }
    
    for(i = 0; i < l.size; ++i) {
        c = list_get(&l, i);
        printf("%s ", c);
    }
    printf("\n");
    list_free(&l);
    return 0;
}
