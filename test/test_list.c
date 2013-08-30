#include "list.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

int test_strtok() {
    size_t i;
    int err;
    list_t list;
    
    err = list_strtok(&list, "hello there are five words", " ");
    assert(!err);
    
    for(i = 0; i < list.size; ++i) {
        char *c;
        
        c = list_get(&list, i);
        printf("%s\n", c);
    }
    
    list_free(&list);
}

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
    
    test_strtok();
    return 0;
}
