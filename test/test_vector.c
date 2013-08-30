#include "vector.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void int_test()
{
    int n;
    size_t i;
    vector_t v;
    vector_init(&v, sizeof(int), 0);
    
    for(n = 0; n < 100; ++n) {
        printf("push: %d\n", n);
        vector_push(&v, &n);
    }
    
    for(i = 0; i < v.size; ++i) {
        int n = *((int*)vector_get(&v, i));
        printf("%d ", n);
    }
    printf("\n");
    vector_free(&v);
}

typedef struct test
{
    int a;
    double b;
    char *c;
} test;

void test_init(test *t) {
    t->a = 1;
    t->b = 2.5;
    t->c = strdup("hello");
}

void test_free(test *t) {
    free(t->c);
}

void struct_test()
{
    int n;
    size_t i;
    vector_t v;
    vector_init(&v, sizeof(test), (void (*)(void *))test_free);
    
    for(n = 0; n < 100; ++n) {
        test t;
        test_init(&t);
        printf("push: %d\n", n);
        vector_push(&v, &t);
    }
    
    for(i = 0; i < v.size; ++i) {
        test* t = vector_get(&v, i);
        printf("a: %d b: %f c: %s\n", t->a, t->b, t->c);
    }
    vector_free(&v);
}

int main() {
    int_test();
    struct_test();
    return 0;
}
