/*
 * vector.h
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#ifndef VECTOR_H_
#define VECTOR_H_
#include <stddef.h>

/*-----------------------------------------------------------------------------
 * Transparent vector type
 *----------------------------------------------------------------------------*/

enum {
    VECTOR_OK = 0,
    VECTOR_ENOMEM
};

typedef struct vector_t {
    size_t elem_size;
    size_t capacity;
    size_t size;
    void *data;
    void (*value_free)(void *value);
} vector_t;

void  vector_init(vector_t *vector, size_t elem_size, void (*value_free)(void *value));
int   vector_reserve(vector_t *vector, size_t count);
void* vector_get(vector_t *vector, size_t index);
void* vector_begin(vector_t *vector);
void* vector_end(vector_t *vector);
void  vector_sort(vector_t *vector, int (*cmp)(const void *l, const void *r));
int   vector_push(vector_t *vector, void *value);
void* vector_find(vector_t *vector, void *value, int (*cmp)(const void *l, const void *r));
/* bsearch precondition: vector is sorted by cmp */
void* vector_bsearch(vector_t *vector, const void *value, int (*cmp)(const void *l, const void *r));
void  vector_free(vector_t *vector);

#endif /* VECTOR_H_ */
