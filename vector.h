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
    void** data;
    void (*value_free)(void *value);
} vector_t;

void  vector_init(vector_t *vector, size_t elem_size, void (*value_free)(void *value));
void* vector_get(vector_t *vector, size_t index);
int   vector_push(vector_t *vector, void* value);
void  vector_free(vector_t *vector);

#endif /* VECTOR_H_ */
