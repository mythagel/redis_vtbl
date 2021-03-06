/*
 * vector.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "vector.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

/*-----------------------------------------------------------------------------
 * Transparent vector type
 *----------------------------------------------------------------------------*/

void vector_init(vector_t *vector, size_t elem_size, void (*value_free)(void *value)) {
    vector->elem_size = elem_size;
    vector->capacity = 0;
    vector->size = 0;
    vector->data = 0;
    vector->value_free = value_free;
}

int vector_reserve(vector_t *vector, size_t count) {
    if(vector->capacity < count) {
        size_t capacity = count;
        void* data = realloc(vector->data, vector->elem_size*capacity);
        if(!data) return VECTOR_ENOMEM;
        vector->capacity = capacity;
        vector->data = data;
    }
    return VECTOR_OK;
}

void* vector_get(vector_t *vector, size_t index) {
    if(index >= vector->size)
        return 0;
    return vector->data + (vector->elem_size * index);
}

void* vector_begin(vector_t *vector) {
    return vector->data;
}
void* vector_end(vector_t *vector) {
    return vector->data + (vector->elem_size * vector->size);
}

void vector_sort(vector_t *vector, int (*cmp)(const void *l, const void *r)) {
    qsort(vector->data, vector->size, vector->elem_size, cmp);
}

int vector_push(vector_t *vector, void *value) {
    void *element;
    
    if (vector->size == vector->capacity) {
        size_t capacity = vector->capacity ? ceil(vector->capacity*1.618) : 1;
        void* data = realloc(vector->data, vector->elem_size*capacity);
        if(!data) return VECTOR_ENOMEM;
        vector->capacity = capacity;
        vector->data = data;
    }
    
    element = vector->data + (vector->elem_size * vector->size);
    memcpy(element, value, vector->elem_size);
    ++vector->size;
    return VECTOR_OK;
}

void* vector_find(vector_t *vector, const void *value, int (*cmp)(const void *l, const void *r)) {
    size_t index;
    for(index = 0; index < vector->size; ++index) {
        void *rvalue = vector_get(vector, index);
        if(cmp(value, rvalue) == 0)
            return rvalue;
    }
    return 0;
}

void* vector_bsearch(vector_t *vector, const void *value, int (*cmp)(const void *l, const void *r)) {
    return bsearch(value, vector->data, vector->size, vector->elem_size, cmp);
}

void vector_clear(vector_t *vector) {
    if(vector->value_free) {
        size_t i;
        for(i = 0; i < vector->size; ++i) {
            void* value = vector_get(vector, i);
            vector->value_free(value);
        }
    }
    vector->size = 0;
}

void vector_free(vector_t *vector) {
    if(vector->value_free) {
        size_t i;
        for(i = 0; i < vector->size; ++i) {
            void* value = vector_get(vector, i);
            vector->value_free(value);
        }
    }
    free(vector->data);
}

