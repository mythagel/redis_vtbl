/*
 * list.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "list.h"
#include <stdlib.h>
#include <math.h>

/*-----------------------------------------------------------------------------
 * Transparent list type
 *----------------------------------------------------------------------------*/

void list_init(list_t *list, void (*value_free)(void *value)) {
    list->capacity = 0;
    list->size = 0;
    list->data = 0;
    list->value_free = value_free;
}

void* list_get(list_t *list, size_t index) {
    if(index > list->size)
        return 0;
    return list->data[index];
}

int list_push(list_t *list, void* value) {
    if (list->size == list->capacity) {
        size_t capacity = list->capacity ? ceil(list->capacity*1.618) : 1;
        void* data = realloc(list->data, sizeof(void*)*capacity);
        if(!data) return LIST_ENOMEM;
        list->capacity = capacity;
        list->data = data;
    }
    list->data[list->size++] = value;
    return LIST_OK;
}

void list_free(list_t *list) {
    if(list->value_free) {
        size_t i;
        for(i = 0; i < list->size; ++i)
            list->value_free(list->data[i]);
    }
    free(list->data);
}
