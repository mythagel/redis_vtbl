/*
 * list.c
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#include "list.h"
#include <stdlib.h>
#include <math.h>
#include <string.h>

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
    if(index >= list->size)
        return 0;
    return list->data[index];
}

void* list_set(list_t *list, size_t index, void *value) {
    void *old;
    if(index >= list->size)
        return 0;
    
    old = list->data[index];
    list->data[index] = value;
    return old;
}

int list_push(list_t *list, void *value) {
    if (list->size == list->capacity) {
        size_t capacity = list->capacity ? ceil(list->capacity*1.618) : 1;
        void **data = realloc(list->data, sizeof(void*)*capacity);
        if(!data) return LIST_ENOMEM;
        list->capacity = capacity;
        list->data = data;
    }
    list->data[list->size++] = value;
    return LIST_OK;
}

void* list_find(list_t *list, void *value, int (*cmp)(const void *l, const void *r)) {
    size_t index;
    for(index = 0; index < list->size; ++index) {
        void *lvalue = list_get(list, index);
        if(cmp(lvalue, value) == 0)
            return lvalue;
    }
    return 0;
}

void list_clear(list_t *list) {
    if(list->value_free) {
        size_t i;
        for(i = 0; i < list->size; ++i)
            list->value_free(list->data[i]);
    }
    list->size = 0;
}

void list_free(list_t *list) {
    if(list->value_free) {
        size_t i;
        for(i = 0; i < list->size; ++i)
            list->value_free(list->data[i]);
    }
    free(list->data);
}

int list_strtok(list_t *list, const char *text, const char *delim) {
    char *token;
    char *buffer;
    char *state;
    
    list_init(list, free);
    buffer = strdup(text);
    if(!buffer) {
        list_free(list);
        return LIST_ENOMEM;
    }
    
    token = strtok_r(buffer, delim, &state);
    while(token) {
        char *str = strdup(token);
        if(!str) {
            free(buffer);
            list_free(list);
            return LIST_ENOMEM;
        }
        
        list_push(list, str);
        token = strtok_r(0, delim, &state);
    }
    
    free(buffer);
    return LIST_OK;
}

