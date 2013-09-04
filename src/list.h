/*
 * list.h
 *
 *  Created on: 2013-08-30
 *      Author: nicholas
 */

#ifndef LIST_H_
#define LIST_H_
#include <stddef.h>

/*-----------------------------------------------------------------------------
 * Transparent list type
 *----------------------------------------------------------------------------*/

enum {
    LIST_OK = 0,
    LIST_ENOMEM
};

typedef struct list_t {
    size_t capacity;
    size_t size;
    void **data;
    void (*value_free)(void *value);
} list_t;

void  list_init(list_t *list, void (*value_free)(void *value));
void* list_get(list_t *list, size_t index);
void* list_set(list_t *list, size_t index, void *value);
int   list_push(list_t *list, void *value);
void* list_find(list_t *list, void *value, int (*cmp)(void *l, void *r));
void  list_clear(list_t *list);
void  list_free(list_t *list);

/* list is allocated within this function */
int   list_strtok(list_t *list, const char *text, const char *delim);

#endif /* LIST_H_ */
