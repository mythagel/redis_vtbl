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
    void** data;
    void (*value_free)(void *value);
} list_t;

void list_init(list_t *list, void (*value_free)(void *value));
int  list_add(list_t *list, void* value);
void list_free(list_t *list);

#endif /* LIST_H_ */
