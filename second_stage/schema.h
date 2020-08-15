#ifndef __SCHEMA_H_
#define __SCHEMA_H_

#include <stdlib.h>

/* TODO dynamically increase attr array length */
#define MAX_ATTR_NUM 16

enum attr_types {
    TYPE_INT,
    TYPE_FLOAT,
    TYPE_LONG
};

int attr_types_get_size(enum attr_types attr);

typedef struct schema * schema_p;
typedef struct schema {
    int attr_num;
    enum attr_types attr [MAX_ATTR_NUM];
    int size;
} schema_t;

schema_p schema();

void schema_add_attr(schema_p schema, enum attr_types attr);

int schema_get_pad(schema_p schema, int vector);


#endif