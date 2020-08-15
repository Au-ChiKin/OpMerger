#include "schema.h"

#include "math.h"

schema_p schema() {
    schema_p schema = (schema_p) malloc(sizeof(schema_t));
    schema->attr_num = 0;
    schema->size = 0;

    return schema;
}

int attr_types_get_size(enum attr_types attr) {
    int ret = 0;

    switch (attr)
    {
    case TYPE_INT:
        ret = 4;
        break;
    case TYPE_FLOAT:
        ret = 4;
        break;
    case TYPE_LONG:
        ret = 8;
        break;
    default:
        break;
    }

    return ret;
}

void schema_add_attr(schema_p schema, enum attr_types attr) {
    schema->attr[schema->attr_num] = attr;
    schema->attr_num += 1;
    schema->size += attr_types_get_size(attr);
}

int schema_get_pad(schema_p schema, int vector) {
    int size_after_padding = ceil(schema->size / (double) vector) * vector;

    return size_after_padding - schema->size;
}

