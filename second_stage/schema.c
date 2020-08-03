#include "schema.h"

schema_p schema() {
    schema_p schema = (schema_p) malloc(sizeof(schema_t));
    schema->attr_num = 0;
    schema->size = 0;

    return schema;
}

void schema_add_attr(schema_p schema, enum attr_type attr) {
    schema->attr[schema->attr_num] = attr;
    schema->attr_num += 1;
    switch (attr)
    {
    case TYPE_INT:
        schema->size += 4;
        break;
    case TYPE_FLOAT:
        schema->size += 4;
        break;
    case TYPE_LONG:
        schema->size += 8;
        break;
    default:
        break;
    }
}