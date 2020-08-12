#ifndef REDUCTION_H
#define REDUCTION_H

#include "batch.h"
#include "schema.h"
#include "operator.h"

#define REDUCTION_KERNEL_NUM 4
#define REDUCTION_CODE_FILENAME "cl/reduce"
#define REDUCTION_CODE_TEMPLATE "cl/templates/reduce_template.cl"
#define REDUCITON_MAX_REFERENCE 2

enum reduction_types {
    SUM
};

typedef struct reduction * reduction_p;
typedef struct reduction {
    operator_p operator; /* As a parent class */

    int id; /* Refer to selection.h */
    int qid;

    int ref_num;
    int refs[REDUCITON_MAX_REFERENCE];
    enum reduction_types expressions [REDUCITON_MAX_REFERENCE];

    schema_p input_schema;
    size_t threads[REDUCTION_KERNEL_NUM];
    size_t threads_per_group [REDUCTION_KERNEL_NUM];

    schema_p output_schema;
    int output_entries[2];

} reduction_t;

/* Refer to selection.h for explainations of following member methods */

reduction_p reduction(
    schema_p input_schema, 
    int ref_num, int const columns[], enum reduction_types const expressions[]);

void reduction_setup(void * reduce_ptr, int batch_size, window_p window, char const * patch);

void reduction_reset(void * reduce_ptr, int new_batch_size);

void reduction_process(void * reduce_ptr, batch_p batch, window_p window, batch_p output);

void reduction_print_output(batch_p outputs, int batch_size, int tuple_size);

#endif