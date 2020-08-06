#ifndef REDUCTION_H
#define REDUCTION_H

#include "batch.h"
#include "schema.h"
#include "operator.h"

#define REDUCTION_KERNEL_NUM 4
#define REDUCTION_CODE_FILENAME "cl/reduce.cl"
#define REDUCCTION_CODE_TEMPLATE "cl/template/reduce_template.cl"

typedef struct reduction * reduction_p;
typedef struct reduction {
    operator_p operator; /* As a parent class */

    int id; /* Refer to selection.h */
    int qid;

    schema_p input_schema;
    int ref;
    size_t threads[REDUCTION_KERNEL_NUM];
    size_t threads_per_group [REDUCTION_KERNEL_NUM];

    int output_entries[2];

} reduction_t;

/* Refer to selection.h for explainations of following member methods */

reduction_p reduction(schema_p input_schema, int ref);

void reduction_setup(void * reduce_ptr, int batch_size);

void reduction_rest_threads(void * reduce_ptr, int new_batch_size);

void reduction_process(void * reduce_ptr, batch_p batch, int pane_size, bool is_range, batch_p output);

void reduction_print_output(batch_p outputs, int batch_size, int tuple_size);

#endif