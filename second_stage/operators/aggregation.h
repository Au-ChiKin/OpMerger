#ifndef AGGREGATION_H
#define AGGREGATION_H

#include "batch.h"
#include "operator.h"
#include "schema.h"

#define AGGREGATION_KERNEL_NUM 4
#define AGGREGATION_CODE_FILENAME "cl/reduce"
#define AGGREGATION_CODE_TEMPLATE "cl/templates/reduce_template.cl"
#define AGGREGATION_MAX_REFERENCE 2

enum aggregation_types {
    CNT,
    SUM,
    AVG,
    MIN,
    MAX
};

typedef struct aggregation * aggregation_p;
typedef struct aggregation {
    operator_p operator; /* As a parent class */

    int id; /* Refer to selection.h */
    int qid;

    int ref_num;
    int refs[AGGREGATION_MAX_REFERENCE];
    enum aggregation_types expressions [AGGREGATION_MAX_REFERENCE];

    schema_p input_schema;
    size_t threads[AGGREGATION_KERNEL_NUM];
    size_t threads_per_group [AGGREGATION_KERNEL_NUM];

    schema_p output_schema;
    int output_entries[2];

} aggregation_t;

/* Refer to selection.h for explainations of following member methods */

aggregation_p aggregation(
    schema_p input_schema, 
    int ref_num, int const columns[], enum aggregation_types const expressions[]);

void aggregation_setup(void * reduce_ptr, int batch_size, window_p window, char const * patch);

void aggregation_reset(void * reduce_ptr, int new_batch_size);

void aggregation_process(void * reduce_ptr, batch_p batch, window_p window, batch_p output);

void aggregation_print_output(batch_p outputs, int batch_size, int tuple_size);

#endif