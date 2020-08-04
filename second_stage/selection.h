#ifndef SELECTION_H
#define SELECTION_H

#include "batch.h"
#include "schema.h"
#include "operator.h"

#define SELECTION_KERNEL_NUM 4
#define SELECTION_TUPLES_PER_THREADS 2
#define SELECTION_CODE_FILENAME "cl/select.cl"
#define SELECTION_CODE_TEMPLATE "cl/template/select_template.cl"

enum comparor {
    GREATER,
    EQUAL,
    LESS,
    GREATER_EQUAL,
    LESS_EQUAL
};

typedef struct ref_value * ref_value_p;
typedef struct ref_value {
    int * i;
    long * l;
    float * f;
    char * c;
} ref_value_t;

typedef struct selection * selection_p;
typedef struct selection {
    operator_p operator; /* As a parent class */

    schema_p input_schema;
    int ref;
    ref_value_p value;
    size_t threads[SELECTION_KERNEL_NUM];
    size_t threads_per_group [SELECTION_KERNEL_NUM];

} selection_t;

ref_value_p ref_value();
void ref_value_free(ref_value_p);

selection_p selection(schema_p input_schema, int ref, ref_value_p value, enum comparor com);

void selection_setup(void * select_ptr, int batch_size);

void selection_process(int qid, void * select_ptr, batch_p batch, batch_p output);

/* Only for debugging. No longer consistent with the current design */
void selection_print_output(selection_p select, batch_p outputs, int batch_size);

int selection_merger_and(selection_p, selection_p, char * source);
int selection_merger_or(selection_p, selection_p, char * source);

#endif