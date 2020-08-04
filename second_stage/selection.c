#include "selection.h"

#include <stdio.h>
#include <string.h>

#include "helpers.h"
#include "libgpu/gpu_agg.h"
#include "config.h"

ref_value_p ref_value() {

    ref_value_p value = (ref_value_p) malloc(sizeof(ref_value_t));
    value->i = NULL;
    value->l = NULL;
    value->f = NULL;
    value->c = NULL;

    return value;
}

void ref_value_free(ref_value_p value) {

    if (value) {
        if (value->i) {
            free(value->i);
        }
        if (value->l) {
            free(value->l);
        }
        if (value->f) {
            free(value->f);
        }
        if (value->c) {
            free(value->c);
        }
        free(value);
    }
} 

selection_p selection(

    schema_p input_schema, 
    int ref, ref_value_p value,
    enum comparor com) {

    selection_p p = (selection_p) malloc(sizeof(selection_t));
    p->operator = (operator_p) malloc(sizeof (operator_t));
    {
        p->operator->setup = selection_setup;
        p->operator->process = selection_process;
        p->operator->process_output = selection_process_output;
        p->operator->reset_threads = selection_reset_threads;
        p->operator->print = selection_print_output;

        p->operator->type = OPERATOR_SELECT;

        strcpy(p->operator->code_name, SELECTION_CODE_FILENAME);
    }
    p->input_schema = input_schema;
    p->ref = ref;
    p->value = value;

    return p;
}

void selection_setup(void * select_ptr, int batch_size) {
    selection_p select = (selection_p) select_ptr;

    int tuple_size = select->input_schema->size;

    /* Operator setup */
    for (int i=0; i<SELECTION_KERNEL_NUM; i++) {
        select->threads[i] = batch_size / SELECTION_TUPLES_PER_THREADS;

        if (select->threads[i] < MAX_THREADS_PER_GROUP) {
            select->threads_per_group[i] = select->threads[0];
        } else {
            select->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Since the outputs have been set at this stage, their size in libgpu cannot be alterred afterwards(during 
       processing, we might want to change to number of threads used to compute according to the input batch).
       Therefore, we need to calculate these entries here instead of when needed. Otherwise they might change
       according to the new settings and do not match up the libgpu settings */
    select->output_entries[0] = 0;
    select->output_entries[1] = 4 * batch_size;
    select->output_entries[2] = 4 * batch_size + 4 * work_group_num;

    /* Source generation */
    char * source = read_source("cl/select.cl");
    int qid = gpu_get_query(source, 2, 1, 4);
    
    /* GPU inputs and outputs setup */
    
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    gpu_set_output (qid, 0, 4 * batch_size,           0, 1, 1, 0, 1); /*      Flags */
    gpu_set_output (qid, 1, 4 * batch_size,           0, 1, 0, 0, 1); /*    Offsets */
    gpu_set_output (qid, 2, 4 * work_group_num,       0, 0, 0, 0, 1); /* Partitions */
    gpu_set_output (qid, 3, batch_size * tuple_size,  1, 0, 0, 1, 0); /*    Results */
    
    /* GPU kernels setup */
    int args[3];
    args[0] = batch_size * tuple_size;
    args[1] = batch_size;
    args[2] = 4 * select->threads_per_group[0] * SELECTION_TUPLES_PER_THREADS;

    gpu_set_kernel_select (qid, args);
}

void selection_reset_threads(void * select_ptr, int new_batch_size) {
    selection_p select = (selection_p) select_ptr;

    for (int i=0; i<SELECTION_KERNEL_NUM; i++) {
        select->threads[i] = new_batch_size / SELECTION_TUPLES_PER_THREADS;

        if (select->threads[i] < MAX_THREADS_PER_GROUP) {
            select->threads_per_group[i] = select->threads[0];
        } else {
            select->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
}

void selection_process(int qid, void * select_ptr, batch_p input, batch_p output) {
    selection_p select = (selection_p) select_ptr;

    int batch_size = input->size;
    int tuple_size = select->input_schema->size;
    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Set input buffer addresses and entries */
    u_int8_t * inputs [1] = {input->buffer + input->start};

    /* Set output buffer addresses and entries */
    u_int8_t * outputs [3] = {
        output->buffer + output->start + select->output_entries[0],  /* flags */
        output->buffer + output->start + select->output_entries[1],  /* partitions */
        output->buffer + output->start + select->output_entries[2]}; /* output */

    /* Validate whether the given output buffer is big enough */
    if ((output->end - output->start) < (long) (4 * batch_size + 4 * work_group_num + batch_size * tuple_size)) {
        fprintf(stderr, "error: Expected output size has exceeded the given output buffer size (%s)\n", __FUNCTION__);
        exit(1);
    }

    /* Execute */
    gpu_execute(qid, select->threads, select->threads_per_group,
        (void *) inputs, (void *) outputs, sizeof(u_int8_t));
}

void selection_print_output(selection_p select, batch_p outputs) {

    int work_group_num = select->threads[0] / select->threads_per_group[0];

    typedef struct {
        long t;
        long _1;
        long _2;
        long _3;
        int _4;
        int _5;
        int _6;
        int _7;
        float _8;
        float _9;
        float _10;
        int _11;
    } output_tuple_t __attribute__((aligned(1)));

    /* Deserialise output buffer */
    int * flags = (int *) (outputs->buffer + outputs->start + select->output_entries[0]);

    int * partitions = (int *) (outputs->buffer + outputs->start + select->output_entries[1]);

    output_tuple_t * output = (output_tuple_t *) (outputs->buffer + outputs->start + select->output_entries[2]);

    /* Calculate output tuples */
    int count = 0;
    for (int i=0; i<work_group_num; i++) {
        count += partitions[i];
    }

    /* print */
    printf("[Results] Output tuple numbers: %d\n", count);
    printf("[Results] Tuple    Timestamp    user-id     event-type    category    priority    cpu\n");
    for (int i=0; i<10; i++) {
        printf("[Results] %-8d %-12ld %-11d %-13d %-11d %-11d %-6.5f\n", 
            i, output->t, output->_4, output->_5, output->_6, output->_7, output->_8);
        output += 1;
    }
}

void selection_process_output (void * select_ptr, batch_p outputs) {
    selection_p select = (selection_p) select_ptr;

    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Calculate output tuples */
    int * partitions = (int *) (outputs->buffer + select->output_entries[1]);
    int count = 0;
    for (int i=0; i<work_group_num; i++) {
        count += partitions[i];
    }
    /* TODO: replace it with the least power of 2 number that bigger than count */
    // if ()

    /* TODO: initialise the extra tuples */

    /* Update pointers to use only the output array (tuples) and exclude flags and partitions */
    outputs->start += select->output_entries[1];
    outputs->size = 128; // count;
}
