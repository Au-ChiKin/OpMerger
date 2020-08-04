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
        p->operator->setup = (void *) selection_setup;
        p->operator->process = (void *) selection_process;
        p->operator->process_output = (void *) selection_process_output;
        p->operator->print = (void *) selection_print_output;

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

    /* Operator setup */
    for (int i=0; i<SELECTION_KERNEL_NUM; i++) {
        select->threads[i] = batch_size / SELECTION_TUPLES_PER_THREADS;

        if (select->threads[i] < MAX_THREADS_PER_GROUP) {
            select->threads_per_group[i] = select->threads[0];
        } else {
            select->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }

    /* Source generation */
    char * source = read_source("cl/select.cl");
    int qid = gpu_get_query(source, 2, 1, 4);
    
    /* GPU inputs and outputs setup */
    int tuple_size = select->input_schema->size;
    int work_group_num = select->threads[0] / select->threads_per_group[0];
    
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

void selection_process(int qid, void * select_ptr, batch_p input, batch_p output) {
    selection_p select = (selection_p) select_ptr;

    int batch_size = input->size;
    int tuple_size = select->input_schema->size;
    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Set input buffer addresses and entries */
    u_int8_t * inputs [1] = {input->buffer + input->start};

    /* Set output buffer addresses and entries */
    u_int8_t * outputs [3] = {
        output->buffer + input->start,                                        /* flags */
        output->buffer + input->start + 4 * batch_size,                       /* partitions */
        output->buffer + input->start + 4 * batch_size + 4 * work_group_num}; /* output */

    /* Validate whether the given output buffer is big enough */
    if ((output->end - output->start) < (long) (4 * batch_size + 4 * work_group_num + batch_size * tuple_size)) {
        fprintf(stderr, "error: Expected output size has exceeded the given output buffer size (%s)\n", __FUNCTION__);
        exit(1);
    }

    /* Execute */
    gpu_execute(qid, select->threads, select->threads_per_group,
        (void *) inputs, (void *) outputs, sizeof(u_int8_t));
}

void selection_print_output(selection_p select, batch_p outputs, int batch_size) {

    int tuple_size = select->input_schema->size;
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
    int current_offset = 0;
    
    int flags_size = 4 * batch_size;
    int * flags = (int *) (outputs->buffer + current_offset);
    current_offset += flags_size;

    int partitions_size = 4 * work_group_num;
    int * partitions = (int *) (outputs->buffer + current_offset);
    current_offset += partitions_size;

    int output_size = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    output_tuple_t * output = (output_tuple_t *) (outputs->buffer + current_offset);
    current_offset += output_size;

    /* Calculate output tuples */
    int count = 0;
    for (int i=0; i<work_group_num; i++) {
        count += partitions[i];
    }

    /* print */
    printf("[Results] Required Output buffer size is %d\n", current_offset);
    printf("[Results] Output tuple numbers: %d\n", count);
    printf("[Results] Tuple    Timestamp    user-id     event-type    category    priority    cpu\n");
    for (int i=0; i<10; i++) {
        printf("[Results] %-8d %-12ld %-11d %-13d %-11d %-11d %-6.5f\n", 
            i, output->t, output->_4, output->_5, output->_6, output->_7, output->_8);
        output += 1;
    }
}

// void selection_configureOutput (int queryId) {
    
//     /* Create a buffer to hold the output */
//     IQueryBuffer outputBuffer = UnboundedQueryBufferFactory.newInstance();
//     TheGPU.getInstance().setOutputBuffer(queryId, /* output id */ 3, outputBuffer);
// }

// void selection_processOutput (int queryId, WindowBatch batch) {
    
//     /* Get the buffer which has been used to store the result from this oeprator */
//     IQueryBuffer buffer = TheGPU.getInstance().getOutputBuffer(queryId, 3);
    
//     // System.out.println(String.format("[DBG] task %10d (\"select\"): output buffer position is %10d", 
//     //		batch.getTaskId(), buffer.position()));
    
//     /* Make it into an input batch to another operator */
//     batch.setBuffer(buffer);
// }