#include "aggregation.h"

#include <stdio.h>
#include <string.h>

#include "config.h"
#include "generators.h"
#include "helpers.h"
#include "libgpu/gpu_agg.h"
#include "schema.h"

#define MAX_LINE_LENGTH 256
#define _sprintf(format, __VA_ARGS__...) \
{\
    sprintf(s, format, __VA_ARGS__);\
    strcat(ret, s);\
}

static int free_id = 0;

aggregation_p aggregation(schema_p input_schema, 
    int ref_num, int const columns[], enum aggregation_types const expressions[]) {

    aggregation_p p = (aggregation_p) malloc(sizeof(aggregation_t));
    p->operator = (operator_p) malloc(sizeof (operator_t));
    {
        p->operator->setup = (void *) aggregation_setup;
        p->operator->reset = (void *) aggregation_reset;
        p->operator->process = (void *) aggregation_process;
        p->operator->print = (void *) aggregation_print_output;

        p->operator->type = OPERATOR_AGGREGATE;

        strcpy(p->operator->code_name, AGGREGATION_CODE_FILENAME);
    }

    p->id = free_id++;

    p->ref_num = ref_num;
    if (ref_num > AGGREGATION_MAX_REFERENCE) {
        fprintf(stderr, "error: the number of reference has exceeded the limit (%d)\n", 
            AGGREGATION_MAX_REFERENCE);
        exit(1);
    }
    for (int i=0; i<ref_num; i++) {
        p->refs[i] = columns[i];
        p->expressions[i] = expressions[i];
    }

    p->input_schema = input_schema;

    p->output_schema = schema();
    {
        schema_add_attr (p->output_schema, TYPE_LONG);
        for (int i=0; i<ref_num; i++) {
            schema_add_attr (p->output_schema, TYPE_FLOAT);
        }
        schema_add_attr (p->output_schema, TYPE_INT);
    }

    return p;    
}

void aggregation_setup(void * aggregate_ptr, int batch_size, window_p window, char const * patch) {
    aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    /* Operator setup */
    for (int i=0; i<AGGREGATION_KERNEL_NUM; i++) {
        aggregate->threads[i] = batch_size;

        if (batch_size < MAX_THREADS_PER_GROUP) {
            aggregate->threads_per_group[i] = batch_size;
        } else {
            aggregate->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }

    /* Refer to selection.c */
    aggregate->output_entries[0] = 0;
    aggregate->output_entries[1] = 20;

    /* Code generation */
    char * source = generate_source(aggregate, window, patch);

#ifdef OPMERGER_DEBUG
    /* Output generated code */
    char * filename = generate_filename(aggregate->id, AGGREGATION_CODE_FILENAME);
    printf("[AGGREGATION] Printing the generated source code to file: %s\n", filename);
    print_to_file(filename, source);
    free(filename);
#endif
    
    /* Build opencl program */
    int qid = gpu_get_query(source, 4, 1, 5);
    aggregate->qid = qid;
    free(source);
    
    /* GPU inputs and outputs setup */
    int tuple_size = aggregate->input_schema->size;
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    int window_pointers_size = 4 * PARTIAL_WINDOWS;
    gpu_set_output(qid, 0, window_pointers_size, 0, 1, 0, 0, 1);
    gpu_set_output(qid, 1, window_pointers_size, 0, 1, 0, 0, 1);
    
    int offset_size = 16; /* The size of two longs */
    gpu_set_output(qid, 2, offset_size, 0, 1, 0, 0, 1);
    
    int window_counts_size = 20; /* 4 integers, +1 that is the mark */
    gpu_set_output(qid, 3, window_counts_size, 0, 0, 1, 0, 1);
    
    int outputSize = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    gpu_set_output(qid, 4, outputSize, 1, 0, 0, 1, 0);
    
    /* GPU kernels setup */
    int args1 [4];
    args1[0] = batch_size; /* tuples */
    args1[1] = batch_size * tuple_size; /* input size */
    args1[2] = PARTIAL_WINDOWS; 
    args1[3] = 32 * MAX_THREADS_PER_GROUP; /* local cache size */

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_set_kernel_aggregate(qid, args1, args2);
}

void aggregation_process(void * aggregate_ptr, batch_p batch, window_p window, batch_p output) {
    aggregation_p aggregate = (aggregation_p) aggregate_ptr;
    
    long args2[2];
    {
        /* Previous pane id, based on stream start pointer, s */
        args2[0] = -1;
        long s = batch->start;
        int  t = aggregate->input_schema->size;
        long p = window->pane_size;
        if (batch->start > 0) {
            if (window->type == RANGE_BASE) {
                int offset = s - t;
                args2[0] = batch_get_timestamp(batch, offset) / p;
            } else {
                args2[0] = ((s / (long) t) / p) - 1;
            }
        }
        /* Force every batch to be a new window because the batch->start here is not the same with Saber's */
        args2[0] = -1;
    
        args2[1] = s;
        
        /* Also the same to curent pane id */
        args2[1] = 0;
    }
    
    u_int8_t * inputs [1] = {
        batch->buffer + batch->start};

    u_int8_t * outputs [2] = {
        output->buffer + output->start + aggregate->output_entries[0], 
        output->buffer + output->start + aggregate->output_entries[1]};

    /* Execute */
    gpu_execute_aggregate(
        aggregate->qid,
        aggregate->threads, aggregate->threads_per_group,
        args2,
        (void **) (inputs), (void **) (outputs), sizeof(u_int8_t));
        // passing the batch without deserialisation
}

void aggregation(int batch_size, int tuple_size) {
    char * source = read_source("cl/aggregation_gen.cl");
    int qid = gpu_get_query(source, 9, 1, 9);

    gpu_set_input(qid, 0, batch_size * tuple_size);

    int window_pointers_size = 4 * 1024 /* SystemConf.PARTIAL_WINDOWS */;
    gpu_set_output(qid, 0, window_pointers_size, 0, 1, 0, 0, 1);
    gpu_set_output(qid, 1, window_pointers_size, 0, 1, 0, 0, 1);

    int failed_flags_size = 4 * batch_size; /* One int per tuple */
    gpu_set_output(qid, 2, failed_flags_size, 1, 1, 0, 0, 1);

    int offset_size = 16; /* The size of two longs */
    gpu_set_output(qid, 3, offset_size, 0, 1, 0, 0, 1);

    int window_counts_size = 24; /* 4 integers, +1 that is the complete windows mark, +1 that is the mark */
    // windowCounts = new UnboundedQueryBuffer (-1, windowCountsSize, false);
    gpu_set_output(qid, 4, window_counts_size, 0, 0, 1, 0, 1);

    /* Set partial window results */
    int outputSize = 1024 * 1024; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    gpu_set_output(qid, 5, outputSize, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 6, outputSize, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 7, outputSize, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 8, outputSize, 1, 0, 0, 1, 1);

    int args1[6];
    args1[0] = batch_size; /* batch size in tuples*/
    args1[1] = batch_size * tuple_size; /* batch size in bytes*/
    args1[2] = outputSize;
    args1[3] = 1024 * 1024; /* SystemConf.HASH_TABLE_SIZE */
    args1[4] = 1024; /* SystemConf.PARTIAL_WINDOWS */
    args1[5] = 8; /* keyLength * numberOfThreadsPerGroup; cache size; for group by */

    long args2[2];
    args2[0] = 0; /* Previous pane id */
    args2[0] = 0; /* Start offset */
    
    gpu_set_kernel_aggregate(0, args1, args2); // TODO: remove comments after create buffers
}