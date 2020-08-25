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
    int ref_num, int const columns[], enum aggregation_types const expressions[],
    int group_num, int const groups[]) {

    aggregation_p p = (aggregation_p) malloc(sizeof(aggregation_t));
    p->operator = (operator_p) malloc(sizeof (operator_t));
    {
        p->operator->setup = (void *) aggregation_setup;
        // p->operator->reset = (void *) aggregation_reset;
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

    /* Generate output schema */
    int key_length = 0;
    p->output_schema = schema();
    {
        /* timestamp */
        schema_add_attr (p->output_schema, TYPE_LONG);

        /* aggregations */
        for (int i=0; i<ref_num; i++) {
            schema_add_attr (p->output_schema, TYPE_FLOAT);
        }

        /* group by attributes */
        for (int i=0; i<group_num; i++) {
            enum attr_types attr = input_schema->attr[groups[i]];

            key_length += attr_types_get_size(attr);
            schema_add_attr (p->output_schema, attr);
        }
    }
    p->key_length = key_length;

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

    /* Code generation */
    // char * source = generate_source(aggregate, window, patch);

#ifdef OPMERGER_DEBUG
    /* Output generated code */
    char * filename = generate_filename(aggregate->id, AGGREGATION_CODE_FILENAME);
    printf("[AGGREGATION] Printing the generated source code to file: %s\n", filename);
    print_to_file(filename, source);
    free(filename);
#endif
    
    /* Build opencl program */
    char * source = read_source("cl/aggregation_gen.cl");
    int qid = gpu_get_query(source, 9, 1, 9);
    aggregate->qid = qid;
    free(source);
    
    /* GPU inputs and outputs setup */
    int tuple_size = aggregate->input_schema->size;
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    int window_pointers_size = 4 * PARTIAL_WINDOWS;
    gpu_set_output(qid, 0, window_pointers_size, 0, 1, 0, 0, 1);
    gpu_set_output(qid, 1, window_pointers_size, 0, 1, 0, 0, 1);

    int failed_flags_size = 4 * batch_size; /* One int per tuple */
    gpu_set_output(qid, 2, failed_flags_size, 1, 1, 0, 0, 1);
    
    int offset_size = 16; /* The size of two longs */
    gpu_set_output(qid, 3, offset_size, 0, 1, 0, 0, 1);
    
    int window_counts_size = 20; /* 4 integers, +1 that is the mark */
    gpu_set_output(qid, 4, window_counts_size, 0, 0, 1, 0, 1);
    
    /* Set partial window results */
    int output_size = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    gpu_set_output(qid, 5, output_size, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 6, output_size, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 7, output_size, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 8, output_size, 1, 0, 0, 1, 1);

    /* Refer to selection.c */
    aggregate->output_entries[0] = 0; /* window_count */
    aggregate->output_entries[1] = 20; /* closing window */
    aggregate->output_entries[2] = 20 + output_size; /* pending window */
    aggregate->output_entries[3] = 20 + output_size * 2; /* complete window */
    aggregate->output_entries[4] = 20 + output_size * 3; /* opening window */
    
    /* GPU kernels setup */
    int args1 [6];
    args1[0] = batch_size; /* tuples */
    args1[1] = batch_size * tuple_size; /* input size */
    args1[2] = output_size;
    args1[3] = HASH_TABLE_SIZE;
    args1[4] = PARTIAL_WINDOWS;
    args1[5] = aggregate->key_length * MAX_THREADS_PER_GROUP; /* local cache size */

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_set_kernel_aggregate(qid, args1, args2);
}

void aggregation_process(void * aggregate_ptr, batch_p batch, window_p window, batch_p output, query_event_p event) {
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
                args2[0] = batch_get_first_tuple_timestamp64(batch, offset) / p;
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

    u_int8_t * outputs [5];
    for (int i=0; i<5; i++) {
        outputs[i] = output->buffer + output->start + aggregate->output_entries[i];
    }

    /* Execute */
    gpu_execute_aggregate(
        aggregate->qid,
        aggregate->threads, aggregate->threads_per_group,
        args2,
        (void **) (inputs), (void **) (outputs), sizeof(u_int8_t),
        event);
        // passing the batch without deserialisation
}

void aggregation_print_output(batch_p outputs, int batch_size, int tuple_size) {
    typedef struct {
        long t; /* timestamp */
        int _1; /* category */
        float _2; /* sum */
    } output_tuple_t;

    /* Deserialise output buffer */
    int current_offset = 0;
    
    int window_counts_size = 20; /* 4 integers, +1 that is the complete windows mark, +1 that is the mark */
    int * window_counts = (int *) (outputs->buffer + current_offset);
    current_offset += window_counts_size;

    int output_size = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    output_tuple_t * output[4];
    for (int i=0; i<4; i++) {
        output[i] = (output_tuple_t *) (outputs->buffer + current_offset);
        current_offset += output_size;
    }

    /* print */
    printf("[Results] Required Output buffer size is %d\n", current_offset);
    printf("[Results] Closing Windows: %d    Pending Windows: %d    Complete Windows: %d    \
Opening Windows: %d    Mark:%d\n",
        window_counts[0], window_counts[1], window_counts[2], 
        window_counts[3], window_counts[4]);

    for (int t=0; t<4; t++) {

        int out_num = 100;
        if (out_num > window_counts[t]) {
            out_num = window_counts[t];
        }

        if (out_num > 0) {
            printf("Window type: %d\n", t);
            printf("[Results] Tuple    Timestamp    Sum        Count\n");
        }

        for (int i=0; i<out_num; i++) {
            printf("[Results] %-8d %-12ld %-8d %09.5f\n", i, output[t]->t, output[t]->_1, output[t]->_2);
            output[t] += 1;
        }
    }
}

void aggregation_process_output(void * aggregate_ptr, batch_p outputs) {
    /* Stud, reduction is a pipeline breaker so it does not need to pass any output to another operator in this attempt */
}

void aggregation_reset(void * aggregate_ptr, int new_batch_size) {
    aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    int tuple_size = aggregate->input_schema->size;

    /* Operator setup */
    for (int i=0; i<AGGREGATION_KERNEL_NUM; i++) {
        aggregate->threads[i] = new_batch_size;

        if (new_batch_size < MAX_THREADS_PER_GROUP) {
            aggregate->threads_per_group[i] = new_batch_size;
        } else {
            aggregate->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
    
    /* TODO */
    int output_size = new_batch_size * tuple_size;

    /* GPU kernels setup */
    int args1 [6];
    args1[0] = new_batch_size; /* tuples */
    args1[1] = new_batch_size * tuple_size; /* input size */
    args1[2] = output_size;
    args1[3] = HASH_TABLE_SIZE;
    args1[4] = PARTIAL_WINDOWS;
    args1[5] = aggregate->key_length * MAX_THREADS_PER_GROUP; /* local cache size */

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_reset_kernel_aggregate(aggregate->qid, args1, args2);
}