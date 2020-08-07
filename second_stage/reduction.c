#include "reduction.h"

#include <stdio.h>
#include <string.h>

#include "schema.h"
#include "config.h"
#include "helpers.h"
#include "libgpu/gpu_agg.h"
#include "generators.h"

static int free_id = 0;

reduction_p reduction(schema_p input_schema, int ref) {
    reduction_p p = (reduction_p) malloc(sizeof(reduction_t));
    p->operator = (operator_p) malloc(sizeof (operator_t));
    {
        p->operator->setup = (void *) reduction_setup;
        p->operator->reset = (void *) reduction_reset;
        p->operator->process = (void *) reduction_process;
        p->operator->print = (void *) reduction_print_output;

        p->operator->type = OPERATOR_REDUCE;

        strcpy(p->operator->code_name, REDUCTION_CODE_FILENAME);
    }

    p->id = free_id++;

    p->input_schema = input_schema;
    p->ref = ref;

    p->output_schema = schema();
    {
        schema_add_attr (p->output_schema, TYPE_LONG);
        schema_add_attr (p->output_schema, TYPE_FLOAT);
        schema_add_attr (p->output_schema, TYPE_INT);
    }
    printf("Created output schema of size %d\n", p->output_schema->size);

    return p;    
}

static char * generate_source(reduction_p reduce, window_p window) {
    char * source = (char *) malloc(MAX_SOURCE_LENGTH * sizeof(char)); *source = '\0';

    char * extensions = read_file("cl/templates/extensions.cl");

    char * headers = read_file("cl/templates/headers.cl");

    /* Input and output vector sizes */
    int const bytes_per_element = 16;
    char define_in_size[32], define_out_size[32];

    if (reduce->input_schema->size % bytes_per_element != 0 || reduce->output_schema->size % bytes_per_element != 0) {
        fprintf(stderr, "error: input/output sizz is not a multiple of the vector `uchar%d`\n", bytes_per_element);
        exit(1);
    }

    sprintf(define_in_size, "#define INPUT_VECTOR_SIZE %d\n", reduce->input_schema->size / bytes_per_element);
    sprintf(define_out_size, "#define OUTPUT_VECTOR_SIZE %d\n\n", reduce->output_schema->size / bytes_per_element);

    /* Input and output tuple struct */
    char * input_tuple = generate_input_tuple(reduce->input_schema, NULL, 16);

    char * output_tuple = generate_output_tuple(reduce->output_schema, NULL, 16);

    /* Window sizes */
    char * windows = generate_window_definition(window);

    /* TODO: generate according to reduce */
    char funcs[] =
"inline void initf (__local output_t *p) {\n\
    p->tuple.t = 0;\n\
    p->tuple._1 = 0;\n\
    p->tuple._2 = 0;\n\
}\n\
\n\
/* r */ inline void reducef (__local output_t *out, __global input_t *in) {\n\
\n\
    /* r */ int flag = 1;\n\
\n\
    /* Selection */\n\
    // int attribute_value = in->tuple._7; /* where category == 1*/\n\
    // flag = flag & (attribute_value == 9);\n\
\n\
    /* Reduce */\n\
    /* r */ out->tuple.t = (out->tuple.t == 0) ? : in->tuple.t;\n\
    out->tuple._1 += in->tuple._8 * flag;\n\
    /* r */ out->tuple._2 += 1;\n\
/* r */ }\n\
\n\
inline void cachef (__local output_t *p, __local output_t *q) {\n\
    q->vectors[0] = p->vectors[0];\n\
}\n\
\n\
inline void mergef (__local output_t *p, __local output_t *q) {\n\
    p->tuple._1 += q->tuple._1;\n\
    p->tuple._2 += q->tuple._2;\n\
}\n\
\n\
inline void copyf (__local output_t *p, __global output_t *q) {\n\
    q->vectors[0] = p->vectors[0];\n\
}\n\n";

    /* Template funcitons */
    char * template = read_file(REDUCTION_CODE_TEMPLATE);

    /* Assembling */
    strcat(source, extensions);

    strcat(source, headers);
    
    strcat(source, define_in_size);
    strcat(source, define_out_size);
    
    strcat(source, input_tuple);
    strcat(source, output_tuple);
    
    strcat(source, windows);
    
    strcat(source, funcs);
    
    strcat(source, template);

    /* Free inputed/generated source */
    free(extensions);
    free(headers);
    free(input_tuple);
    free(output_tuple);
    free(input_tuple);
    free(windows);
    free(template);

    return source;
}

void reduction_setup(void * reduce_ptr, int batch_size, window_p window) {
    reduction_p reduce = (reduction_p) reduce_ptr;

    /* Operator setup */
    for (int i=0; i<REDUCTION_KERNEL_NUM; i++) {
        reduce->threads[i] = batch_size;

        if (batch_size < MAX_THREADS_PER_GROUP) {
            reduce->threads_per_group[i] = batch_size;
        } else {
            reduce->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }

    /* Refer to selection.c */
    reduce->output_entries[0] = 0;
    reduce->output_entries[1] = 20;

    /* Code generation */
    char * source = generate_source(reduce, window);

    /* Output generated code */
    char * filename = generate_filename(reduce->id, REDUCTION_CODE_FILENAME);
    printf("[REDUCTION] Printing the generated source code to file: %s\n", filename);
    print_to_file(filename, source);
    free(filename);
    
    /* Build opencl program */
    int qid = gpu_get_query(source, 4, 1, 5);
    reduce->qid = qid;
    free(source);
    
    /* GPU inputs and outputs setup */
    int tuple_size = reduce->input_schema->size;
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
    args1[3] = 16 * MAX_THREADS_PER_GROUP; /* local cache size */

    fprintf(stderr, "args1 0: %d\n", args1[0]);
    fprintf(stderr, "args1 1: %d\n", args1[1]);
    fprintf(stderr, "args1 2: %d\n", args1[2]);
    fprintf(stderr, "args1 3: %d\n", args1[3]);

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_set_kernel_reduce(qid, args1, args2);
}

void reduction_process(void * reduce_ptr, batch_p batch, int pane_size, bool is_range, batch_p output) {
    reduction_p reduce = (reduction_p) reduce_ptr;
    
    long args2[2];
    {
        /* Previous pane id, based on stream start pointer, s */
        args2[0] = -1;
        long s = batch->start;
        int  t = reduce->input_schema->size;
        long p = pane_size;
        if (batch->start > 0) {
            if (is_range) {
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
        output->buffer + output->start + reduce->output_entries[0], 
        output->buffer + output->start + reduce->output_entries[1]};

    /* Execute */
    gpu_execute_reduce(
        reduce->qid,
        reduce->threads, reduce->threads_per_group,
        args2,
        (void **) (inputs), (void **) (outputs), sizeof(u_int8_t));
        // passing the batch without deserialisation
}

void reduction_print_output(batch_p outputs, int batch_size, int tuple_size) {
    typedef struct {
        long t; /* timestamp */
        float _1; /* sum */
        int _2; /* count */
    } output_tuple_t;

    /* Deserialise output buffer */
    int current_offset = 0;
    
    int window_counts_size = 20; /* 4 integers, +1 that is the mark */
    int * window_counts = (int *) (outputs->buffer + current_offset);
    current_offset += window_counts_size;

    int output_size = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    output_tuple_t * output = (output_tuple_t *) (outputs->buffer + current_offset);
    current_offset += output_size;

    /* print */
    printf("[Results] Required Output buffer size is %d\n", current_offset);
    printf("[Results] Closing Windows: %d    Pending Windows: %d    Complete Windows: %d    Opening Windows: %d    Mark: %d\n",
        window_counts[0], window_counts[1], window_counts[2], window_counts[3], window_counts[4]);
    printf("[Results] Tuple    Timestamp    Sum        Count\n");
    for (int i=0; i<10; i++) {
        printf("[Results] %-8d %-12ld %09.5f  %d\n", i, output->t, output->_1, output->_2);
        output += 1;
    }
}

void reduction_process_output(void * reduce_ptr, batch_p outputs) {
    /* Stud, reduction is a pipeline breaker so it does not need to pass any output to another operator in this attempt */
}

void reduction_reset(void * reduce_ptr, int new_batch_size) {
    reduction_p reduce = (reduction_p) reduce_ptr;

    int tuple_size = reduce->input_schema->size;

    /* Operator setup */
    for (int i=0; i<REDUCTION_KERNEL_NUM; i++) {
        reduce->threads[i] = new_batch_size;

        if (new_batch_size < MAX_THREADS_PER_GROUP) {
            reduce->threads_per_group[i] = new_batch_size;
        } else {
            reduce->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
    
    /* GPU kernels setup */
    int args1 [4];
    args1[0] = new_batch_size; /* tuples */
    args1[1] = new_batch_size * tuple_size; /* input size */
    args1[2] = PARTIAL_WINDOWS; 
    args1[3] = 16 * MAX_THREADS_PER_GROUP; /* local cache size */

    fprintf(stderr, "After reset\n");
    fprintf(stderr, "args1 0: %d\n", args1[0]);
    fprintf(stderr, "args1 1: %d\n", args1[1]);
    fprintf(stderr, "args1 2: %d\n", args1[2]);
    fprintf(stderr, "args1 3: %d\n", args1[3]);

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_reset_kernel_reduce(reduce->qid, args1, args2);
}