#include "reduction.h"

#include <stdio.h>
#include <string.h>

#include "schema.h"
#include "config.h"
#include "helpers.h"
#include "libgpu/gpu_agg.h"
#include "generators.h"

#define MAX_LINE_LENGTH 256
#define _sprintf(format, __VA_ARGS__...) \
{\
    sprintf(s, format, __VA_ARGS__);\
    strcat(ret, s);\
}

static int free_id = 0;

reduction_p reduction(schema_p input_schema, 
    int ref_num, int const columns[], enum aggregation_types const expressions[]) {

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

    p->ref_num = ref_num;
    if (ref_num > REDUCTION_MAX_REFERENCE) {
        fprintf(stderr, "error: the number of reference has exceeded the limit (%d)\n", 
            REDUCTION_MAX_REFERENCE);
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

static char * generate_initf(reduction_p reduce) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO support aggregation types and multiple of them */
    int aggregation_num = reduce->ref_num;

    /* initf */
    _sprintf("inline void initf (output_t *p) {\n", NULL);
    
    /* Set timestamp */
    _sprintf("    p->tuple.t = 0;\n", NULL);

    int i;
    for (i = 0; i < aggregation_num; ++i) {
        
        switch (reduce->expressions[i]) {
        case CNT:
        case SUM:
        case AVG: _sprintf("    p->tuple._%d = %s;\n", (i + 1), "0"); break;
        case MIN: _sprintf("    p->tuple._%d = %s;\n", (i + 1), "FLT_MAX"); break;
        case MAX: _sprintf("    p->tuple._%d = %s;\n", (i + 1), "FLT_MIN"); break;
        default:
            // throw new IllegalArgumentException("error: invalid aggregation type");
            break;
        }
    }
    /* Set count to zero */
    _sprintf("    p->tuple._%d = 0;\n", (i + 1));
    _sprintf("}\n", NULL);
    
    _sprintf("\n", NULL);

    return ret;
}

static char * generate_reducef (reduction_p reduce, char const * patch) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";
    int i;

    /* TODO support aggregation types and multiple of them */
    int aggregation_num = reduce->ref_num;

    /* reducef */
    _sprintf("inline void reducef (output_t *out, __global input_t *in) {\n", NULL);
    
    /* Reserve for select */
    _sprintf("    int flag = 1;\n\n", NULL);

    /* Insert patch */
    if (patch) {
        _sprintf(patch, NULL);
    }

    /* Set timestamp */
    _sprintf("    out->tuple.t = (!flag || (flag && out->tuple.t > in->tuple.t)) ? out->tuple.t : in->tuple.t;\n", NULL);

    /* Aggregation */
    for (i = 0; i < aggregation_num; ++i) {
        
        int column = reduce->refs[i];
        
        switch (reduce->expressions[i]) {
        case CNT: 
            _sprintf("    out->tuple._%d += 1 * flag;\n", (i + 1)); 
            break;
        case SUM:
        case AVG: 
            _sprintf("    out->tuple._%d += in->tuple._%d * flag;\n", (i + 1), column); 
            break;
        case MIN:
            _sprintf("    out->tuple._%d = (flag && out->tuple._%d > in->tuple._%d) ? in->tuple._%d : out->tuple._%d;\n",
                    (i + 1), (i + 1), column, column, (i + 1));
            break;
        case MAX:
            _sprintf("    out->tuple._%d = (flag && out->tuple._%d < in->tuple._%d) ? in->tuple._%d : out->tuple._%d;\n", 
                    (i + 1), (i + 1), column, column, (i + 1));
            break;
        default:
        //     throw new IllegalArgumentException("error: invalid aggregation type");
            break;
        }
    }

    /* Increase counter */
    _sprintf("    out->tuple._%d += 1 * flag;\n", (i + 1));
    _sprintf("}\n", NULL);
    
    _sprintf("\n", NULL);

    return ret;
}

static char * generate_cachef(reduction_p reduce) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO */
    int numberOfVectors = 1;

    /* cachef */
    _sprintf("inline void cachef (output_t * tuple, __local output_t * cache) {\n", NULL);
    
    for (int i = 0; i < numberOfVectors; i++) {
        _sprintf("    cache->vectors[%d] = tuple->vectors[%d];\n", i, i);
    }
    
    _sprintf("}\n", NULL);
    
    _sprintf("\n", NULL);

    return ret;
}

static char * generate_mergef(reduction_p reduce) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO */
    int aggregation_num = reduce->ref_num;

    /* mergef */
    _sprintf("inline void mergef (__local output_t * mine, __local output_t * other) {\n", NULL);

    /* Always pick the largest timestamp as the new timestamp */
    _sprintf("   if (mine->tuple.t < other->tuple.t) {\n", NULL);
    _sprintf("        mine->tuple.t = other->tuple.t;\n", NULL);
    _sprintf("    }\n", NULL);
    
    int i;
    for (i = 0; i < aggregation_num; ++i) {
        
        switch (reduce->expressions[i]) {
        case CNT:
        case SUM:
        case AVG:
            _sprintf("    mine->tuple._%d += other->tuple._%d;\n", (i + 1), (i + 1));
            break;
        case MIN:
            _sprintf("    mine->tuple._%d = (mine->tuple._%d > other->tuple._%d) ? other->tuple._%d : mine->tuple._%d;\n", 
                    (i + 1), (i + 1), (i + 1), (i + 1), (i + 1));
            break;
        case MAX:
            _sprintf("    mine->tuple._%d = (mine->tuple._%d < other->tuple._%d) ? other->tuple._%d : mine->tuple._%d;\n", 
                    (i + 1), (i + 1), (i + 1), (i + 1), (i + 1));
            break;
        default:
        //     throw new IllegalArgumentException("error: invalid aggregation type");
            break;
        }
    }
    _sprintf("    mine->tuple._%d += other->tuple._%d;\n", (i + 1), (i + 1));
    _sprintf("}\n", NULL);
    
    _sprintf("\n", NULL);

    return ret;
}

static char * generate_copyf(reduction_p reduce, int vector) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO */
    int aggregation_num = reduce->ref_num;
    int numberOfVectors = (reduce->output_schema->size + schema_get_pad(reduce->output_schema, vector)) / vector;

    /* copyf */
    _sprintf("inline void copyf (__local output_t *p, __global output_t *q) {\n", NULL);
    
    /* Compute average */
    bool containsAverage = false;
    for (int i = 0; i < aggregation_num; ++i) {
        if (reduce->expressions[i] == AVG) {
            containsAverage = true;
        }
    }
    
    if (containsAverage) {
        
        int countAttribute = aggregation_num + 1;
        _sprintf("    int count = p->tuple._%d;\n", countAttribute);
        
        for (int i = 0; i < aggregation_num; ++i)
            if (reduce->expressions[i] == AVG) {
                _sprintf("    p->tuple._%d = p->tuple._%d / (float) count;\n", (i + 1), (i + 1));
            }
    }
    
    for (int i = 0; i < numberOfVectors; i++) {
        _sprintf("    q->vectors[%d] = p->vectors[%d];\n", i, i);
    }
    
    _sprintf("}\n", NULL);
    
    _sprintf("\n", NULL);

    return ret;
}


static char * generate_source(reduction_p reduce, window_p window, char const * patch) {
    int vector = 16;

    char * source = (char *) malloc(MAX_SOURCE_LENGTH * sizeof(char)); *source = '\0';

    char * extensions = read_file("cl/templates/extensions.cl");

    char * headers = read_file("cl/templates/headers.cl");

    /* Input and output vector sizes */
    char * tuple_size = generate_tuple_size(reduce->input_schema, reduce->output_schema, vector);

    /* Input and output tuple struct */
    char * input_tuple = generate_input_tuple(reduce->input_schema, NULL, vector);

    char * output_tuple = generate_output_tuple(reduce->output_schema, NULL, vector);

    /* Window sizes */
    char * windows = generate_window_definition(window);

    /* Inline functions */
    char * initf = generate_initf(reduce);
    char * reducef = generate_reducef(reduce, patch);
    char * cachef = generate_cachef(reduce);
    char * mergef = generate_mergef(reduce);
    char * copyf = generate_copyf(reduce, vector);

    /* Template funcitons */
    char * template = read_file(REDUCTION_CODE_TEMPLATE);

    /* Assembling */
    strcat(source, extensions);

    strcat(source, headers);
    
    strcat(source, tuple_size);
    
    strcat(source, input_tuple);
    strcat(source, output_tuple);
    
    strcat(source, windows);
    
    strcat(source, initf);
    strcat(source, reducef);
    strcat(source, cachef);
    strcat(source, mergef);
    strcat(source, copyf);
    
    strcat(source, template);

    /* Free inputed/generated source */
    free(extensions);

    free(headers);

    free(tuple_size);

    free(output_tuple);
    free(input_tuple);

    free(windows);

    free(initf);
    free(reducef);
    free(cachef);
    free(mergef);
    free(copyf);
    
    free(template);

    return source;
}

void reduction_setup(void * reduce_ptr, int batch_size, window_p window, char const * patch) {
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
    char * source = generate_source(reduce, window, patch);

#ifdef OPMERGER_DEBUG
    /* Output generated code */
    char * filename = generate_filename(reduce->id, REDUCTION_CODE_FILENAME);
    printf("[REDUCTION] Printing the generated source code to file: %s\n", filename);
    print_to_file(filename, source);
    free(filename);
#endif
    
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
    args1[3] = 32 * MAX_THREADS_PER_GROUP; /* local cache size */

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_set_kernel_reduce(qid, args1, args2);
}

void reduction_process(void * reduce_ptr, batch_p batch, window_p window, batch_p output, query_event_p event) {
    reduction_p reduce = (reduction_p) reduce_ptr;
    
    long args2[2];
    {
        /* Previous pane id, based on stream start pointer, s */
        args2[0] = -1;
        long s = batch->start;
        int  t = reduce->input_schema->size;
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
        output->buffer + output->start + reduce->output_entries[0], 
        output->buffer + output->start + reduce->output_entries[1]};

    /* Execute */
    gpu_execute_reduce(
        reduce->qid,
        reduce->threads, reduce->threads_per_group,
        args2,
        (void **) (inputs), (void **) (outputs), sizeof(u_int8_t),
        event);
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

    int out_num = 100;
    if (out_num > window_counts[4] / 16) {
        out_num = window_counts[4] / 16;
    }
    for (int i=0; i<out_num; i++) {
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
    args1[3] = 32 * MAX_THREADS_PER_GROUP; /* local cache size */

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_reset_kernel_reduce(reduce->qid, args1, args2);
}