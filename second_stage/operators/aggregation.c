#include "aggregation.h"

#include <stdio.h>
#include <string.h>

#include "config.h"
#include "generators.h"
#include "helpers.h"
#include "libgpu/gpu_agg.h"
#include "schema.h"

static int free_id = 0;

aggregation_p aggregation(
    schema_p input_schema, 
    int ref_num, int const refs[], enum aggregation_types const expressions[],
    int group_num, int const groups[]) {

    aggregation_p p = (aggregation_p) malloc(sizeof(aggregation_t));

    /* Set up operator */
    p->operator = (operator_p) malloc(sizeof (operator_t));
    {
        p->operator->setup = (void *) aggregation_setup;
        // p->operator->reset = (void *) aggregation_reset;
        p->operator->process = (void *) aggregation_process;
        p->operator->process_output = (void *) aggregation_process_output;
        p->operator->get_output_buffer = (void *) aggregation_get_output_buffer;
        p->operator->get_output_schema_size = (void *) aggregation_get_output_schema_size;

        p->operator->type = OPERATOR_AGGREGATE;

        strcpy(p->operator->code_name, AGGREGATION_CODE_FILENAME);
    }

    p->id = free_id++;

    p->input_schema = input_schema;

    p->ref_num = ref_num;
    if (ref_num > AGGREGATION_MAX_REFERENCE) {
        fprintf(stderr, "error: the number of reference has exceeded the limit (%d)\n", 
            AGGREGATION_MAX_REFERENCE);
        exit(1);
    }
    for (int i=0; i<ref_num; i++) {
        p->refs[i] = refs[i];
        p->expressions[i] = expressions[i];
    }

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

static char * generate_initf(aggregation_p aggregate) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO support aggregation types and multiple of them */
    int aggregation_num = aggregate->ref_num;

    /* initf */
    _sprint("inline void initf (output_t *p) {\n");
    
    /* Set timestamp */
    _sprint("    p->tuple.t = 0;\n");

    int i;
    for (i = 0; i < aggregation_num; ++i) {
        
        switch (aggregate->expressions[i]) {
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
    _sprint("}\n");
    
    _sprint("\n");

    return ret;
}

static char * generate_updatef (aggregation_p aggregate, char const * patch) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";
    int i;

    /* TODO support aggregation types and multiple of them */
    int aggregation_num = aggregate->ref_num;

    /* aggregatef */
    _sprint("inline void aggregatef (output_t *out, __global input_t *in) {\n");
    
    /* Reserve for select */
    _sprint("    int flag = 1;\n\n");

    /* Insert patch */
    if (patch) {
        _sprintf("%s", patch);
    }

    /* Set timestamp */
    _sprint("    out->tuple.t = (!flag || (flag && (out->tuple.t > in->tuple.t))) ? out->tuple.t : in->tuple.t;\n");

    /* Aggregation */
    for (i = 0; i < aggregation_num; ++i) {
        
        int column = aggregate->refs[i];
        
        switch (aggregate->expressions[i]) {
        case CNT: 
            _sprintf("    out->tuple._%d += 1 * flag;\n", (i + 1)); 
            break;
        case SUM:
        case AVG: 
            _sprintf("    out->tuple._%d += in->tuple._%d * flag;\n", (i + 1), column); 
            break;
        case MIN:
            _sprintf("    out->tuple._%d = (flag && (out->tuple._%d > in->tuple._%d)) ? in->tuple._%d : out->tuple._%d;\n",
                    (i + 1), (i + 1), column, column, (i + 1));
            break;
        case MAX:
            _sprintf("    out->tuple._%d = (flag && (out->tuple._%d < in->tuple._%d)) ? in->tuple._%d : out->tuple._%d;\n",
                    (i + 1), (i + 1), column, column, (i + 1));
            break;
        default:
            fprintf(stderr, "error: invalid aggregation type\n");
            break;
        }
    }

    /* Increase counter */
    _sprintf("    out->tuple._%d += 1 * flag;\n", (i + 1));
    _sprint("}\n");
    
    _sprint("\n");

    return ret;
}

static char * generate_cachef(aggregation_p aggregate) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO */
    int numberOfVectors = 1;

    /* cachef */
    _sprint("inline void cachef (output_t * tuple, __local output_t * cache) {\n");
    
    for (int i = 0; i < numberOfVectors; i++) {
        _sprintf("    cache->vectors[%d] = tuple->vectors[%d];\n", i, i);
    }
    
    _sprint("}\n");
    
    _sprint("\n");

    return ret;
}

static char * generate_mergef(aggregation_p aggregate) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO */
    int aggregation_num = aggregate->ref_num;

    /* mergef */
    _sprint("inline void mergef (__local output_t * mine, __local output_t * other) {\n");

    /* Always pick the largest timestamp as the new timestamp */
    _sprint("   if (mine->tuple.t < other->tuple.t) {\n");
    _sprint("        mine->tuple.t = other->tuple.t;\n");
    _sprint("    }\n");
    
    int i;
    for (i = 0; i < aggregation_num; ++i) {
        
        switch (aggregate->expressions[i]) {
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
    _sprint("}\n");
    
    _sprint("\n");

    return ret;
}

static char * generate_copyf(aggregation_p aggregate, int vector) {
    char * ret = (char *) malloc(512 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    /* TODO */
    int aggregation_num = aggregate->ref_num;
    int numberOfVectors = (aggregate->output_schema->size + schema_get_pad(aggregate->output_schema, vector)) / vector;

    /* copyf */
    _sprint("inline void copyf (__local output_t *p, __global output_t *q) {\n");
    
    /* Compute average */
    bool containsAverage = false;
    for (int i = 0; i < aggregation_num; ++i) {
        if (aggregate->expressions[i] == AVG) {
            containsAverage = true;
        }
    }
    
    if (containsAverage) {
        
        int countAttribute = aggregation_num + 1;
        _sprintf("    int count = p->tuple._%d;\n", countAttribute);
        
        for (int i = 0; i < aggregation_num; ++i)
            if (aggregate->expressions[i] == AVG) {
                _sprintf("    p->tuple._%d = p->tuple._%d / (float) count;\n", (i + 1), (i + 1));
            }
    }
    
    for (int i = 0; i < numberOfVectors; i++) {
        _sprintf("    q->vectors[%d] = p->vectors[%d];\n", i, i);
    }
    
    _sprint("}\n");
    
    _sprint("\n");

    return ret;
}


static char * generate_source(aggregation_p aggregate, window_p window, char const * patch) {
    int vector = 16;

    char * source = (char *) malloc(MAX_SOURCE_LENGTH * sizeof(char)); *source = '\0';

    char * extensions = read_file("cl/templates/extensions.cl");

    char * headers = read_file("cl/templates/headers.cl");

    /* Input and output vector sizes */
    char * tuple_size = generate_tuple_size(aggregate->input_schema, aggregate->output_schema, vector);

    /* Input and output tuple struct */
    char * input_tuple = generate_input_tuple(aggregate->input_schema, NULL, vector);

    char * output_tuple = generate_output_tuple(aggregate->output_schema, NULL, vector);

    /* Window sizes */
    char * windows = generate_window_definition(window);

    /* Inline functions */
    char * initf = generate_initf(aggregate);
    char * aggregatef = generate_aggregatef(aggregate, patch);
    char * cachef = generate_cachef(aggregate);
    char * mergef = generate_mergef(aggregate);
    char * copyf = generate_copyf(aggregate, vector);

    /* Template funcitons */
    char * template = read_file(AGGREGATION_CODE_TEMPLATE);

    /* Assembling */
    strcat(source, extensions);

    strcat(source, headers);
    
    strcat(source, tuple_size);
    
    strcat(source, input_tuple);
    strcat(source, output_tuple);
    
    strcat(source, windows);
    
    strcat(source, initf);
    strcat(source, aggregatef);
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
    free(aggregatef);
    free(cachef);
    free(mergef);
    free(copyf);
    
    free(template);

    return source;
}


void aggregation_setup(void * aggregate_ptr, int batch_size, window_p window, char const * patch) {
    aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    /* Operator setup */
    aggregate->batch_size = batch_size;
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
    int out_tuple_size = aggregate->output_schema->size;
    int output_size = batch_size * out_tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
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

void aggregation_process(void * aggregate_ptr, batch_p batch, window_p window, u_int8_t ** processed_outputs, query_event_p event) {
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

    /* Execute */
    gpu_execute_aggregate(
        aggregate->qid,
        aggregate->threads, aggregate->threads_per_group,
        args2,
        (void **) (inputs), (void **) (processed_outputs), sizeof(u_int8_t),
        event);
        // passing the batch without deserialisation
}

void aggregation_process_output(void * aggregate_ptr, batch_p outputs) {
    // aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    int current_offset = 0;
    // int tuple_size = aggregate->output_schema->size;
    int batch_size = outputs->size;

    /* Deserialise output buffer */
    int window_counts_size = 20; /* 4 integers, +1 that is the mark */
    int * window_counts = (int *) (outputs->buffer + current_offset);
    current_offset += window_counts_size;
    
    outputs->closing_windows = window_counts[0];
    outputs->pending_windows = window_counts[1];
    outputs->complete_windows = window_counts[2];
    outputs->opening_windows = window_counts[3];

    /* Keep only the windows */
    outputs->start += current_offset;

    // typedef struct {
    //     long t; /* timestamp */
    //     float _1; /* sum */
    //     int _2; /* count */
    // } output_tuple_t;

    // int output_size = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    // output_tuple_t * output = (output_tuple_t *) (outputs->buffer + current_offset);
    // current_offset += output_size;

    // outputs->closing_windows = (u_int8_t *) malloc(window_counts[0] * tuple_size * sizeof(u_int8_t));
    // memcpy(outputs->closing_windows, output, window_counts[0] * tuple_size);
    // output += window_counts[0] * tuple_size;

    // outputs->pending_windows = (u_int8_t *) malloc(window_counts[1] * tuple_size * sizeof(u_int8_t));
    // memcpy(outputs->pending_windows, output, window_counts[1] * tuple_size);
    // output += window_counts[1] * tuple_size;

    // outputs->complete_windows = (u_int8_t *) malloc(window_counts[2] * tuple_size * sizeof(u_int8_t));
    // memcpy(outputs->complete_windows, output, window_counts[2] * tuple_size);
    // output += window_counts[2] * tuple_size;

    // outputs->opening_windows = (u_int8_t *) malloc(window_counts[3] * tuple_size * sizeof(u_int8_t));
    // memcpy(outputs->opening_windows, output, window_counts[3] * tuple_size);
    // output += window_counts[3] * tuple_size;

    /* Print for debug */
    // printf("[Results] Required Output buffer size is %d\n", current_offset);

    // printf("[Results] Closing Windows: %d    ", window_counts[0]);
    // printf(          "Pending Windows: %d    ", window_counts[1]);
    // printf(         "Complete Windows: %d    ", window_counts[2]);
    // printf(          "Opening Windows: %d    ", window_counts[3]);
    // printf(     "Output Buffer Position: %d\n", window_counts[4]);

    // {
    //     int out_num = 100;
    //     if (out_num > window_counts[4] / 16) {
    //         out_num = window_counts[4] / 16;
    //     }
    //     for (int i=0; i<out_num; i++) {
    //         printf("[Results] %-8d %-12ld %09.5f  %d\n", i, output->t, output->_1, output->_2);
    //         output += 1;
    //     }
    // }
}

u_int8_t ** aggregation_get_output_buffer(void * aggregate_ptr, batch_p output) {
    aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    u_int8_t ** outputs = (u_int8_t **) malloc(5 * sizeof(u_int8_t *));

    /* Validate whether the given output buffer is big enough */
    int batch_size = aggregate->batch_size;
    int tuple_size = aggregate->output_schema->size;

    if ((output->end - output->start) < (long) (20 + 4 * batch_size * tuple_size)) {
        fprintf(stderr, "error: Expected output size has exceeded the given output buffer size (%s)\n", __FUNCTION__);
        exit(1);
    }

    /* Set output buffer addresses and entries */
    for (int i=0; i<5; i++) {
        outputs[i] = output->buffer + output->start + aggregate->output_entries[i];
    }

    return outputs;
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

int aggregation_get_output_schema_size(void * aggregate_ptr) {
    aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    return aggregate->output_schema->size;
}

void aggregation_reset(void * aggregate_ptr, int new_batch_size) {
    // aggregation_p aggregate = (aggregation_p) aggregate_ptr;

    // int tuple_size = aggregate->input_schema->size;

    // /* Operator setup */
    // for (int i=0; i<AGGREGATION_KERNEL_NUM; i++) {
    //     aggregate->threads[i] = new_batch_size;

    //     if (new_batch_size < MAX_THREADS_PER_GROUP) {
    //         aggregate->threads_per_group[i] = new_batch_size;
    //     } else {
    //         aggregate->threads_per_group[i] = MAX_THREADS_PER_GROUP;
    //     }
    // }
    
    // /* TODO */
    // int output_size = new_batch_size * tuple_size;

    // /* GPU kernels setup */
    // int args1 [6];
    // args1[0] = new_batch_size; /* tuples */
    // args1[1] = new_batch_size * tuple_size; /* input size */
    // args1[2] = output_size;
    // args1[3] = HASH_TABLE_SIZE;
    // args1[4] = PARTIAL_WINDOWS;
    // args1[5] = aggregate->key_length * MAX_THREADS_PER_GROUP; /* local cache size */

    // long args2 [2];
    // args2[0] = 0; /* Previous pane id   */
    // args2[1] = 0; /* Batch start offset */

    // gpu_reset_kernel_aggregate(aggregate->qid, args1, args2);
}