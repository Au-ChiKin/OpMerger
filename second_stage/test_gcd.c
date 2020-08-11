/* 
 * The main logic to run the experiment of merged operators
 */
#include "libgpu/gpu_agg.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "cirbuf/circular_buffer.h"
#include "config.h"
#include "operators/selection.h"
#include "operators/reduction.h"
#include "operators/aggregation.h"
#include "batch.h"
#include "window.h"
#include "query.h"

#define GCD_LINE_NUM 144370688 // maximum lines for input txts


/* Input data of interest from files */
void read_input_buffers(cbuf_handle_t cbufs [], int buffer_num);

/* Print out n tuples for debug */
void print_tuples(cbuf_handle_t cbufs [], int n);

void run_processing_gpu(
    u_int8_t * buffers [], int buffer_size, int buffer_num,
    u_int8_t * result, 
    enum test_cases mode, bool is_merging, bool is_debug) {
    
    /* TODO extend the batch struct into a real memoery manager that could create a batch 
    according to the start and end pointer */
    batch_p input [8812];
    for (int b=0; b<buffer_num; b++) {
        input[b] = batch(buffer_size, 0, buffers[b], buffer_size, TUPLE_SIZE);
    }

    /* TODO: dynmaically decide the output buffer size */
    batch_p output = batch(4 * buffer_size, 0, result, 4 * buffer_size, TUPLE_SIZE);

    int const query_num = 2;
    gpu_init(query_num);

    /* Construct schemas */
    schema_p schema1 = schema();
    schema_add_attr(schema1, TYPE_LONG);  /* time_stamp */
    schema_add_attr(schema1, TYPE_LONG);  /* job_id */
    schema_add_attr(schema1, TYPE_LONG);  /* task_id */
    schema_add_attr(schema1, TYPE_LONG);  /* machine_id */
    schema_add_attr(schema1, TYPE_INT);   /* user_id */
    schema_add_attr(schema1, TYPE_INT);   /* event_type */
    schema_add_attr(schema1, TYPE_INT);   /* category */
    schema_add_attr(schema1, TYPE_INT);   /* priority */
    schema_add_attr(schema1, TYPE_FLOAT); /* cpu */
    schema_add_attr(schema1, TYPE_FLOAT); /* ram */
    schema_add_attr(schema1, TYPE_FLOAT); /* disk */
    schema_add_attr(schema1, TYPE_INT);   /* constraints */

    /* simplified query creation */
    switch (mode) {
        case SELECTION: 
            fprintf(stdout, "========== Running selection test ===========\n");
            {
                /* Construct a select: where column 6 (category) == 0 */
                int col1 = 6;

                enum comparor com1 = EQUAL;

                int i1 = 0;
                ref_value_p val1 = ref_value();
                val1->i = &i1;
                
                selection_p select1 = selection(schema1, col1, val1, com1);

                /* Create a query */
                window_p window1 = window(64, 64, RANGE_BASE);

                int batch_size = buffer_size;
                query_p query1 = query(0, batch_size, window1, is_merging);

                query_add_operator(query1, (void *) select1, select1->operator);

                query_setup(query1);

                for (int b=0; b<buffer_num; b++) {
                    /* Execute */
                    query_process(query1, input[b], output);

                    /* For debugging */
                    selection_print_output(select1, output);
                }
            }

            break;
        /* Merely reduction cannot be merged (unlike aggregation) */
        case REDUCTION: 
            fprintf(stdout, "========== Running reduction test ===========\n");
            {
                /* Construct a reduce: sum column 8 (cpu) */
                int col1 = 8;

                reduction_p reduce1 = reduction(schema1, col1);

                /* Create a query */
                window_p window1 = window(256, 256, RANGE_BASE);

                int batch_size = buffer_size;
                query_p query1 = query(0, batch_size, window1, is_merging);

                query_add_operator(query1, (void *) reduce1, reduce1->operator);

                query_setup(query1);

                for (int b=0; b<buffer_num; b++) {
                    /* Execute */
                    query_process(query1, input[b], output);

                    /* For debugging */
                    reduction_print_output(output, buffer_size, schema1->size);
                }
            }
            break;
        case TWO_SELECTION: 
            fprintf(stdout, "========== Running separate selection test ===========\n");
            {
                /* Construct a select: where column 5 (eventType) == 0 */
                int col1 = 5;

                enum comparor com1 = EQUAL;

                int i1 = 0;
                ref_value_p val1 = ref_value();
                val1->i = &i1;
                
                selection_p select1 = selection(schema1, col1, val1, com1);

                /* Construct a select: where column 6 (category) == 2 */
                int col2 = 6;

                enum comparor com2 = EQUAL;

                int i2 = 2;
                ref_value_p val2 = ref_value();
                val2->i = &i2;
                
                selection_p select2 = selection(schema1, col2, val2, com2);

                /* Create a query */
                window_p window1 = window(64, 64, RANGE_BASE);

                int batch_size = buffer_size;
                query_p query1 = query(0, batch_size, window1, is_merging);

                query_add_operator(query1, (void *) select1, select1->operator);
                query_add_operator(query1, (void *) select2, select2->operator);

                query_setup(query1);

                for (int b=0; b<buffer_num; b++) {
                    /* Execute */
                    query_process(query1, input[b], output);

                    /* For debugging */
                    if (is_debug) {
                        selection_print_output(select2, output);
                    }
                }
            }
            break;
        case QUERY2:
            /**
             * Query 2:
             * 
             * input: TaskEvents
             *     long  time_stamp;
             *     long  job_id; 
             *     long  task_id;
             *     long  machine_id;
             *     int   user_id;
             *     int   event_type;
             *     int   category;
             *     int   priority;
             *     float cpu;
             *     float ram;
             *     float disk;
             *     int constraints;
             * 
             * output: CPUusagePerCategory 
             *     long timestamp 
             *     int category 
             *     float totalCpu
             * 
             * query:
             *     select timestamp, category, sum(cpu) as totalCpu
             *     from TaskEvents [range 60 slide 1]
             *     group by category
             * 
             * Since we have not yet implemented aggregation
             * 
             * Query 2 - variant:
             *     select timestamp, category, sum(cpu) as totalCpu
             *     from TaskEvents [range 60 slide 1]
             *     where category == 0
             * 
             * Output becomes
             * output: CPUusageForCategory1
             *     long timestamp 
             *     float totalCpu
             *     int counter
             **/
            fprintf(stdout, "========== Running query2 of google cluster dataset ===========\n");
            {
                /* Construct a select: where column 6 (category) == 0 */
                int col1 = 6;

                enum comparor com1 = EQUAL;

                int i1 = 0;
                ref_value_p val1 = ref_value();
                val1->i = &i1;
                
                selection_p select1 = selection(schema1, col1, val1, com1);

                /* Construct a reduce: sum column 8 (cpu) */
                int col2 = 8;

                reduction_p reduce1 = reduction(schema1, col2);

                /* Create a query */
                window_p window1 = window(1024, 1024, RANGE_BASE);

                int batch_size = buffer_size;
                query_p query1 = query(0, batch_size, window1, is_merging);

                query_add_operator(query1, (void *) select1, select1->operator);
                query_add_operator(query1, (void *) reduce1, reduce1->operator);

                query_setup(query1);

                for (int b=0; b<buffer_num; b++) {
                    /* Execute */
                    /* Temparory using the first buffer */
                    query_process(query1, input[0], output);

                    /* For debugging */
                    if (is_debug) {
                        reduction_print_output(output, input[b]->size, schema1->size);
                    }
                }
            }
            break;
        default: 
            break; 
    }

    free(output);

    for (int b=0; b<buffer_num; b++) {
        free(input[b]);
    }

    gpu_free();

}

void print_tuples(cbuf_handle_t cbufs [], int n) {
    float sum = 0;

    printf("[MAIN] Printing %d tuples as sample\n", n);
    for (int i=0; i<n; i++) {
        input_t tuple;
        circular_buf_read_bytes(cbufs[0], tuple.vectors, TUPLE_SIZE);

        printf("       Tuple %-3ld has %5d %5d %5d      %.5f\n", 
            tuple.tuple.time_stamp, 
            tuple.tuple.event_type,
            tuple.tuple.category,
            tuple.tuple.priority,
            tuple.tuple.cpu);

        sum += tuple.tuple.cpu;
        if (i % 32 == 31) {
            printf("Sum is %09.5f\n", sum);
            sum = 0;
        }
    }
    printf("       ......\n");
}

void read_input_buffers(cbuf_handle_t cbufs [], int buffer_num) {
    // int extraBytes = 5120 * TUPLE_SIZE; // for?

    char dataDir [64] = "../datasets/google-cluster-data/";

    char filenames [4][64] = {
        "norm-event-types.txt",
        "categories.txt",
        "priorities.txt",
        "cpu-utilisation.txt",
    };
    for (int i=0; i<4; i++) {
        char tmp [64] = "";
        strcpy(tmp, dataDir);
        strcat(tmp, filenames[i]);
        strcpy(filenames[i], tmp);
    }

    bool containsInts [4] = { true, true, true, false };

    FILE * files [4];
    for (int i=0; i<4; i++) {
        printf("[MAIN] loading file %s\n", filenames[i]);
        files[i] = fopen(filenames[i], "r");
        if (!files[i]) {
            fprintf(stderr, "error: cannot open file %s\n", filenames[i]);
            exit(1);
        }
    }

    char line [256] = "";
    int line_num = 0; /* line_num is the 'absolute' index while tupleIndex is relative to a buffer */
    int bufferIndex = 0, tupleIndex = 0, attributeIndex;
    bool is_finished = false;
    cbuf_handle_t cbuf;
    while (!is_finished) {
        if (tupleIndex >= BUFFER_SIZE) {
            tupleIndex = 0; // tupleIndex in a buffer
            bufferIndex ++;
            if (bufferIndex >= buffer_num) {
                // buffer has been full
                break;
            }
        }
        cbuf = cbufs[bufferIndex];

        input_t tuple;
        tuple.tuple.time_stamp = line_num;
        tuple.tuple.job_id = 0;
        tuple.tuple.task_id = 0;
        tuple.tuple.machine_id = 0;
        tuple.tuple.user_id = 0;

        /* Load file into memory */
        for (int i = 0; i < 4; i++) {

            if (fscanf(files[i], "%s", line) != -1) {

                attributeIndex = 36 + (i * sizeof(int));

                if (containsInts[i]) {
                    union {
                        int num;
                        uint8_t vectors[sizeof(int)];
                    } num;

                    num.num = atoi(line);
                    for (int j=0; j<4; j++) {
                        tuple.vectors[attributeIndex+j] = num.vectors[j];
                    }
                } else {
                    union {
                        float num;
                        uint8_t vectors[sizeof(float)];
                    } num;

                    num.num = atof(line);
                    for (int j=0; j<4; j++) {
                        tuple.vectors[attributeIndex+j] = num.vectors[j];
                    }
                }

                if (i == 3) { // Last file, inc index
                    tupleIndex ++;
                    line_num += 1;
                }
            } else {
                is_finished = true;
            }
        }

        circular_buf_put_bytes(cbuf, tuple.vectors, TUPLE_SIZE);
    } // While !is_finished

    // Lets forget about the case when the buffers size is bigger than the avaliable tuples

    /* Fill in the extra lines */
    // printf("MAIN %d lines last buffer position at %d has remaining ? %5s (%d bytes)",
    //     line_num, 
    //     tupleIndex, 
    //     tupleIndex < BUFFER_SIZE - 1 ? "No" : "True", 
    //     tupleIndex - (BUFFER_SIZE - 1));

    // if (i != 0) {
    //     int destPos = buffer.capacity() - extraBytes;
    //     System.arraycopy(buffer.array(), 0, buffer.array(), destPos, extraBytes);
    // }

    for (int i=0; i<4; i++) {
        fclose(files[i]);
    }

    printf("[MAIN] finished loading files\n");
}

int main(int argc, char * argv[]) {

    /* Arguments */
    bool is_merging = false;
    bool is_debug = false;
    int work_load = 1; // default to be 1MB
    enum test_cases mode = SELECTION;

    parse_arguments(argc, argv, &mode, &work_load, &is_merging, &is_debug);

    /* Read input from files */
    static int buffers_num = GCD_LINE_NUM / BUFFER_SIZE; // about 8812
    u_int8_t * buffers [buffers_num];
    cbuf_handle_t cbufs [buffers_num];

    if (buffers_num > work_load) {
        buffers_num =work_load;
    }

    /* Temperory use only 1 buffer */
    for (int i=0; i<1; i++) {
        buffers[i] = (u_int8_t *) malloc(BUFFER_SIZE * TUPLE_SIZE * sizeof(u_int8_t)); // creates 8812 ByteBuffers
        cbufs[i] = circular_buf_init(buffers[i], BUFFER_SIZE * TUPLE_SIZE);
    }
    read_input_buffers(cbufs, 1);

    print_tuples(cbufs, 32);

    /* Create output buffers */
    u_int8_t * result = (u_int8_t *) malloc( 4 * BUFFER_SIZE * TUPLE_SIZE * sizeof(u_int8_t));

    /* Start processing */
    run_processing_gpu(
        buffers, BUFFER_SIZE, buffers_num, /* input */
        result, /* output */
        mode, is_merging, is_debug);  /* configs */

    /* Clear up */
    for (int i=0; i<buffers_num; i++) {
        free(buffers[i]);
        circular_buf_free(cbufs[i]);
    }
    free(result);

    return 0;
}