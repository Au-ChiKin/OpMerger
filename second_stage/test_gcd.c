/* 
 * The main logic to run the experiment of merged operators
 * 
 * Query:
 * select timestamp, category, sum(cpu) as totalCpu
 * from TaskEvents [range 60 slide 1]
 * grop by category
 * 
 * Query 1: (aggregation)
 * select others, category(aggregated), sum(cpu)
 * from TaskEvents
 * group by category
 * output as TaskEvents1
 *     input tuple 1 time + 11 attr
 *     output tuple 1 time + 11 attr (but timestamp, cpu and category are different)
 * 
 * Query 2: (projection)
 * select timestamp, category, sum(cpu) as totalCpu
 * from TaskEvents
 * group by category
 *     input tuple 1 time + 11 attr (output from the first query)
 *     output tuple 1 time + 2 attr
 */
#include "gpu_agg.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "libcirbuf/circular_buffer.h"
#include "config.h"
#include "aggregation.h"
#include "reduction.h"
#include "batch.h"

/* Input data of interest from files */
void read_input_buffers(cbuf_handle_t cbufs [], int buffer_num);

/* Print out 10 tuples for debug */
void print_10_tuples(cbuf_handle_t cbufs []);

void run_processing_gpu(cbuf_handle_t buffers [], int size, int * result, int load, enum test_cases mode) {
    u_int8_t * batch = (u_int8_t *) malloc(size * sizeof(tuple_t));
    /* TODO copy data from buffers into this batch */
    batch_t wrapped_batch;
    wrapped_batch.start = 0;
    wrapped_batch.end = size;
    wrapped_batch.buffer = batch;
    wrapped_batch.pane_size = size; /* tumbling window for now */
    batch_p input_batch = &wrapped_batch;

    int const query_num = 1;
    gpu_init(query_num);

    /* simplified query creation */
    switch (mode) {
        case MERGED_AGGREGATION: 
            fprintf(stdout, "========== Running merged aggregation test ===========\n");
            aggregation(16384, 64);
            break;
        case MERGED_REDUCTION: 
            fprintf(stdout, "========== Running sepaerate select test ===========\n");
            /* TODO wrap in a general setup method */
            reduction_setup(16384, 64);
            /* TODO wrap in a general query process method */
            // reduction_process(input_batch, 64 /* tuple_size */, 0);
            break;
        default: 
            break; 
    }

    gpu_free();
}

int main(int argc, char * argv[]) {

    int work_load = 1; // default to be 1MB
    enum test_cases mode = MERGED_AGGREGATION;

    parse_arguments(argc, argv, &mode, &work_load);

    /* Read input from files */

    /* 144370688 lines for each txt, 144370688 / BUFFER_SIZE is about 8812 */
    static int const task_num = 1; // 8812;
    u_int8_t * buffers [task_num];
    cbuf_handle_t cbufs [task_num];
    for (int i=0; i<task_num; i++) {
        buffers[i] = (u_int8_t *) malloc(BUFFER_SIZE * TUPLE_SIZE * sizeof(u_int8_t)); // creates 8812 ByteBuffers
        cbufs[i] = circular_buf_init(buffers[i], BUFFER_SIZE * TUPLE_SIZE);
    }
    read_input_buffers(cbufs, task_num);

    /* Start processing */

    // int results_size = 0;
    // tuple_t results_tuple[BUFFER_SIZE];

    int results[BUFFER_SIZE];
    for (int i=0; i<BUFFER_SIZE; i++) {
        results[i] = 0;
    }

    run_processing_gpu(cbufs, BUFFER_SIZE, results, work_load, mode);

    for (int i=0; i<task_num; i++) {
        free(buffers[i]);
        circular_buf_free(cbufs[i]);
    }

    return 0;
}

void print_10_tuples(cbuf_handle_t cbufs []) {
    for (int i=0; i<10; i++) {
        input_t tuple;
        circular_buf_read_bytes(cbufs[0], tuple.vectors, TUPLE_SIZE);

        printf("Tuple %ld has %d %d %d %.2f\n", 
            tuple.tuple.time_stamp, 
            tuple.tuple.event_type,
            tuple.tuple.category,
            tuple.tuple.priority,
            tuple.tuple.cpu);
    }
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
    int line_num = 0;
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

        line_num += 1;

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

                    num.num = atoi(line);
                    for (int j=0; j<4; j++) {
                        tuple.vectors[attributeIndex+j] = num.vectors[j];
                    }
                }

                tupleIndex ++;
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
