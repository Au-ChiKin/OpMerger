/* 
 * The main logic to run the experiment of merged operators
 */
#include "libgpu/gpu_agg.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>

#include "config.h"
#include "batch.h"
#include "dispatcher.h"
#include "window.h"
#include "query.h"
#include "task.h"
#include "cirbuf/circular_buffer.h"
#include "operators/reduction.h"
#include "scheduler/scheduler.h"

#define GCD_LINE_NUM 144370688 // maximum lines for input txts


/* Input data of interest from files */
void read_input_buffers(cbuf_handle_t cbufs [], int buffer_num, int batch_size);

/* Print out n tuples for debug */
void print_tuples(cbuf_handle_t cbufs [], int n);

void run_processing_gpu(
    u_int8_t * buffers [], int buffer_size, int buffer_num,
    u_int8_t * result, 
    enum test_cases mode, int work_load, int pipeline_num, bool is_merging, bool is_debug) {
    
    /* TODO extend the batch struct into a real memoery manager that could create a batch 
    according to the start and end pointer */
    /* Pre-assembled batches */
    batch_p input [8812];
    for (int b=0; b<buffer_num; b++) {
        input[b] = batch(buffer_size, 0, buffers[b], buffer_size, TUPLE_SIZE);
    }

    /* Used as an output stream */
    batch_p output = batch(6 * buffer_size, 0, result, 6 * buffer_size, TUPLE_SIZE);


    /* Start throughput monitoring */
    event_manager_p manager = event_manager_init();

    /* Start scheduler */
    scheduler_p scheduler  = scheduler_init(pipeline_num, manager);

    int const query_num = 2;
    gpu_init(query_num, pipeline_num, manager);

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

    /* Start the actual monitoring */
    monitor_p monitor = monitor_init(manager);

    /* simplified query creation */
    switch (mode) {
        /* Merely reduction cannot be merged (unlike aggregation) */
        case REDUCTION: 
            fprintf(stdout, "========== Running reduction test ===========\n");
            {
                /* Construct a reduce: sum column 8 (cpu) */
                int ref_num = 1;
                int cols [1] = {8};
                enum aggregation_types exps [1] = {SUM};

                reduction_p reduce1 = reduction(schema1, ref_num, cols, exps);

                /* Create a query */
                window_p window1 = window(256, 256, RANGE_BASE);

                int batch_size = buffer_size;
                query_p query1 = query(0, batch_size, window1, is_merging);

                query_add_operator(query1, (void *) reduce1, reduce1->operator);

                query_setup(query1);

                dispatcher_p dispatcher = dispatcher_init(scheduler, query1, 0, input, buffer_num);
                pthread_join(dispatcher_get_thread(dispatcher), NULL);

                /* For debugging */
                if (is_debug) {
                    reduction_print_output(output, buffer_size, schema1->size);
                }
            }
            break;
        default:
            fprintf(stderr, "error: wrong test case name, runs an no-op query\n");
            break;
    }

    /* Wait for worker threads */
    pthread_join(scheduler_get_thread(), NULL);

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

void read_input_buffers(cbuf_handle_t cbufs [], int buffer_num, int tuple_per_insert) {
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
        if (tupleIndex >= tuple_per_insert) {
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
    int work_load = 32; // default to be 32MB
    int batch_size = 32; // default to be 32MB per batch
    int buffer_num = 1;
    int pipeline_num = 1;
    int tuple_per_insert = batch_size * ((1024 * 1024) / TUPLE_SIZE);
    enum test_cases mode = QUERY1;

    parse_arguments(argc, argv, 
        &mode, &work_load, &batch_size, &buffer_num, &pipeline_num,
        &is_merging, &is_debug);

    if (work_load < batch_size) {
        printf("Reset batch size to be %d\n", work_load);
        batch_size = work_load;
        work_load = 1;
    } else {
        if (work_load % batch_size != 0) {
            fprintf(stderr, "error: workload shoud be muiltiple of %d if >= %d\n",
                batch_size, batch_size);
            exit(1);
        }
        work_load /= batch_size;
    }
    /* convert into in tuples */
    batch_size *= (1024 * 1024) / TUPLE_SIZE; 

    /* Read input from files */
    static int max_buffer_num = GCD_LINE_NUM / ((1024 * 1024) / TUPLE_SIZE); // about 8812
    u_int8_t * buffers [max_buffer_num];
    cbuf_handle_t cbufs [max_buffer_num];
    /* TODO: Add a dispatcher allow dispatch tuples of size different to bath size */
    tuple_per_insert = batch_size;

    if (buffer_num > max_buffer_num) {
        printf("[MAIN] the requested buffer number has exceeded the limit (%d) and is reset it\n", max_buffer_num);
        buffer_num = max_buffer_num;
    }
    for (int i=0; i<buffer_num; i++) {
        buffers[i] = (u_int8_t *) malloc(tuple_per_insert * TUPLE_SIZE * sizeof(u_int8_t)); // creates 8812 ByteBuffers
        cbufs[i] = circular_buf_init(buffers[i], tuple_per_insert * TUPLE_SIZE);
    }
    read_input_buffers(cbufs, 1, tuple_per_insert);

    print_tuples(cbufs, 32);

    /* Create output buffers */
    u_int8_t * result = (u_int8_t *) malloc( 4 * batch_size * TUPLE_SIZE * sizeof(u_int8_t));

    /* Start processing */
    run_processing_gpu(
        buffers, batch_size, buffer_num, /* input */
        result, /* output */
        mode, work_load, pipeline_num, is_merging, is_debug);  /* configs */

    /* Clear up */
    /* Temperory using 1 buffer */
    for (int i=0; i<buffer_num; i++) {
        free(buffers[i]);
        circular_buf_free(cbufs[i]);
    }
    free(result);

    return 0;
}