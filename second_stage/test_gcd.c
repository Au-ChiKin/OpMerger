/* 
 * The main logic to run the experiment of merged operators
 */
#include "libgpu/gpu_agg.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "libcirbuf/circular_buffer.h"
#include "config.h"
#include "aggregation.h"
#include "reduction.h"
#include "selection.h"
#include "batch.h"

/* Input data of interest from files */
void read_input_buffers(cbuf_handle_t cbufs [], int buffer_num);

/* Print out 10 tuples for debug */
void print_10_tuples(cbuf_handle_t cbufs []);

void run_processing_gpu(
    u_int8_t * buffers [], int buffer_size, int buffer_num, int tuple_size,
    u_int8_t * result, 
    int load, enum test_cases mode) {
    
    u_int8_t * batch = buffers[0];
    /* TODO extend the batch struct into a real memoery manager */
    batch_t wrapped_batch;
    {
        wrapped_batch.start = 0;
        wrapped_batch.end = buffer_size;
        wrapped_batch.buffer = batch;
        wrapped_batch.pane_size = buffer_size; /* tumbling window for now, use the whole buffer as a batch */
    }
    batch_p input_batch = &wrapped_batch;

    /* TODO: dynmaically decide the output buffer size */
    batch_t wrapped_output;
    {
        wrapped_output.start = 0;
        wrapped_output.end = 4 * buffer_size;
        wrapped_output.buffer = result;
        wrapped_output.pane_size = 4 * buffer_size; /* tumbling window for now, use the whole buffer as a batch */
    }
    batch_p output = &wrapped_output;

    int const query_num = 1;
    gpu_init(query_num);

    /* simplified query creation */
    switch (mode) {
        /* TODO what should be done is one selection follows anther without copying out the data */
        /* TODO deep merged or shallow merged ? */
        case MERGED_SELECTION: 
            fprintf(stdout, "========== Running merged selection test ===========\n");
            selection_init(buffer_size);
            /* TODO wrap in a general setup method */
            selection_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            selection_process(input_batch, buffer_size, tuple_size, 0, output);

            selection_print_output(output, buffer_size, tuple_size);
            
            /* TODO some internal connection */
            break;
        /* Merely reduction cannot be merged (unlike aggregation) */
        case REDUCTION: 
            fprintf(stdout, "========== Running reduction test ===========\n");
            reduction_init(buffer_size);
            /* TODO wrap in a general setup method */
            reduction_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            reduction_process(input_batch, tuple_size, 0, output);

            reduction_print_output(output, buffer_size, tuple_size);
            break;
        /* TODO: the previous multiple test cases */
        case SEPARATE_SELECTION: 
            fprintf(stdout, "========== Running separate selection test ===========\n");
            selection_init(buffer_size);
            /* TODO wrap in a general setup method */
            selection_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            selection_process(input_batch, buffer_size, tuple_size, 0, output);

            selection_print_output(output, buffer_size, tuple_size);

            /* Connection */

            selection_init(buffer_size);
            /* TODO wrap in a general setup method */
            selection_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            selection_process(input_batch, buffer_size, tuple_size, 0, output);

            selection_print_output(output, buffer_size, tuple_size);

            /* Connection */

            selection_init(buffer_size);
            /* TODO wrap in a general setup method */
            selection_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            selection_process(input_batch, buffer_size, tuple_size, 0, output);

            selection_print_output(output, buffer_size, tuple_size);
            break;
        case QUERY2:
            /**
             * Query 2:
             * select timestamp, category, sum(cpu) as totalCpu
             * from TaskEvents [range 60 slide 1]
             * group by category
             * 
             * Since we have not yet implemented aggregation
             * 
             * Query 2 - variant:
             * select timestamp, category, sum(cpu) as totalCpu
             * from TaskEvents [range 60 slide 1]
             * where category == 1
             **/
            fprintf(stdout, "========== Running query2 of google cluster dataset ===========\n");
            selection_init(buffer_size);
            /* TODO wrap in a general setup method */
            selection_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            selection_process(input_batch, buffer_size, tuple_size, 0, output);

            selection_print_output(output, buffer_size, tuple_size);

            /* TODO a connection here but not merging one */

            reduction_init(buffer_size);
            /* TODO wrap in a general setup method */
            reduction_setup(buffer_size, tuple_size);
            /* TODO wrap in a general query process method */
            reduction_process(input_batch, tuple_size, 0, output);

            reduction_print_output(output, buffer_size, tuple_size);
            break;
        default: 
            break; 
    }

    gpu_free();

}

void print_tuples(cbuf_handle_t cbufs [], int n) {
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
    int work_load = 1; // default to be 1MB
    enum test_cases mode = MERGED_AGGREGATION;

    parse_arguments(argc, argv, &mode, &work_load);

    /* Read input from files */

    static int const gcd_lines = 144370688; /* maximum lines for input txts */
    static int const buffers_num = 1; // gcd_lines / BUFFER_SIZE; // about 8812
    u_int8_t * buffers [buffers_num];
    cbuf_handle_t cbufs [buffers_num];
    for (int i=0; i<buffers_num; i++) {
        buffers[i] = (u_int8_t *) malloc(BUFFER_SIZE * TUPLE_SIZE * sizeof(u_int8_t)); // creates 8812 ByteBuffers
        cbufs[i] = circular_buf_init(buffers[i], BUFFER_SIZE * TUPLE_SIZE);
    }
    read_input_buffers(cbufs, buffers_num);

    print_tuples(cbufs, 32);

    /* Create output buffers */
    
    u_int8_t * result = (u_int8_t *) malloc( 4 * BUFFER_SIZE * TUPLE_SIZE * sizeof(u_int8_t));

    /* Start processing */

    run_processing_gpu(
        buffers, BUFFER_SIZE, buffers_num, TUPLE_SIZE, /* input */
        result, /* output */
        work_load, mode); /* configs */

    /* Clear up */

    for (int i=0; i<buffers_num; i++) {
        free(buffers[i]);
        circular_buf_free(cbufs[i]);
    }
    free(result);

    return 0;
}