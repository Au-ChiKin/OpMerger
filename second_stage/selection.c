#include "selection.h"

#include <stdio.h>

#include "helpers.h"
#include "gpu_agg.h"
#include "config.h"

#define KERNEL_NUM 4
#define TUPLES_PER_THREADS 2

static size_t threads[KERNEL_NUM];
static size_t threads_per_group [KERNEL_NUM];

void selection_init(int batch_size) {
    for (int i=0; i<KERNEL_NUM; i++) {
        threads[i] = batch_size / TUPLES_PER_THREADS;

        if (threads[i] < MAX_THREADS_PER_GROUP) {
            threads_per_group[i] = threads[0];
        } else {
            threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
}

void selection_setup(int batch_size, int tuple_size) {
    char * source = read_source("select.cl");
    int qid = gpu_get_query(source, 2, 1, 4);
    
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    gpu_set_output (qid, 0, 4 * batch_size,                        0, 1, 1, 0, 1); /*      Flags */
    gpu_set_output (qid, 1, 4 * batch_size,                        0, 1, 0, 0, 1); /*    Offsets */
    gpu_set_output (qid, 2, 4 * threads[0] / threads_per_group[0], 0, 0, 0, 0, 1); /* Partitions */
    gpu_set_output (qid, 3, batch_size * tuple_size,               1, 0, 0, 1, 0); /*    Results */
    
    int args[3];
    args[0] = batch_size * tuple_size;
    args[1] = batch_size;
    args[2] = 4 * threads_per_group[0] * TUPLES_PER_THREADS;

    gpu_set_kernel_select (qid, args);
}

void selection_process(batch_p batch, int batch_size, int tuple_size, int qid, batch_p output) {
    
    /* Set input */
    
    // IQueryBuffer inputBuffer = batch.getBuffer();
    // int start = batch.getBufferStartPointer();
    // int end   = batch.getBufferEndPointer();
    
    // TheGPU.getInstance().setInputBuffer(qid, 0, inputBuffer, start, end);
    
    /* Set output for a previously executed operator */
    
    // WindowBatch pipelinedBatch = TheGPU.getInstance().shiftUp(batch);
    
    // IOperatorCode pipelinedOperator = null; 
    // if (pipelinedBatch != null) {
    //     pipelinedOperator = pipelinedBatch.getQuery().getMostUpstreamOperator().getGpuCode();
    //     pipelinedOperator.configureOutput (qid);
    // }

    u_int8_t * inputs [1] = {batch->buffer + batch->start};

    u_int8_t * outputs [3] = {
        output->buffer + batch->start,
        output->buffer + batch->start + 4 * batch_size,
        output->buffer + batch->start + 4 * batch_size + 4 * threads[0] / threads_per_group[0]};

    /* Execute */
    gpu_execute(qid, threads, threads_per_group, 
        (void *) inputs, (void *) outputs, sizeof(u_int8_t));
    
    // if (pipelinedBatch != null)
    //     pipelinedOperator.processOutput (qid, pipelinedBatch);
    
    // api.outputWindowBatchResult (pipelinedBatch);
}

void selection_print_output(batch_p outputs, int batch_size, int tuple_size) {
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

    int partitions_size = 4 * threads[0] / threads_per_group[0];
    int * partitions = (int *) (outputs->buffer + current_offset);
    current_offset += partitions_size;

    int output_size = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    output_tuple_t * output = (output_tuple_t *) (outputs->buffer + current_offset);
    current_offset += output_size;

    /* Calculate output tuples */
    int count = 0;
    for (int i=0; i<threads[0] / threads_per_group[0]; i++) {
        count += partitions[i];
    }

    /* print */
    printf("[Results] Required Output buffer size is %d\n", current_offset);
    printf("[Results] Output tuple numbers: %d\n", count);
    printf("[Results] Tuple    Timestamp    user-id     event-type    category    priority    cpu\n");
    for (int i=0; i<10; i++) {
        printf("[Results] %-8d %-8ld %-8d %-6d %-6d %-6d %-6.5f\n", i, output->t, output->_4, output->_5, output->_6, output->_7, output->_8);
        output += 1;
    }
}