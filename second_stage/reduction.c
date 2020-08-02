#include "reduction.h"

#include <stdio.h>

#include "helpers.h"
#include "gpu_agg.h"
#include "config.h"

#define KERNEL_NUM 4

static size_t threads[KERNEL_NUM];
static size_t threads_per_group [KERNEL_NUM];

void reduction_init(int batch_size) {
    for (int i=0; i<KERNEL_NUM; i++) {
        threads[i] = batch_size;

        if (batch_size < MAX_THREADS_PER_GROUP) {
            threads_per_group[i] = batch_size;
        } else {
            threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
}

void reduction_setup(int batch_size, int tuple_size) {
    char * source = read_source("reduce.cl");
    int qid = gpu_get_query(source, 4, 1, 5);
    
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    int window_pointers_size = 4 * PARTIAL_WINDOWS;
    gpu_set_output(qid, 0, window_pointers_size, 0, 1, 0, 0, 1);
    gpu_set_output(qid, 1, window_pointers_size, 0, 1, 0, 0, 1);
    
    int offset_size = 16; /* The size of two longs */
    gpu_set_output(qid, 2, offset_size, 0, 1, 0, 0, 1);
    
    int window_counts_size = 20; /* 4 integers, +1 that is the mark */
    // windowCounts = new UnboundedQueryBuffer (-1, windowCountsSize, false);
    gpu_set_output(qid, 3, window_counts_size, 0, 0, 1, 0, 1);
    
    int outputSize = batch_size * tuple_size; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    gpu_set_output(qid, 4, outputSize, 1, 0, 0, 1, 0);
    
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

void reduction_process(batch_p batch, int tuple_size, int qid, batch_p output) {
    
    /* Set input */
    
    // IQueryBuffer inputBuffer = batch.getBuffer();
    // int start = batch.getBufferStartPointer();
    // int end   = batch.getBufferEndPointer();
    
    /* For setting start and end pointers in memory manager, no need for us */
    // java_gpu_setInputBuffer(qid, /*bid*/ 0, inputBuffer, start, end);
    
    long args2[2];
    {
        /* Previous pane id, based on stream start pointer, s */
        args2[0] = -1;
        long s = batch->start;
        int  t = tuple_size;
        long p = batch->pane_size;
        if (batch->start > 0) {
            // TODO: support range based windows 
            // if (batch.getWindowDefinition().isRangeBased()) {
            //     int offset = start - t;
            //     args2[0] = batch.getTimestamp(offset) / p;
            // } else {
                args2[0] = ((s / (long) t) / p) - 1;
            // }
        }
    
        args2[1] = s;
    }
    
    u_int8_t * inputs [1] = {batch->buffer + batch->start};

    u_int8_t * outputs [2] = {output->buffer + batch->start, output->buffer + batch->start + 20};

    /* TODO: support pipelined operators */
    /* Set output for a previously executed operator */
    
    // WindowBatch pipelinedBatch = gpu_shiftUp(batch);
    
    // IOperatorCode pipelinedOperator = null; 
    // if (pipelinedBatch != null) {
    //     pipelinedOperator = pipelinedBatch.getQuery().getMostUpstreamOperator().getGpuCode();
    //     pipelinedOperator.configureOutput (qid);
    // }
    
    /* Execute */
    gpu_execute_reduce(
        qid, 
        threads, threads_per_group, 
        args2, 
        (void **) (inputs), (void **) (outputs), sizeof(u_int8_t)); 
        // passing the batch without deserialisation
    
    /* TODO: output result */
    // if (pipelinedBatch != null)
    //     pipelinedOperator.processOutput (qid, pipelinedBatch);
    
    // api.outputWindowBatchResult (pipelinedBatch);
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
    printf("[Results] Tuple    Timestamp    Sum     Count\n");
    for (int i=0; i<10; i++) {
        output += 1;
        printf("[Results] %-8d %-12ld %-6.5f %d\n", i, output->t, output->_1, output->_2);
    }
}
