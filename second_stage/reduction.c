#include "reduction.h"

#include "helpers.h"
#include "gpu_agg.h"

#define KERNEL_NUM 4
#define MAX_THREADS_PER_GROUP 256

static size_t threads[KERNEL_NUM];
static size_t threads_per_group [KERNEL_NUM];

void reduction_init(int tuple_size) {
    for (int i=0; i<KERNEL_NUM; i++) {
        threads[i] = tuple_size;
        threads_per_group[i] = MAX_THREADS_PER_GROUP;
    }
}

void reduction_setup(int batch_size, int tuple_size) {
    char * source = read_source("reduce.cl");
    int qid = gpu_get_query(source, 4, 1, 5);
    
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    int window_pointers_size = 4 * 1024; /* SystemConf.PARTIAL_WINDOWS */
    gpu_set_output(qid, 0, window_pointers_size, 0, 1, 0, 0, 1);
    gpu_set_output(qid, 1, window_pointers_size, 0, 1, 0, 0, 1);
    
    int offset_size = 16; /* The size of two longs */
    gpu_set_output(qid, 2, offset_size, 0, 1, 0, 0, 1);
    
    int window_counts_size = 20; /* 4 integers, +1 that is the mark */
    // windowCounts = new UnboundedQueryBuffer (-1, windowCountsSize, false);
    gpu_set_output(qid, 3, window_counts_size, 0, 0, 1, 0, 1);
    
    int outputSize = 1024 * 1024; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    gpu_set_output(0, 4, outputSize, 1, 0, 0, 1, 0);
    
    int args1 [4];
    args1[0] = batch_size;
    args1[1] = batch_size * tuple_size;
    args1[2] = window_pointers_size;
    args1[3] = 1024 * 1024; /* SystemConf.HASH_TABLE_SIZE */

    long args2 [2];
    args2[0] = 0; /* Previous pane id   */
    args2[1] = 0; /* Batch start offset */

    gpu_set_kernel_reduce(0, args1, args2);
}

void reduction_process(batch_p batch, int tuple_size, int qid) {
    
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
    
    /* TODO: support pipelined operators */
    /* Set output for a previously executed operator */
    
    // WindowBatch pipelinedBatch = gpu_shiftUp(batch);
    
    // IOperatorCode pipelinedOperator = null; 
    // if (pipelinedBatch != null) {
    //     pipelinedOperator = pipelinedBatch.getQuery().getMostUpstreamOperator().getGpuCode();
    //     pipelinedOperator.configureOutput (qid);
    // }
    
    /* Execute */
    gpu_execute_reduce(qid, 
        threads, threads_per_group, 
        args2, 
        (void **) (batch->buffer + batch->start), sizeof(u_int8_t)); // passing the batch without deserialisation
    
    /* TODO: output result */
    // if (pipelinedBatch != null)
    //     pipelinedOperator.processOutput (qid, pipelinedBatch);
    
    // api.outputWindowBatchResult (pipelinedBatch);
}
