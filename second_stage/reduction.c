#include "reduction.h"

#include "helpers.h"
#include "gpu_agg.h"

void reduction(int batch_size, int tuple_size) {
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