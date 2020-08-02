#include "aggregation.h"

#include "helpers.h"
#include "gpu_agg.h"

void aggregation(int batch_size, int tuple_size) {
    char * source = read_source("cl/aggregation_gen.cl");
    int qid = gpu_get_query(source, 9, 1, 9);

    gpu_set_input(qid, 0, batch_size * tuple_size);

    int window_pointers_size = 4 * 1024 /* SystemConf.PARTIAL_WINDOWS */;
    gpu_set_output(qid, 0, window_pointers_size, 0, 1, 0, 0, 1);
    gpu_set_output(qid, 1, window_pointers_size, 0, 1, 0, 0, 1);

    int failed_flags_size = 4 * batch_size; /* One int per tuple */
    gpu_set_output(qid, 2, failed_flags_size, 1, 1, 0, 0, 1);

    int offset_size = 16; /* The size of two longs */
    gpu_set_output(qid, 3, offset_size, 0, 1, 0, 0, 1);

    int window_counts_size = 24; /* 4 integers, +1 that is the complete windows mark, +1 that is the mark */
    // windowCounts = new UnboundedQueryBuffer (-1, windowCountsSize, false);
    gpu_set_output(qid, 4, window_counts_size, 0, 0, 1, 0, 1);

    /* Set partial window results */
    int outputSize = 1024 * 1024; /* SystemConf.UNBOUNDED_BUFFER_SIZE */
    gpu_set_output(qid, 5, outputSize, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 6, outputSize, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 7, outputSize, 1, 0, 0, 1, 1);
    gpu_set_output(qid, 8, outputSize, 1, 0, 0, 1, 1);

    int args1[6];
    args1[0] = batch_size; /* batch size in tuples*/
    args1[1] = batch_size * tuple_size; /* batch size in bytes*/
    args1[2] = outputSize;
    args1[3] = 1024 * 1024; /* SystemConf.HASH_TABLE_SIZE */
    args1[4] = 1024; /* SystemConf.PARTIAL_WINDOWS */
    args1[5] = 8; /* keyLength * numberOfThreadsPerGroup; cache size; for group by */

    long args2[2];
    args2[0] = 0; /* Previous pane id */
    args2[0] = 0; /* Start offset */
    
    gpu_set_kernel_aggregate(0, args1, args2); // TODO: remove comments after create buffers
}