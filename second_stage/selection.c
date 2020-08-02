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
        threads[i] = batch_size;

        if (batch_size < MAX_THREADS_PER_GROUP) {
            threads_per_group[i] = batch_size;
        } else {
            threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
}

void selection_setup(int batch_size, int tuple_size) {
    char * source = read_source("select.cl");
    int qid = gpu_get_query(source, 2, 1, 4);
    
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    gpu_set_output (qid, 0, 4 * batch_size,              0, 1, 1, 0, 1); /*      Flags */
    gpu_set_output (qid, 1, 4 * batch_size,              0, 1, 0, 0, 1); /*    Offsets */
    gpu_set_output (qid, 2, 4 * MAX_THREADS_PER_GROUP,   0, 1, 0, 0, 1); /* Partitions */
    gpu_set_output (qid, 3, batch_size * tuple_size,     1, 0, 0, 1, 0); /*    Results */
    
    int args[3];
    args[0] = batch_size * tuple_size;
    args[1] = batch_size;
    args[2] = 4 * threads_per_group[0] * TUPLES_PER_THREADS;

    gpu_set_kernel_reduce (qid, args);
}