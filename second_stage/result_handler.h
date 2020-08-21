#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include <pthread.h>

#include "task.h"

#define RESULT_HANDLER_MAX_PIPELINE_DEPTH 4
#define RESULT_HANDLER_QUEUE_LIMIT 100

typedef struct result_handler * result_handler_p;
typedef struct result_handler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int queue_head;
    volatile int queue_tail;
    volatile task_p queue [RESULT_HANDLER_QUEUE_LIMIT];

    int const pipeline_num;
    volatile int cur_output;
    volatile batch_p outputs [RESULT_HANDLER_MAX_PIPELINE_DEPTH];

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} result_handler_t;

result_handler_p result_handler_init();

void result_handler_add_task (result_handler_p p, task_p t);

batch_p result_handler_shift_output(result_handler_p p, batch_p batch);

#endif