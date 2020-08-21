#ifndef __SCHEDULER_H_
#define __SCHEDULER_H_

#include <pthread.h>

#include "task.h"
#include "result_handler.h"

#define SCHEDULER_MAX_PIPELINE_DEPTH 4
#define SCHEDULER_QUEUE_LIMIT 10000

typedef struct scheduler * scheduler_p;
typedef struct scheduler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int queue_head;
    volatile int queue_tail;
    volatile task_p queue [SCHEDULER_QUEUE_LIMIT];

    /* A pipeline of intermediate result */
    int pipeline_depth;
    volatile int cur_output;
    volatile task_p pipeline [SCHEDULER_MAX_PIPELINE_DEPTH];

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} scheduler_t;

scheduler_p scheduler_init(int pipeline_depth);

void scheduler_add_task (scheduler_p p, task_p t);

#endif