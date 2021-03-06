#ifndef __SCHEDULER_H_
#define __SCHEDULER_H_

#include <pthread.h>
#include <semaphore.h>

#include "task.h"
#include "monitor/event_manager.h"

#define SCHEDULER_MAX_PIPELINE_DEPTH 4
// Warning! Should be much larger than the allowed sum of concurrent tasks of all pipelines
#define SCHEDULER_QUEUE_LIMIT 256

typedef struct scheduler * scheduler_p;
typedef struct scheduler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;
    pthread_cond_t * took;

    volatile unsigned start;

    int queue_size;
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

pthread_t scheduler_get_thread();

#endif