#ifndef __SCHEDULER_H_
#define __SCHEDULER_H_

#include <pthread.h>

#include "task.h"

#define SCHEDULER_QUEUE_LIMIT 100

typedef struct scheduler * scheduler_p;
typedef struct scheduler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int queue_head;
    volatile int queue_tail;
    volatile task_p queue [SCHEDULER_QUEUE_LIMIT];

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} scheduler_t;

scheduler_p scheduler_init();

void scheduler_add_task (scheduler_p p, task_p t);

#endif