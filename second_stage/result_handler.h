#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include <pthread.h>

#include "task.h"

#define RESULT_HANDLER_QUEUE_LIMIT 100

typedef struct result_handler * result_handler_p;
typedef struct result_handler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int task_head;
    volatile int task_tail;
    volatile task_p tasks [RESULT_HANDLER_QUEUE_LIMIT];

    event_manager_p manager;

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} result_handler_t;

result_handler_p result_handler_init(event_manager_p event_manager);

void result_handler_add_task (result_handler_p p, task_p t);

#endif