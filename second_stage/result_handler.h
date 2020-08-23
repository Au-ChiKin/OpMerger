#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include <pthread.h>

#include "task.h"

#define RESULT_HANDLER_QUEUE_LIMIT 1000

typedef struct result_handler * result_handler_p;
typedef struct result_handler {
    volatile unsigned start;

    volatile int task_head;
    volatile int task_tail;
    volatile task_p tasks [RESULT_HANDLER_QUEUE_LIMIT];

    u_int8_t * buffer;
    int batch_size;
    int accumulated;

    event_manager_p manager;

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;

    volatile void * downstream;

    pthread_t thr;
} result_handler_t;

result_handler_p result_handler_init(event_manager_p event_manager, int batch_size);

void result_handler_add_task (result_handler_p p, task_p t);

#endif