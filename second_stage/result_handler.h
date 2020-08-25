#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include <pthread.h>
#include <semaphore.h>

#include "task.h"

#define RESULT_HANDLER_QUEUE_LIMIT 10000

typedef struct result_handler * result_handler_p;
typedef struct result_handler {
    pthread_t thr;
    pthread_mutex_t * mutex; // For p->size
    pthread_cond_t * took;
    pthread_cond_t * added;
    volatile unsigned start;

    int batch_size;
    query_p query;
    int operator_id;

    volatile int size;
    volatile int task_head;
    volatile int task_tail;
    volatile task_p tasks [RESULT_HANDLER_QUEUE_LIMIT];

    int accumulated;
    u_int8_t * downstream_buffer;
    long buffer_timestamp;

    task_p previous;

    volatile void * downstream;
    volatile batch_p output_stream;

    event_manager_p manager;
} result_handler_t;

result_handler_p result_handler_init(event_manager_p event_manager, query_p query, int operator_id);

void result_handler_add_task (result_handler_p p, task_p t);

#endif