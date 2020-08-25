#ifndef __DISPATCHER_H_
#define __DISPATCHER_H_

#include <pthread.h>

#include "task.h"
#include "scheduler/scheduler.h"
#include "result_handler.h"

#define DISPATCHER_CONCURRENT_TASK 25
#define DISPATCHER_QUEUE_LIMIT 25
#define DISPATCHER_INSERT_TIMEOUT 100 // ms

typedef struct dispatcher * dispatcher_p;
typedef struct dispatcher {
    pthread_t thr;
    pthread_mutex_t * mutex; // For p->size
    pthread_mutex_t * mutex_t; // For p->cur_tasks
    pthread_cond_t * took;
    pthread_cond_t * added;
    pthread_cond_t * finished;
    volatile unsigned start;

    scheduler_p scheduler;
    query_p query;
    int operator_id;

    volatile int cur_tasks;

    u_int8_t ** buffers;
    int buffer_num;

    volatile int size;
    volatile int task_head;
    volatile int task_tail;
    volatile task_p tasks [DISPATCHER_QUEUE_LIMIT];

    result_handler_p handler;

    event_manager_p manager;

} dispatcher_t;

dispatcher_p dispatcher_init(scheduler_p scheduler, query_p query, int oid, event_manager_p event_manager);

void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len, long upstream_time);

result_handler_p dispatcher_get_handler(dispatcher_p p);

void dispatcher_set_downstream(dispatcher_p p, dispatcher_p downstream);

void dispatcher_set_output_stream(dispatcher_p p, batch_p output_stream);

void dispatcher_close_one_task(dispatcher_p p);

#endif