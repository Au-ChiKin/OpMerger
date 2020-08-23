#ifndef __DISPATCHER_H_
#define __DISPATCHER_H_

#include <pthread.h>

#include "task.h"
#include "scheduler/scheduler.h"

#define DISPATCHER_INTERVAL 100 // us

typedef struct dispatcher * dispatcher_p;
typedef struct dispatcher {
    volatile unsigned start;

    scheduler_p scheduler;
    query_p query;
    int operator_id;

    batch_p ** buffers;
    int buffer_num;

    long buffer_capacity;
    long mask;
    long accumulated;
    long thisBatchStartPointer;
    long nextBatchEndPointer;

    pthread_t thr;
} dispatcher_t;

dispatcher_p dispatcher_init(scheduler_p scheduler, query_p query, int oid, u_int8_t * ready_buffers [], int buffer_num);

pthread_t dispatcher_get_thread(dispatcher_p p);

#endif