#ifndef __DISPATCHER_H_
#define __DISPATCHER_H_

#include "task.h"
#include "scheduler/scheduler.h"

#define DISPATCHER_INTERVAL 100 // us

typedef struct dispatcher * dispatcher_p;
typedef struct dispatcher {
    scheduler_p scheduler;
    query_p query;
    int operator_id;

    u_int8_t ** buffers;
    int buffer_num;

    long buffer_capacity;
    long mask;
    long accumulated;
    long thisBatchStartPointer;
    long nextBatchEndPointer;
} dispatcher_t;

dispatcher_p dispatcher(scheduler_p scheduler, query_p query, int oid);

void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len);

#endif