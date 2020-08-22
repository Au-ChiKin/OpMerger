#ifndef __TASK_H_
#define __TASK_H_

#include "stdbool.h"

#include "monitor/event_manager.h"
#include "query.h"
#include "batch.h"

typedef struct task * task_p;
typedef struct task {
    int id;

    query_p query;
    int oid;

    batch_p batch;

    batch_p output;

    query_event_p event;
} task_t;

task_p task(query_p query, batch_p batch);
task_p task_downstream(query_p query, int oid, batch_p batch);

void task_run(task_p t);

bool task_has_downstream(task_p t);

task_p task_transfer_output(task_p from);

void task_free(task_p t);
#endif