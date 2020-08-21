#ifndef __TASK_H_
#define __TASK_H_

#include "query.h"
#include "batch.h"

typedef struct task * task_p;
typedef struct task {
    int id;

    query_p query;
    batch_p batch;

    /* TODO: delete this */
    batch_p output;
} task_t;

task_p task(query_p query, batch_p batch, batch_p output);

void task_run(task_p t, 
    void * scheduler, 
    batch_p (* callback_shift_bathc) (void * scheduler, batch_p batch));

#endif