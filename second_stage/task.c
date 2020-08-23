#include "task.h"

#include <limits.h>
#include <time.h>
#include <stdio.h>

#define MAX_ID INT_MAX
static int free_id = 0;

bool task_is_most_upstream(task_p t);

task_p task(query_p query, int oid, batch_p batch, void * dispatcher) {
    task_p task = (task_p) malloc(sizeof(task_t));

    task->id = free_id++ % MAX_ID;

    task->query = query;
    task->oid = oid;
    task->dispatcher = dispatcher;

    task->batch = batch;

    task->output = NULL;

    return task;
}

task_p task_downstream(query_p query, int oid, batch_p batch, void * dispatcher) {
    task_p ret = task(query, oid, batch, dispatcher);

    return ret;
}

void task_run(task_p t) {

    query_p query = t->query;
    int tuple_size = 64;

    if (task_is_most_upstream(t)) {
        /* Log start time and create the event */
        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC_RAW, &start);

        query_event_p event = (query_event_p) malloc(sizeof(query_event_t));
        {
            event->query_id = query->id;
            event->batch_id = ++query->batch_count;

            event->start = start.tv_sec * 1000000 + start.tv_nsec / 1000;
            event->tuples = query->batch_size;
            /* TODO need to somehow pass the tuple size from query to monitor */
            event->tuple_size = tuple_size;
        }

        t->event = event;
    }

    u_int8_t * buffer = (u_int8_t *) malloc(2 * query->batch_size * tuple_size);
    t->output = batch(2 * query->batch_size, 0, buffer, 2 * query->batch_size, tuple_size);

    query_process(t->query, t->oid, t->batch, t->output);
}

bool task_is_most_upstream(task_p t) {
    return t->oid == 0;
}

bool task_has_downstream(task_p t) {
    return t->oid < t->query->operator_num - 1 ;
}

task_p task_transfer_output(task_p from) {
    if (!task_has_downstream(from)) {
        fprintf(stderr, "error: output transfer only applys to from an upstream operator to a down stream one\n");
        exit(1);
    }

    query_p query = from->query;

    /* Get output batch ready for being an input */
    query_process_output(query, from->oid, from->output);

    /* Pass output batch ownership */
    task_p ret = task(query, from->oid+1, from->output, from->dispatcher);
    from->output = NULL;

    ret->event = from->event;

    return ret;
}

void task_free(task_p t) {
    /* For the most upstream task, input is directly from the buffer */
    if (!task_is_most_upstream(t)) {
        batch_free_all(t->batch);
    }
    batch_free_all(t->output);
    free(t);
}