#include "task.h"

#include <limits.h>
#include <time.h>
#include <stdio.h>

#define MAX_ID INT_MAX
static int free_id = 0;

bool task_is_most_upstream(task_p t);

task_p task(query_p query, int oid, batch_p batch, void * dispatcher, event_manager_p manager) {
    task_p task = (task_p) malloc(sizeof(task_t));

    task->id = free_id++ % MAX_ID;

    task->query = query;
    task->oid = oid;
    task->dispatcher = dispatcher;

    task->batch = batch;

    task->output = NULL;

    task->manager = manager;
    task->event = (query_event_p) malloc(sizeof(query_event_t));
    {
        task->event->query_id = query->id;
        task->event->operator_id = task->oid;

        task->event->tuples = query->batch_size;

        int tuple_size = 64;
        task->event->tuple_size = tuple_size;
    }
    event_set_insert(task->event, batch->timestamp);
    event_set_create(task->event, event_get_mtime());

    return task;
}

void task_run(task_p t) {

    query_p query = t->query;
    int tuple_size = 64;

    /* Log start time and create the event */
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC_RAW, &start);

    // query_event_p event = (query_event_p) malloc(sizeof(query_event_t));
    query_event_p event = t->event;
    {
        event->query_id = query->id;
        event->operator_id = t->oid;

        event->start = start.tv_sec * 1000000 + start.tv_nsec / 1000;
        event->tuples = query->batch_size;
        event->tuple_size = tuple_size;
    }

    t->event = event;

    u_int8_t * buffer = (u_int8_t *) malloc(1.1 * query->batch_size * tuple_size);
    t->output = batch(1.1 * query->batch_size, 0, buffer, 1.1 * query->batch_size, tuple_size);

    query_process(t->query, t->oid, t->batch, t->output);
}

void task_end(task_p t) {
    event_set_end(t->event, event_get_mtime());
    event_manager_add_event(t->manager, t->event);
    t->event = NULL;
}

bool task_is_most_upstream(task_p t) {
    return t->oid == 0;
}

bool task_has_downstream(task_p t) {
    return t->oid < t->query->operator_num - 1 ;
}

void task_process_output(task_p t) {
    /* Get output batch ready for being an input */
    query_process_output(t->query, t->oid, t->output);
}

void task_free(task_p t) {
    /* For the most upstream task, input is directly from the buffer */
    if (!task_is_most_upstream(t)) {
        batch_free_all(t->batch);
    } else {
        batch_free(t->batch);
    }
    batch_free_all(t->output);
    free(t);
}