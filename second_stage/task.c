#include "task.h"

#include <limits.h>
#include <time.h>
#include <stdio.h>

#include "dispatcher.h"

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
    task->create_time = event_get_mtime();

    return task;
}

void task_run(task_p t) {

    query_p query = t->query;
    int tuple_size = 64;

    t->event = (query_event_p) malloc(sizeof(query_event_t));
    {
        t->event->query_id = query->id;
        t->event->operator_id = t->oid;

        t->event->tuples = query->batch_size;

        int tuple_size = 64;
        t->event->tuple_size = tuple_size;
    }
    event_set_insert(t->event, t->batch->timestamp);
    event_set_create(t->event, t->create_time);

    /* Log start time and create the event */
    event_set_start(t->event, event_get_mtime());

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
    if (t->output) {
        batch_free_all(t->output);
    }
    free(t);
}