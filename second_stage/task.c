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
    int batch_size = t->query->batch_size;
    int tuple_size = t->batch->tuple_size;

    /* Log start time*/
    event_set_start(t->event, event_get_mtime());

    u_int8_t * buffer = (u_int8_t *) malloc(1.1 * batch_size * tuple_size);
    t->output = batch(1.1 * batch_size, 0, buffer, 1.1 * batch_size, tuple_size);

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