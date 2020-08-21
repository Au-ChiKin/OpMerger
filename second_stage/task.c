#include "task.h"

#include <limits.h>
#include <time.h>

#define MAX_ID INT_MAX
static int free_id = 0;

bool task_is_most_upstream(task_p t);

task_p task(query_p query, batch_p batch) {
    task_p task = (task_p) malloc(sizeof(task_t));

    task->id = free_id++ % MAX_ID;

    task->query = query;
    task->oid = 0;

    task->batch = batch;

    task->output = NULL;

    return task;
}

task_p task_downstream(query_p query, int oid, batch_p batch) {
    task_p ret = task(query, batch);

    ret->oid = oid;

    return ret;
}

void task_run(task_p t) {

    query_p query = t->query;
    int tuple_size = 64;

    if (!task_is_most_upstream(t)) {
        query_process_output(t->query, t->oid-1, t->batch);
    } else {
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

void task_free(task_p t) {
    if (!task_is_most_upstream(t)) {
        batch_free_all(t->batch);
    }
    batch_free_all(t->output);
    free(t);
}