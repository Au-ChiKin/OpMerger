#include "task.h"

#include <limits.h>

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

    if (!task_is_most_upstream(t)) {
        query_process_output(t->query, t->oid-1, t->batch);
    }

    u_int8_t * buffer = (u_int8_t *) malloc(6 * query->batch_size * 64);
    t->output = batch(6 * query->batch_size, 0, buffer, 6 * query->batch_size, 64);

    query_process(t->query, t->oid, t->batch, t->output);
}

bool task_is_most_upstream(task_p t) {
    return t->oid == 0;
}

bool task_has_downstream(task_p t) {
    return t->oid < t->query->operator_num - 1 ;
}