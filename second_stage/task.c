#include "task.h"

#include <limits.h>

#define MAX_ID INT_MAX
static int free_id = 0;

task_p task(query_p query, batch_p batch) {
    task_p task = (task_p) malloc(sizeof(task_t));

    task->id = free_id++ % MAX_ID;

    task->query = query;
    task->batch = batch;

    task->output = NULL;

    return task;
}

void task_run(task_p t) {

    query_p query = t->query;

    u_int8_t * buffer = (u_int8_t *) malloc(6 * query->batch_size * 64);
    t->output = batch(6 * query->batch_size * 64, 0, buffer, 6 * query->batch_size * 64, 64);

    query_process(t->query, t->batch, t->output);
}

bool task_has_downstream(task_p t) {
    return false;
}