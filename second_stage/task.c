#include "task.h"

#include <limits.h>

#define MAX_ID INT_MAX
static int free_id = 0;

task_p task(query_p query, batch_p batch, batch_p output) {
    task_p task = (task_p) malloc(sizeof(task_t));

    task->id = free_id++ % MAX_ID;

    task->query = query;
    task->batch = batch;

    /* TODO: delete this */
    task->output = output;

    return task;
}