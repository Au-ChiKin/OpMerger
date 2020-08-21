#include "task.h"

task_p task(query_p query, batch_p batch, batch_p output) {
    task_p task = (task_p) malloc(sizeof(task_t));

    task->query = query;
    task->batch = batch;

    /* TODO: delete this */
    task->output = output;

    return task;
}