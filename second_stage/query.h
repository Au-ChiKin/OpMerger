#ifndef QUERY_H
#define QUERY_H

#include <stdbool.h>

#include "batch.h"
#include "window.h"
#include "operators/operator.h"
#include "monitor/monitor.h"
#include "monitor/event_manager.h"

/* TODO to support more than one operator */
#define QUERY_MAX_OPERATOR_NUM 2

typedef struct query * query_p;
typedef struct query {
    int id;
    int batch_size;
    int batch_count;
    bool has_setup;

    window_p window;

    int operator_num;
    void * operators[QUERY_MAX_OPERATOR_NUM];
    operator_p callbacks[QUERY_MAX_OPERATOR_NUM];
    bool is_merging;

    /* Profiling */
    event_manager_p manager;
    monitor_p monitor;
} query_t;

query_p query(int id, int batch_size, window_p window, bool is_merging);

void query_add_operator(query_p query, void * new_operator, operator_p operator_callbacks);

void query_setup(query_p query, event_manager_p manager, monitor_p monitor);

void query_process(query_p query, batch_p input, batch_p output);

void query_free(query_p query);

void query_run(query_p query, int buffer_num, batch_p input [], int work_load, batch_p output);

#endif