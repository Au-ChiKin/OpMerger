#ifndef APPLICATION_H
#define APPLICATION_H

#include "query.h"
#include "dispatcher/dispatcher.h"
#include "monitor/event_manager.h"
#include "scheduler/scheduler.h"

typedef struct application * application_p;
typedef struct application {
    scheduler_p scheduler;
    dispatcher_p dispatchers[2];

    query_p query;

    /* input */
    u_int8_t ** buffers;
    int buffer_size;
    int buffer_num;

    batch_p output;

    event_manager_p manager;
} application_t;

application_p application(
    int pipeline_depth,
    query_p query,
    u_int8_t ** buffers, int buffer_size, int buffer_num,
    u_int8_t * result);

void application_run(application_p p,
    int workload);

void application_free(application_p p);

#endif