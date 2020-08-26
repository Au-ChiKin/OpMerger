#ifndef __QUERY_EVENT_H_
#define __QUERY_EVENT_H_

#include <pthread.h>

#include "event_manager.h"
#include "../dispatcher.h"
#include "scheduler/scheduler.h"

#define THROUGHPUT_MONITOR_INTERVAL 1.0 // second

typedef struct monitor * monitor_p;
typedef struct monitor {
    volatile unsigned start;

    event_manager_p manager;

    int pipeline_num;
    dispatcher_p * dispatchers;

    scheduler_p scheduler;
} monitor_t;

monitor_p monitor_init(event_manager_p manager, scheduler_p scheduler, int pipeline_num, dispatcher_p dispatchers[]);

#endif