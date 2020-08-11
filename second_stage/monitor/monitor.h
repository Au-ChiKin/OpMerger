#ifndef __QUERY_EVENT_H_
#define __QUERY_EVENT_H_

#include <pthread.h>

#include "event_manager.h"

#define THROUGHPUT_MONITOR_INTERVAL 1 // second

typedef struct monitor * monitor_p;
typedef struct monitor {
    volatile unsigned start;

    event_manager_p manager;
} monitor_t;

monitor_p monitor_init(event_manager_p manager);

#endif