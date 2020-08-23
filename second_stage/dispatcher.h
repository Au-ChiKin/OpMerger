#ifndef __SCHEDULER_H_
#define __SCHEDULER_H_

#include <pthread.h>

#include "event_manager.h"

#define DISPATCHER_INTERVAL 1.0 // second

typedef struct dispatcher * dispatcher_p;
typedef struct dispatcher {
    volatile unsigned start;

    event_manager_p manager;
} dispatcher_t;

dispatcher_p dispatcher_init(event_manager_p manager);

#endif