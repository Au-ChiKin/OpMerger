#ifndef __EVENT_MANAGER_H_
#define __EVENT_MANAGER_H_

#include <pthread.h>

#define EVENT_MANAGER_QUEUE_LIMIT 100


typedef struct query_event * query_event_p;
typedef struct query_event {
    int query_id;
    int batch_id;

    long start;
    long end;
    int tuples;
    int tuple_size;
} query_event_t;

typedef struct event_manager * event_manager_p;
typedef struct event_manager {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int event_head;
    volatile int event_tail;
    volatile query_event_p events [EVENT_MANAGER_QUEUE_LIMIT];

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} event_manager_t;

event_manager_p event_manager_init();

void event_manager_add_event (event_manager_p p, query_event_p e);

void event_manager_get_data (event_manager_p p, int * event_num, long * processed_data, long * latency_sum);

#endif