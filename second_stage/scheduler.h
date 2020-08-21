#ifndef __SCHEDULER_H_
#define __SCHEDULER_H_

#include <pthread.h>

#define SCHEDULER_QUEUE_LIMIT 100


typedef struct query_event * query_event_p;
typedef struct query_event {
    int query_id;
    int batch_id;

    long start;
    long end;
    int tuples;
    int tuple_size;
} query_event_t;

typedef struct scheduler * scheduler_p;
typedef struct scheduler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int event_head;
    volatile int event_tail;
    volatile query_event_p events [SCHEDULER_QUEUE_LIMIT];

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} scheduler_t;

scheduler_p scheduler_init();

void scheduler_add_event (scheduler_p p, query_event_p e);

void scheduler_get_data (scheduler_p p, int * event_num, long * processed_data, long * latency_sum);

#endif