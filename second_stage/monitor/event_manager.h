#ifndef __EVENT_MANAGER_H_
#define __EVENT_MANAGER_H_

#include <pthread.h>

#define EVENT_MANAGER_QUEUE_LIMIT 1000
#define EVENT_MANAGER_OPERATOR_LIMIT 2

typedef struct query_event * query_event_p;
typedef struct query_event {
    int query_id;
    int operator_id;

    long insert;
    long create;
    long start;
    long end;
    int tuples;
    int tuple_size;
} query_event_t;

long event_get_mtime();

void event_set_insert(query_event_p event, long time);

void event_set_create(query_event_p event, long time);

void event_set_start(query_event_p event, long time);

void event_set_end(query_event_p event, long time);

typedef struct event_manager * event_manager_p;
typedef struct event_manager {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    int operator_num;

    volatile unsigned start;

    volatile int event_head;
    volatile int event_tail;
    volatile query_event_p events [EVENT_MANAGER_QUEUE_LIMIT];

    /* Accumulated data */
    volatile int event_num[EVENT_MANAGER_OPERATOR_LIMIT];
    volatile long processed_data[EVENT_MANAGER_OPERATOR_LIMIT];
    volatile long latency_sum[EVENT_MANAGER_OPERATOR_LIMIT];
} event_manager_t;

event_manager_p event_manager_init(int operator_num);

void event_manager_add_event (event_manager_p p, query_event_p e);

void event_manager_get_data (event_manager_p p, 
    int * num, int * event_num, long * processed_data, long * latency_sum);

#endif