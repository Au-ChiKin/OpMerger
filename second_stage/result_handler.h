#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include <pthread.h>

#define RESULT_HANDLER_QUEUE_LIMIT 100

typedef struct result_handler * result_handler_p;
typedef struct result_handler {
    pthread_mutex_t * mutex;
    pthread_cond_t * added;

    volatile unsigned start;

    volatile int event_head;
    volatile int event_tail;
    volatile query_event_p events [RESULT_HANDLER_QUEUE_LIMIT];

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} result_handler_t;

result_handler_p result_handler_init();

void result_handler_add_event (result_handler_p p, query_event_p e);

void result_handler_get_data (result_handler_p p, int * event_num, long * processed_data, long * latency_sum);

#endif