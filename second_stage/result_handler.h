#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include "task.h"

typedef struct result_handler * result_handler_p;
typedef struct result_handler {

    /* Accumulated data */
    volatile int event_num;
    volatile long processed_data;
    volatile long latency_sum;
} result_handler_t;

result_handler_p result_handler();

void result_handler_add_task (result_handler_p p, task_p t);

#endif