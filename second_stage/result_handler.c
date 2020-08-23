#include "result_handler.h"

#include <stdlib.h>
#include <stdio.h>

static void process_one_task (result_handler_p m);
static void reset_data(result_handler_p p);

static void * result_handler(void * args) {
	result_handler_p p = (result_handler_p) args;

	/* Unblocks the thread (which runs result_handler_init) waiting for this thread to attach to the JVM */
	p->start = 1;

    while (1) {
        pthread_mutex_lock (p->mutex);
            while (p->task_tail - p->task_head == 0) {
                pthread_cond_wait(p->added, p->mutex);
            }

                process_one_task(p);
        pthread_mutex_unlock (p->mutex);
    }

	return (args) ? NULL : args;
}

result_handler_p result_handler_init(event_manager_p event_manager) {

	result_handler_p p = (result_handler_p) malloc (sizeof(result_handler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

    p->task_head = 0;
    p->task_tail = 0;
    for (int i=0; i<RESULT_HANDLER_QUEUE_LIMIT; i++) {
        p->tasks[i] = NULL;
    }

	p->manager = event_manager;

    /* Accumulated data */
    reset_data(p);

	/* Initialise mutex and conditions */
	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

	/* Initialise thread */
	if (pthread_create(&p->thr, NULL, result_handler, (void *) p)) {
		fprintf(stderr, "error: failed to create throughput monitor thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;
}

void result_handler_add_task (result_handler_p p, task_p t) {
	pthread_mutex_lock (p->mutex);
    	p->tasks[p->task_tail] = t;
        p->task_tail = (p->task_tail + 1) % RESULT_HANDLER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);
}

static void process_one_task (result_handler_p p) {
    task_p t = p->tasks[p->task_head];

	if (task_has_downstream(t)) {


		task_p downstream = task_transfer_output(t);

		// scheduler_add_task(p, downstream);
	} else {
		query_event_p event = t->event;

		struct timespec end;
		clock_gettime(CLOCK_MONOTONIC_RAW, &end);
		event->end = end.tv_sec * 1000000 + end.tv_nsec / 1000;

		event_manager_add_event(p->manager, event);

		/* TODO: Just delete for now */
		task_free(t);
	}

    p->task_head = (p->task_head + 1) % RESULT_HANDLER_QUEUE_LIMIT;
}

static void reset_data(result_handler_p p) {
    p->event_num = 0;
    p->latency_sum = 0;
    p->processed_data = 0;
}

