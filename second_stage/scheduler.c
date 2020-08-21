#include "scheduler.h"

#include <stdlib.h>
#include <stdio.h>

static pthread_t thr = NULL;

static void process_one_event (scheduler_p m);
static void reset_data(scheduler_p p);

static void * scheduler(void * args) {
	scheduler_p p = (scheduler_p) args;

	/* Unblocks the thread (which runs result_handler_init) waiting for this thread to attach to the JVM */
	p->start = 1;

    while (1) {
        pthread_mutex_lock (p->mutex);
            while (p->event_tail - p->event_head == 0) {
                pthread_cond_wait(p->added, p->mutex);
            }

                process_one_event(p);
        pthread_mutex_unlock (p->mutex);
    }

	return (args) ? NULL : args;
}

scheduler_p scheduler_init() {

	scheduler_p p = (scheduler_p) malloc (sizeof(scheduler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

    p->event_head = 0;
    p->event_tail = 0;

    /* Accumulated data */
    reset_data(p);

	/* Initialise mutex and conditions */
	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

	/* Initialise thread */
	if (pthread_create(&thr, NULL, scheduler, (void *) p)) {
		fprintf(stderr, "error: failed to create throughput monitor thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;
}

void scheduler_add_event (scheduler_p p, query_event_p e) {
	pthread_mutex_lock (p->mutex);
        // if (p->events[p->event_tail] != NULL) {
        //     free(p->events[p->event_tail]);
        //     p->events[p->event_tail] = NULL;
        // }
    	p->events[p->event_tail] = e;
        p->event_tail = (p->event_tail + 1) % SCHEDULER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);
}

void scheduler_get_data (scheduler_p p, int * event_num, long * processed_data, long * latency_sum) {
    pthread_mutex_lock (p->mutex);
        *event_num = p->event_num;
        *processed_data = p->processed_data;
        *latency_sum = p->latency_sum;

        reset_data(p);
    pthread_mutex_unlock (p->mutex);
}

static void process_one_event (scheduler_p p) {
    query_event_p e = p->events[p->event_head];

    p->event_num += 1;
    p->processed_data += e->tuples * e->tuple_size;
    p->latency_sum += e->end - e->start;

    p->event_head = (p->event_head + 1) % SCHEDULER_QUEUE_LIMIT;
}

static void reset_data(scheduler_p p) {
    p->event_num = 0;
    p->latency_sum = 0;
    p->processed_data = 0;
}

