#include "event_manager.h"

#include <stdlib.h>
#include <stdio.h>

static pthread_t thr = NULL;

static void process_one_event (event_manager_p m);
static void reset_data(event_manager_p p);

static void * event_manager(void * args) {
	event_manager_p p = (event_manager_p) args;

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

event_manager_p event_manager_init(int operator_num) {

	event_manager_p p = (event_manager_p) malloc (sizeof(event_manager_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

    p->operator_num = operator_num;

    p->event_head = 0;
    p->event_tail = 0;
    for (int i=0; i<EVENT_MANAGER_QUEUE_LIMIT; i++) {
        p->events[i] = NULL;
    }

    /* Accumulated data */
    reset_data(p);

	/* Initialise mutex and conditions */
	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

	/* Initialise thread */
	if (pthread_create(&thr, NULL, event_manager, (void *) p)) {
		fprintf(stderr, "error: failed to create throughput monitor thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;
}

void event_manager_add_event (event_manager_p p, query_event_p e) {
	pthread_mutex_lock (p->mutex);
        if (p->events[p->event_tail] != NULL) {
            free(p->events[p->event_tail]);
            p->events[p->event_tail] = NULL;
        }
    	p->events[p->event_tail] = e;
        p->event_tail = (p->event_tail + 1) % EVENT_MANAGER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);
}

void event_manager_get_data (event_manager_p p, int * num, int * event_num, long * processed_data, long * latency_sum) {
    pthread_mutex_lock (p->mutex);
        *num = p->operator_num;

        for (int i=0; i<p->operator_num; i++) {
            event_num[i] = p->event_num[i];
            processed_data[i] = p->processed_data[i];
            latency_sum[i] = p->latency_sum[i];
        }

        reset_data(p);
    pthread_mutex_unlock (p->mutex);
}

static void process_one_event (event_manager_p p) {
    query_event_p e = p->events[p->event_head];

    p->event_num[e->operator_id] += 1;
    p->processed_data[e->operator_id] += e->tuples * e->tuple_size;
    p->latency_sum[e->operator_id] += e->end - e->start;

    p->event_head = (p->event_head + 1) % EVENT_MANAGER_QUEUE_LIMIT;
}

static void reset_data(event_manager_p p) {
    for (int i=0; i<p->operator_num; i++) {
        p->event_num[i] = 0;
        p->latency_sum[i] = 0;
        p->processed_data[i] = 0;
    }
}

