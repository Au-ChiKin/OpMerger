#include "event_manager.h"

#include <stdlib.h>
#include <stdio.h>

static pthread_t thr = NULL;

static query_event_p take_one_event(event_manager_p p);
static void process_one_event (event_manager_p p, query_event_p e);
static void reset_data(event_manager_p p);

void event_set_insert(query_event_p event, long time) {
    event->insert = time;
}

void event_set_create(query_event_p event, long time) {
    event->create = time;
}

void event_set_start(query_event_p event, long time) {
    event->start = time;
}

void event_set_end(query_event_p event, long time) {
    event->end = time;
}

long event_get_mtime() {
    static struct timespec now;
    static long mtime;

    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    mtime = now.tv_sec * 1000000 + now.tv_nsec / 1000;

    return mtime;
}

static query_event_p take_one_event(event_manager_p p) {
    query_event_p e = p->events[p->event_head];
    p->events[p->event_head] = NULL;

    p->event_head = (p->event_head + 1) % EVENT_MANAGER_QUEUE_LIMIT;

    return e;
}

static void * event_manager(void * args) {
	event_manager_p p = (event_manager_p) args;

	/* Unblocks the thread (which runs event_manager_init) waiting for this thread to start */
	p->start = 1;

    while (1) {
        pthread_mutex_lock (p->mutex);
            while (p->event_tail - p->event_head == 0) {
                pthread_cond_wait(p->added, p->mutex);
            }
            
        query_event_p e = take_one_event(p);

        pthread_mutex_unlock (p->mutex);

        process_one_event(p, e);
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

void event_manager_get_data (event_manager_p p, 
    int * num, int * event_num, long * processed_data, long * latency_sum) {
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

static void process_one_event (event_manager_p p, query_event_p e) {
    p->event_num[e->operator_id] += 1;
    p->processed_data[e->operator_id] += e->tuples * e->tuple_size;
    p->latency_sum[e->operator_id] += e->end - e->insert;

    free(e);
}

static void reset_data(event_manager_p p) {
    for (int i=0; i<p->operator_num; i++) {
        p->event_num[i] = 0;
        p->latency_sum[i] = 0;
        p->processed_data[i] = 0;
    }
}

