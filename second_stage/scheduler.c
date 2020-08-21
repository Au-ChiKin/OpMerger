#include "scheduler.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>

static pthread_t thr = NULL;

static void process_one_task (scheduler_p m);

static void * scheduler(void * args) {
	scheduler_p p = (scheduler_p) args;

	/* Unblocks the thread (which runs scheduler_init) waiting for this thread to start */
	p->start = 1;

    while (1) {
        pthread_mutex_lock (p->mutex);
            while (p->queue_tail - p->queue_head == 0) {
                pthread_cond_wait(p->added, p->mutex);
            }

			process_one_task(p);
        pthread_mutex_unlock (p->mutex);
    }

	return (args) ? NULL : args;
}

scheduler_p scheduler_init(int pipeline_depth, event_manager_p event_manager) {

	scheduler_p p = (scheduler_p) malloc (sizeof(scheduler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

    p->queue_head = 0;
    p->queue_tail = 0;
	for (int i=0; i<SCHEDULER_QUEUE_LIMIT; i++) {
		p->queue[i] = NULL;
	}

	for (int i=0; i<SCHEDULER_MAX_PIPELINE_DEPTH; i++) {
		p->pipeline[i] = NULL;
	}
    p->pipeline_depth = pipeline_depth;

	p->manager = event_manager;

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

void scheduler_add_task (scheduler_p p, task_p t) {
	pthread_mutex_lock (p->mutex);
    	p->queue[p->queue_tail] = t;
        p->queue_tail = (p->queue_tail + 1) % SCHEDULER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);
}

static void scheduler_add_task_nolock (scheduler_p p, task_p t) {
    p->queue[p->queue_tail] = t;
    p->queue_tail = (p->queue_tail + 1) % SCHEDULER_QUEUE_LIMIT;
}

static task_p scheduler_collect_task(scheduler_p p, task_p task) {
	task_p ret = p->pipeline[0];
	for (int i = 0; i < p->pipeline_depth - 1; ++i) {
		p->pipeline[i] = p->pipeline[i + 1];
	}
	p->pipeline[p->pipeline_depth - 1] = task;

	return ret;
}

static void process_one_task (scheduler_p p) {
    /* Run the head task */
    task_p t = p->queue[p->queue_head];

	task_run(t);
    p->queue_head = (p->queue_head + 1) % SCHEDULER_QUEUE_LIMIT;

    /* Handle task popping out from the pipeline */
	task_p processed = scheduler_collect_task(p, t);
	
    if (processed != NULL) {
        if (task_has_downstream(processed)) {
            task_p downstream = task_downstream(processed->query, processed->oid+1, processed->output);

            scheduler_add_task_nolock(p, downstream);
        } else {
			/* TODO Move them to result handler */
			query_event_p event = processed->event;

			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC_RAW, &end);
			event->end = end.tv_sec * 1000000 + end.tv_nsec / 1000;

			event_manager_add_event(p->manager, event);

            /* TODO: Just delete for now */
            free(processed->output);
            free(processed);
            // result_handler_add_task(processed);
        }
    }
}
