#define _GNU_SOURCE

#include "scheduler.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <sched.h>

#include "dispatcher/dispatcher.h"

static pthread_t thr = NULL;

static task_p scheduler_collect_task(scheduler_p p, task_p task);
static void process_one_task (scheduler_p m, task_p t);
static task_p take_one_task(scheduler_p p);

static void * scheduler(void * args) {
	scheduler_p p = (scheduler_p) args;

#ifndef __APPLE__
	/* Pin this thread to a particular core: 0 is the dispatcher, 1 is the GPU */
	int core = 1;
	cpu_set_t set;
	CPU_ZERO (&set);
	CPU_SET (core, &set);
	sched_setaffinity (0, sizeof(set), &set);
	fprintf(stdout, "[DBG] result handler attached to core 2\n");
	fflush (stdout);
#endif

	/* Unblocks the thread (which runs scheduler_init) waiting for this thread to start */
	p->start = 1;

    while (1) {
        pthread_mutex_lock (p->mutex);
			static int warned = 0;
            while (p->queue_size == 0) {
                pthread_cond_wait(p->added, p->mutex);
            }

			task_p t = take_one_task(p);
        pthread_mutex_unlock (p->mutex);
		pthread_cond_signal(p->took);

		process_one_task(p, t);
    }

	return (args) ? NULL : args;
}

scheduler_p scheduler_init(int pipeline_depth) {

	scheduler_p p = (scheduler_p) malloc (sizeof(scheduler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

	p->queue_size = 0;
    p->queue_head = 0;
    p->queue_tail = 0;
	for (int i=0; i<SCHEDULER_QUEUE_LIMIT; i++) {
		p->queue[i] = NULL;
	}

	for (int i=0; i<SCHEDULER_MAX_PIPELINE_DEPTH; i++) {
		p->pipeline[i] = NULL;
	}
    p->pipeline_depth = pipeline_depth;

	/* Initialise mutex and conditions */
	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

	p->took = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->took, NULL);

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
		while (p->queue_size >= SCHEDULER_QUEUE_LIMIT) {
			pthread_cond_wait(p->took, p->mutex);
		}

		p->queue_size++;
    	p->queue[p->queue_tail] = t;
        p->queue_tail = (p->queue_tail + 1) % SCHEDULER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);
}

pthread_t scheduler_get_thread() {
	return thr;
}

static task_p scheduler_collect_task(scheduler_p p, task_p task) {
	task_p ret = p->pipeline[0];
	for (int i = 0; i < p->pipeline_depth - 1; ++i) {
		p->pipeline[i] = p->pipeline[i + 1];
	}
	p->pipeline[p->pipeline_depth - 1] = task;

	return ret;
}

static void process_one_task (scheduler_p p, task_p t) {
    /* Handle task popping out from the pipeline */
	task_p processed = scheduler_collect_task(p, t);

	/* Run the head task */
	task_run(t, processed);

	/* Transfer ownership of the task */
    if (processed != NULL) {
		result_handler_p handler = dispatcher_get_handler((dispatcher_p) processed->dispatcher);
		result_handler_add_task(handler, processed);
    }
}

static task_p take_one_task(scheduler_p p) {
    task_p t = p->queue[p->queue_head];
	p->queue[p->queue_head] = NULL;

	p->queue_size--;
    p->queue_head = (p->queue_head + 1) % SCHEDULER_QUEUE_LIMIT;

	return t;
}