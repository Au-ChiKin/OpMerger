#include "scheduler.h"

#include <stdlib.h>
#include <stdio.h>

static pthread_t thr = NULL;

static void process_one_task (scheduler_p m);
static void reset_data(scheduler_p p);

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

scheduler_p scheduler_init() {

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
        if (p->queue[p->queue_tail] != NULL) {
            free(p->queue[p->queue_tail]);
            p->queue[p->queue_tail] = NULL;
        }
    	p->queue[p->queue_tail] = t;
        p->queue_tail = (p->queue_tail + 1) % SCHEDULER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);
}

static void process_one_task (scheduler_p p) {
    task_p t = p->queue[p->queue_head];

	task_run(t, (void *) p, scheduler_shift_output);

    p->queue_head = (p->queue_head + 1) % SCHEDULER_QUEUE_LIMIT;
}

batch_p scheduler_shift_output(void * scheduler, batch_p batch) {
	scheduler_p p = (scheduler_p) scheduler;

	batch_p ret = p->outputs[0];
	for (int i = 0; i < p->pipeline_num - 1; ++i) {
		p->outputs[i] = p->outputs[i + 1];
	}
	p->outputs[p->pipeline_num - 1] = batch;

	return ret;
}