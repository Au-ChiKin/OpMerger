#include "result_handler.h"

#include <stdlib.h>
#include <stdio.h>

static pthread_t thr = NULL;

static void process_one_task (result_handler_p m);
static void reset_data(result_handler_p p);

static void * result_handler(void * args) {
	result_handler_p p = (result_handler_p) args;

	/* Unblocks the thread (which runs result_handler_init) waiting for this thread to start */
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

result_handler_p result_handler_init() {

	result_handler_p p = (result_handler_p) malloc (sizeof(result_handler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

    p->queue_head = 0;
    p->queue_tail = 0;
	for (int i=0; i<RESULT_HANDLER_QUEUE_LIMIT; i++) {
		p->queue[i] = NULL;
	}

	p->cur_output = 1;
	for (int i=0; i<RESULT_HANDLER_QUEUE_LIMIT; i++) {
		p->outputs[i] = NULL;
	}

	/* Initialise mutex and conditions */
	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

	/* Initialise thread */
	if (pthread_create(&thr, NULL, result_handler, (void *) p)) {
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
        if (p->queue[p->queue_tail] != NULL) {
            free(p->queue[p->queue_tail]);
            p->queue[p->queue_tail] = NULL;
        }
    	p->queue[p->queue_tail] = t;
        p->queue_tail = (p->queue_tail + 1) % RESULT_HANDLER_QUEUE_LIMIT;
    pthread_mutex_unlock (p->mutex);
	
    pthread_cond_signal (p->added);

}

batch_p result_handler_shift_output(result_handler_p p, batch_p batch) {
	pthread_mutex_lock (p->mutex);
		batch_p ret = p->outputs[0];
		for (int i = 0; i < p->pipeline_num - 1; ++i) {
			p->outputs[i] = p->outputs[i + 1];
		}
		p->outputs[p->pipeline_num - 1] = batch;
    pthread_mutex_unlock (p->mutex);

	return ret;
}

static void process_one_task (result_handler_p p) {
    task_p t = p->queue[p->queue_head];

    query_process(t->query, t->batch);

    p->queue_head = (p->queue_head + 1) % RESULT_HANDLER_QUEUE_LIMIT;
}

