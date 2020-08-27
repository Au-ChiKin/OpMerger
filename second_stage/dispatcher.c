#define _GNU_SOURCE

#include "dispatcher.h"

#include <sched.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static task_p take_one_task(dispatcher_p p);
static void create_task(dispatcher_p p, batch_p batch);
static void assemble(dispatcher_p p, batch_p batch, int length);
static void send_one_task(dispatcher_p p, task_p t);

static void * dispatcher(void * args) {
	dispatcher_p p = (dispatcher_p) args;

#ifndef __APPLE__
	/* Pin this thread to a particular core: 0 is the dispatcher, 1 is the GPU */
	int core = 0;
	cpu_set_t set;
	CPU_ZERO (&set);
	CPU_SET (core, &set);
	sched_setaffinity (0, sizeof(set), &set);
	fprintf(stdout, "[DBG] result handler attached to core 2\n");
	fflush (stdout);
#endif

	/* Unblocks the thread (which runs result_handler_init) waiting for this thread to start*/
	p->start = 1;

    while (1) {
		pthread_mutex_lock(p->mutex);

			while (p->size == 0) {
				pthread_cond_wait(p->added, p->mutex);
			}

            task_p t = take_one_task(p);
    
			p->size--;
		pthread_mutex_unlock(p->mutex);
		pthread_cond_signal(p->took);

		// if (t->query->operator_num > 1 && task_is_most_upstream(t)) {
			pthread_mutex_lock(p->mutex_t);
				while (p->cur_tasks == DISPATCHER_CONCURRENT_TASK) {
					pthread_cond_wait(p->finished, p->mutex_t);
				}

				send_one_task(p, t);

				p->cur_tasks++;
			pthread_mutex_unlock(p->mutex_t);
		// } else {
			// send_one_task(p, t);
		// }
    }

	return (args) ? NULL : args;
}

void dispatcher_close_one_task(dispatcher_p p, task_p t) {
	// if (t->query->operator_num > 1 && task_is_most_upstream(t)) {
		pthread_mutex_lock(p->mutex_t);
			p->cur_tasks--;
		pthread_mutex_unlock(p->mutex_t);
		pthread_cond_signal(p->finished);
	// } else {
	// 	return;
	// }
}

dispatcher_p dispatcher_init(scheduler_p scheduler, query_p query, int oid, event_manager_p event_manager) {

	dispatcher_p p = (dispatcher_p) malloc (sizeof(dispatcher_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->scheduler = scheduler;

    p->query = query;
    p->operator_id = oid;

    p->handler = result_handler_init(event_manager, query, oid);
	p->manager = event_manager;

	p->cur_tasks = 0;

    p->start = 0;

	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->mutex_t = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex_t, NULL);

	p->took = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->took, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

	p->finished = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->finished, NULL);

    p->task_head = 0;
    p->task_tail = 0;
    p->size = 0;
    for (int i=0; i<DISPATCHER_QUEUE_LIMIT; i++) {
        p->tasks[i] = NULL;
    }

	/* Initialise thread */
	if (pthread_create(&p->thr, NULL, dispatcher, (void *) p)) {
		fprintf(stderr, "error: failed to create throughput monitor thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;

    return p;  
}

void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len, long upstream_time) {
    batch_p new_batch = batch(p->query->batch_size, 0, data, p->query->batch_size, 64);
	batch_reset_timestamp(new_batch, upstream_time);

	pthread_mutex_lock(p->mutex);
		while (p->size == DISPATCHER_QUEUE_LIMIT) {
			pthread_cond_wait(p->took, p->mutex);
		}

		if (p->size == DISPATCHER_QUEUE_LIMIT-1) {
			// printf("Warning Dispatcher queue of operator %d has been full\n", p->operator_id);
			// fflush(stdout);
		}
		
		p->size++;

        assemble(p, new_batch, len);

	pthread_mutex_unlock(p->mutex);

	pthread_cond_signal(p->added);
}

void dispatcher_set_downstream(dispatcher_p p, dispatcher_p downstream) {
    if (query_get_operator_num(p->query) - 1 == p->operator_id) {
        printf("error: the last operator of a query does not have downstream\n");
        exit(1);
    }
	p->handler->downstream = (void *) downstream;
}

void dispatcher_set_output_stream(dispatcher_p p, batch_p output_stream) {
    if (query_get_operator_num(p->query) - 1 != p->operator_id) {
        printf("error: only the last operator of a query should be set an output batch\n");
        exit(1);
    }

	p->handler->output_stream = output_stream;
}

result_handler_p dispatcher_get_handler(dispatcher_p p) {
	return p->handler;
}

static void send_one_task(dispatcher_p p, task_p t) {
    /* Execute */
    scheduler_add_task(p->scheduler, t);

    p->task_head = (p->task_head + 1) % DISPATCHER_QUEUE_LIMIT;
}

static void create_task(dispatcher_p p, batch_p batch) {
    task_p new_task = task(p->query, p->operator_id, batch, (void *)p, p->manager);

	if (p->tasks[p->task_tail]) {
		task_free(p->tasks[p->task_tail]);
	}

    p->tasks[p->task_tail] = new_task;
    p->task_tail = (p->task_tail + 1) % DISPATCHER_QUEUE_LIMIT;
}

static void assemble(dispatcher_p p, batch_p batch, int length) {
    /* Launch task */
    create_task (p, batch);
}

static task_p take_one_task(dispatcher_p p) {
    task_p t = p->tasks[p->task_head];
    p->tasks[p->task_head] = NULL;

    return t;
}
