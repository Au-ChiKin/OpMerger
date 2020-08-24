#include "result_handler.h"

#include <sched.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "dispatcher.h"

static task_p take_one_task(result_handler_p p);
static void process_one_task (result_handler_p p, task_p t);
static void reset_buffer(result_handler_p p);
static u_int8_t * fill_buffer(result_handler_p p, batch_p data);

static void * result_handler(void * args) {
	result_handler_p p = (result_handler_p) args;

	/* Unblocks the thread (which runs result_handler_init) waiting for this thread to start */
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

		process_one_task(p, t);
    }

	return (args) ? NULL : args;
}

result_handler_p result_handler_init(event_manager_p event_manager, int batch_size) {

	result_handler_p p = (result_handler_p) malloc (sizeof(result_handler_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->start = 0;

	p->mutex = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex, NULL);

	p->took = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->took, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

    p->task_head = 0;
    p->task_tail = 0;
	p->size = 0;
    for (int i=0; i<RESULT_HANDLER_QUEUE_LIMIT; i++) {
        p->tasks[i] = NULL;
    }

	p->batch_size = batch_size;
	reset_buffer(p);

	p->manager = event_manager;

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
	pthread_mutex_lock(p->mutex);
		while (p->size == RESULT_HANDLER_QUEUE_LIMIT) {
			pthread_cond_wait(p->took, p->mutex);
		}
		p->size++;

		p->tasks[p->task_tail] = t;
		p->task_tail = (p->task_tail + 1) % RESULT_HANDLER_QUEUE_LIMIT;
	pthread_mutex_unlock(p->mutex);

	pthread_cond_signal(p->added);
}

static void reset_buffer(result_handler_p p) {
	p->downstream_buffer = (u_int8_t *) malloc(p->batch_size * 64 * sizeof(u_int8_t));
	p->accumulated = 0;
}

int min(int x, int y) {
	return (x > y) ? y : x;
}

static u_int8_t * fill_buffer(result_handler_p p, batch_p data) {
	u_int8_t * ret = NULL;

	int tuple_size = data->tuple_size;

	int data_size_b = data->size * tuple_size;
	int accumulated_b = p->accumulated * tuple_size;
	int buffer_size_b = p->batch_size * tuple_size;
	
	int spare_b = buffer_size_b - accumulated_b;
	
	int to_copy = min(data_size_b, spare_b);

	int remain = data_size_b - to_copy;

	/* Materialisation */
	memcpy(p->downstream_buffer + accumulated_b, data->buffer + data->start, to_copy);

	if (remain >= 0) {
		ret = p->downstream_buffer;

		reset_buffer(p);
		p->accumulated = remain / tuple_size;

		memcpy(p->downstream_buffer, data->buffer + data->start, remain);
	} else {
		p->accumulated += data->size;
	}

	return ret;
}

static task_p take_one_task(result_handler_p p) {
    task_p t = p->tasks[p->task_head];
	p->tasks[p->task_head] = NULL;

	return t;
}

static void process_one_task (result_handler_p p, task_p t) {

	/* Handle Outputs */
	task_process_output(t);
	if (task_has_downstream(t)) {
		u_int8_t * data = fill_buffer(p, t->output);

		/* Count */
		{
			query_event_p event = t->event;
			t->event = NULL;

			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC_RAW, &end);
			event->end = end.tv_sec * 1000000 + end.tv_nsec / 1000;

			event_manager_add_event(p->manager, event);
		}

		if (data) {
			dispatcher_insert((dispatcher_p) p->downstream, data, p->batch_size);
		}
	} else {
		/* Count */{			
			query_event_p event = t->event;
			t->event = NULL;

			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC_RAW, &end);
			event->end = end.tv_sec * 1000000 + end.tv_nsec / 1000;

			event_manager_add_event(p->manager, event);
		}


	}

	task_free(t);
    p->task_head = (p->task_head + 1) % RESULT_HANDLER_QUEUE_LIMIT;
}

static void handle_windows(result_handler_p p) {


}