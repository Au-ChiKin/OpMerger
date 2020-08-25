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
			// while (p->tasks[p->task_head] == NULL) {
				pthread_cond_wait(p->added, p->mutex);
				// sched_yield();
			}
			
			task_p t = take_one_task(p);

			p->size--;
		pthread_mutex_unlock(p->mutex);
		pthread_cond_signal(p->took);

		process_one_task(p, t);
    }

	return (args) ? NULL : args;
}

result_handler_p result_handler_init(event_manager_p event_manager, query_p query, int operator_id) {

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

	p->query = query;
	p->batch_size = query->batch_size;
	p->operator_id = operator_id;

    p->task_head = 0;
    p->task_tail = 0;
	p->size = 0;
    for (int i=0; i<RESULT_HANDLER_QUEUE_LIMIT; i++) {
        p->tasks[i] = NULL;
    }

	reset_buffer(p);

	p->previous = NULL;

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
		// while (p->size == RESULT_HANDLER_QUEUE_LIMIT) {
		if (p->size == RESULT_HANDLER_QUEUE_LIMIT) {
			struct timespec time_to_wait;
    		time_to_wait.tv_sec = 0;
			time_to_wait.tv_nsec = 100 * 1000;

			pthread_cond_timedwait(p->took, p->mutex, &time_to_wait);
			// pthread_cond_wait(p->took, p->mutex);
		}
		if (p->size == RESULT_HANDLER_QUEUE_LIMIT) {
			printf("Result hanlder of operator %d queue has exceeded\n", p->operator_id);
			fflush(stdout);
		}
		p->size++;

		if (p->tasks[p->task_tail]) {
			task_free(p->tasks[p->task_tail]);
		}

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

	p->task_head = (p->task_head + 1) % RESULT_HANDLER_QUEUE_LIMIT;
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
		
		task_free(t);
	} else {

		/* Assuming only tumbling windows*/
		if (p->previous) {
			long current_offset = 0;

			if (t->output->closing_windows) {
				/* Output opening windows of the previous with the closing windows */
			}

			if (t->output->pending_windows) {
				/* Take the remained opending windows of the previous and merge it with
				   the pending windows to produce new opending windows */
			}

			if (t->output->complete_windows) {
				/* Output */
				u_int8_t * buffer_start = t->output->buffer + t->output->start;
				int tuple_size = t->output->tuple_size;
				int complete_windows = t->output->complete_windows;

				memcpy(p->output_stream->buffer, buffer_start + current_offset, complete_windows * tuple_size);
			}

			if (!t->output->closing_windows && !t->output->pending_windows 
				&& p->previous->output->opening_windows) {
				
				u_int8_t * buffer_start = p->previous->output->buffer + p->previous->output->start;
				int opening_windows = p->previous->output->opening_windows;
				int tuple_size = p->previous->output->tuple_size;
				long offset = (p->previous->output->closing_windows + p->previous->output->pending_windows)
					* tuple_size;

				memcpy(p->output_stream->buffer, buffer_start + offset, opening_windows * tuple_size);
			}

			/* Count */
			{			
				query_event_p event = p->previous->event;
				p->previous->event = NULL;

				struct timespec end;
				clock_gettime(CLOCK_MONOTONIC_RAW, &end);
				event->end = end.tv_sec * 1000000 + end.tv_nsec / 1000;

				event_manager_add_event(p->manager, event);
			}

			task_free(p->previous);
			p->previous = t;
		} else {
			p->previous = t;
		}
	}
}
