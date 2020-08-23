#include "dispatcher.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static void create_task(dispatcher_p p, batch_p batch);
static void assemble(dispatcher_p p, batch_p batch, int length);
static void send_one_task(dispatcher_p p);

static void * dispatcher(void * args) {
	dispatcher_p p = (dispatcher_p) args;

	/* Unblocks the thread (which runs result_handler_init) waiting for this thread to attach to the JVM */
	p->start = 1;

    while (1) {
        while (p->tasks[p->task_head] == NULL) {
            pthread_yield_np();
        }

        send_one_task(p);
    }

	return (args) ? NULL : args;
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

    // p->buffer_capacity = query->batch_size - 1;
    // p->accumulated = 0;
    // p->thisBatchStartPointer = 0;
    // p->nextBatchEndPointer = query->batch_size * 64;

    p->handler = result_handler_init(event_manager, query->batch_size);

    p->start = 0;

    p->task_head = 0;
    p->task_tail = 0;
    for (int i=0; i<DISPATCHER_QUEUE_LIMIT; i++) {
        p->tasks[i] = NULL;
    }

	/* Initialise mutex and conditions */
	p->mutex_t = (pthread_mutex_t *) malloc (sizeof(pthread_mutex_t));
	pthread_mutex_init (p->mutex_t, NULL);

	p->added = (pthread_cond_t *) malloc (sizeof(pthread_cond_t));
	pthread_cond_init (p->added, NULL);

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

static void send_one_task(dispatcher_p p) {
    task_p t = p->tasks[p->task_head];
    p->tasks[p->task_head] = NULL;

    /* Execute */
    scheduler_add_task(p->scheduler, t);

    p->task_head = (p->task_head + 1) % DISPATCHER_QUEUE_LIMIT;
}

static void create_task(dispatcher_p p, batch_p batch) {
    task_p new_task = task(p->query, p->operator_id, batch, (void *)p);

    p->tasks[p->task_tail] = new_task;
    p->task_tail = (p->task_tail + 1) % DISPATCHER_QUEUE_LIMIT;
}

void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len) {
    batch_p new_batch = batch(p->query->batch_size, 0, data, p->query->batch_size, 64);

    assemble(p, new_batch, len);
}

static void assemble(dispatcher_p p, batch_p batch, int length) {
    
    // p->accumulated += (length);
        
    // while (p->accumulated >= p->nextBatchEndPointer) {
        
    //     long f = p->nextBatchEndPointer & p->mask;
    //     f = (f == 0) ? p->buffer_capacity : f;
    //     f--;

        /* Launch task */
        create_task (p, batch);
        
    //     p->thisBatchStartPointer += p->query->batch_size;
    //     p->nextBatchEndPointer   += p->query->batch_size;
    // }    
}

void dispatcher_set_downstream(dispatcher_p p, dispatcher_p downstream) {
	p->handler->downstream = (void *) downstream;
}

result_handler_p dispatcher_get_handler(dispatcher_p p) {
	return p->handler;
}