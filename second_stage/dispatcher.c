#include "dispatcher.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static void create_task(dispatcher_p p, batch_p batch);
static batch_p assemble(dispatcher_p p, batch_p batch, int length);
void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len);

static void * dispatcher(void * args) {
	dispatcher_p p = (dispatcher_p) args;

	/* Unblocks the thread waiting for this thread to start */
	p->start = 1;

    int b=0;
    while (1) {
        usleep(DISPATCHER_INTERVAL);

        if (p->buffers) {
            dispatcher_insert(p, p->buffers[b], p->query->batch_size);
            b = (b+1) % p->buffer_num;
        } else {
            // batch_p new_batch = assemble();

            // create_task(p, new_batch);
        }
    }

	return (args) ? NULL : args;
}

dispatcher_p dispatcher_init(scheduler_p scheduler, query_p query, int oid, u_int8_t * ready_buffers [], int buffer_num) {

	dispatcher_p p = (dispatcher_p) malloc (sizeof(dispatcher_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->scheduler = scheduler;

    p->query = query;
    p->operator_id = oid;

    p->buffers = ready_buffers;
    p->buffer_num = buffer_num;

	/* Initialise thread */
	if (pthread_create(&p->thr, NULL, dispatcher, (void *) p)) {
		fprintf(stderr, "error: failed to create dispatcher thread for query %d\n", p->query->id);
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;    
}

static void create_task(dispatcher_p p, batch_p batch) {
    task_p new_task = task(p->query, p->operator_id, batch);

    /* Execute */
    scheduler_add_task(p->scheduler, new_task);
}

void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len) {
    batch_p new_batch = batch(p->query->batch_size, 0, data, p->query->batch_size, 64);

    assemble(p, new_batch, len);
}

static batch_p assemble(dispatcher_p p, batch_p batch, int length) {
    
    p->accumulated += (length);
        
    while (p->accumulated >= p->nextBatchEndPointer) {
        
        long f = p->nextBatchEndPointer & p->mask;
        f = (f == 0) ? p->buffer_capacity : f;
        f--;
        /* Launch task */
        create_task (p, batch);
        
        p->thisBatchStartPointer += p->query->batch_size;
        p->nextBatchEndPointer   += p->query->batch_size;
    }    
}

pthread_t dispatcher_get_thread(dispatcher_p p) {
    return p->thr;
}