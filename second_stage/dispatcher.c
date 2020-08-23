#include "dispatcher.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static void create_task(dispatcher_p p, batch_p batch);
static void assemble(dispatcher_p p, batch_p batch, int length);

dispatcher_p dispatcher(scheduler_p scheduler, query_p query, int oid, event_manager_p event_manager) {

	dispatcher_p p = (dispatcher_p) malloc (sizeof(dispatcher_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->scheduler = scheduler;

    p->query = query;
    p->operator_id = oid;

    p->accumulated = 0;
    p->thisBatchStartPointer = 0;
    p->nextBatchEndPointer = query->batch_size * 64;

    p->buffer_capacity = query->batch_size - 1;

    p->handler = result_handler_init(event_manager);

    return p;  
}

void dispatcher_insert(dispatcher_p p, u_int8_t * data, int len) {
    batch_p new_batch = batch(p->query->batch_size, 0, data, p->query->batch_size, 64);

    assemble(p, new_batch, len);
}

static void create_task(dispatcher_p p, batch_p batch) {
    task_p new_task = task(p->query, p->operator_id, batch, (void *)p);

    /* Execute */
    scheduler_add_task(p->scheduler, new_task);
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