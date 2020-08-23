#include "dispatcher.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static void create_task(dispatcher_p p, int bid);

static void * dispatcher(void * args) {
	dispatcher_p p = (dispatcher_p) args;

	/* Unblocks the thread waiting for this thread to start */
	p->start = 1;

    int b=0;
    while (1) {
        usleep(DISPATCHER_INTERVAL);
        create_task(p, b);
        b = (b+1) % p->input_num;
    }

	return (args) ? NULL : args;
}

dispatcher_p dispatcher_init(scheduler_p scheduler, query_p query, batch_p * ready_buffers, int buffer_num) {

	dispatcher_p p = (dispatcher_p) malloc (sizeof(dispatcher_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->scheduler = scheduler;

    p->query = query;

    p->inputs = ready_buffers;
    p->input_num = buffer_num;

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

static void create_task(dispatcher_p p, int bid) {
    task_p new_task = task(p->query, p->inputs[bid]);

    /* Execute */
    scheduler_add_task(p->scheduler, new_task);
}

pthread_t dispatcher_get_thread(dispatcher_p p) {
    return p->thr;
}