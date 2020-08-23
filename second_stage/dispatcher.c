#include "dispatcher.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static pthread_t thr = NULL;

static void print_data(dispatcher_p p);

static void * dispatcher(void * args) {
	dispatcher_p p = (dispatcher_p) args;

	/* Unblocks the thread waiting for this thread to start */
	p->start = 1;

    while (1) {
        usleep(DISPATCHER_INTERVAL * 1000 * 1000);
        print_data(p);
    }

	return (args) ? NULL : args;
}

dispatcher_p dispatcher_init(event_manager_p manager) {

	dispatcher_p p = (dispatcher_p) malloc (sizeof(dispatcher_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->manager = manager;

	/* Initialise thread */
	if (pthread_create(&thr, NULL, dispatcher, (void *) p)) {
		fprintf(stderr, "error: failed to create throughput dispatcher thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;    
}

static void print_data(dispatcher_p p) {
    int event_num;
    long processed_data, latency_sum;
    
    event_manager_get_data(p->manager, &event_num, &processed_data, &latency_sum);

    if (event_num == 0) {
        printf("[DISPATCHER] No event has been passed\n");
        return;
    }

    float throughput = ((float) processed_data / 1024.0 / 1024.0) / DISPATCHER_INTERVAL; /* MB/s */
    float latency_avg = latency_sum / (float) event_num; /* us */

	// printf("processed_data %ld\n", processed_data);
	// printf("latency_sum %ld    event_num %d\n", latency_sum, event_num);

    printf("[DISPATCHER] t: %9.3f MB/s    l: %9.3f us\n", throughput, latency_avg);
}
