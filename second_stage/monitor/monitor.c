#include "monitor.h"

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sched.h>

static pthread_t thr = NULL;

static void print_data(monitor_p p);

static void * monitor(void * args) {
	monitor_p p = (monitor_p) args;

#ifndef __APPLE__
	/* Pin this thread to a particular core: 0 is the dispatcher, 1 is the GPU */
	int core = 3;
	cpu_set_t set;
	CPU_ZERO (&set);
	CPU_SET (core, &set);
	sched_setaffinity (0, sizeof(set), &set);
	fprintf(stdout, "[DBG] result handler attached to core 2\n");
	fflush (stdout);
#endif

	/* Unblocks the thread waiting for this thread to start */
	p->start = 1;

    while (1) {
        usleep(THROUGHPUT_MONITOR_INTERVAL * 1000 * 1000);
        print_data(p);
    }

	return (args) ? NULL : args;
}

monitor_p monitor_init(event_manager_p manager, scheduler_p scheduler, int pipeline_num, dispatcher_p dispatchers[]) {

	monitor_p p = (monitor_p) malloc (sizeof(monitor_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->manager = manager;

	p->pipeline_num = pipeline_num;
	p->dispatchers = dispatchers;

	p->scheduler = scheduler;

	/* Initialise thread */
	if (pthread_create(&thr, NULL, monitor, (void *) p)) {
		fprintf(stderr, "error: failed to create throughput monitor thread\n");
		exit (1);
	}
	/* Wait until thread attaches itself to the JVM */
	while (! p->start)
		;
	return p;    
}

static void print_data(monitor_p p) {
    int event_num[2], operators;
    long processed_data[2], latency_sum[2];
	float avg_throughput = 0;
    
    event_manager_get_data(p->manager, &operators, event_num, processed_data, latency_sum);

	printf("[MONITOR] ");
	for (int i=0; i<operators; i++) {
		float throughput = ((float) processed_data[i] / 1024.0 / 1024.0) / THROUGHPUT_MONITOR_INTERVAL; /* MB/s */
		float latency_avg = latency_sum[i] / (float) event_num[i]; /* us */
		avg_throughput += throughput;

		printf("(%d) t: %9.3f MB/s  l: %9.3f us   ", i, throughput, latency_avg);
	}
	// printf("(avg) t: %9.3f MB/s", avg_throughput / operators);

	printf("sch queue: %d   ", p->scheduler->queue_size);

	for (int i=0; i<p->pipeline_num; i++) {
		printf("d%d queue: %d   r%d queue: %d   ", 
			i, p->dispatchers[i]->size, i, dispatcher_get_handler(p->dispatchers[i])->size);
	}
	printf("\n");
}
