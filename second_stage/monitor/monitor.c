#include "monitor.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static pthread_t thr = NULL;

static void print_data(monitor_p p);

static void * monitor(void * args) {
	monitor_p p = (monitor_p) args;

	/* Unblocks the thread waiting for this thread to start */
	p->start = 1;

    while (1) {
        usleep(THROUGHPUT_MONITOR_INTERVAL * 1000 * 1000);
        print_data(p);
    }

	return (args) ? NULL : args;
}

monitor_p monitor_init(event_manager_p manager) {

	monitor_p p = (monitor_p) malloc (sizeof(monitor_t));
	if (! p) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

    p->manager = manager;

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
    int event_num;
    long processed_data, latency_sum;
    
    event_manager_get_data(p->manager, &event_num, &processed_data, &latency_sum);

    if (event_num == 0) {
        printf("[MONITOR] No event has been passed\n");
        return;
    }

    float throughput = ((float) processed_data / 1024.0 / 1024.0) / THROUGHPUT_MONITOR_INTERVAL; /* MB/s */
    float latency_avg = latency_sum / (float) event_num; /* us */

	// printf("processed_data %ld\n", processed_data);
	// printf("latency_sum %ld    event_num %d\n", latency_sum, event_num);

    printf("[MONITOR] t: %9.3f MB/s    l: %9.3f us\n", throughput, latency_avg);
}
