#include "application.h"

#include "tuple.h"
#include "libgpu/gpu_agg.h"

application_p application(
    int pipeline_depth,
    query_p query,
    u_int8_t ** buffers, int buffer_size, int buffer_num,
    u_int8_t * result) {

    application_p p = (application_p) malloc(sizeof(application_t));

    p->query = query;

    p->buffers = buffers;
    p->buffer_size = buffer_size;
    p->buffer_num = buffer_num;

    /* TODO fix the tuple size */
    /* Used as an output stream */
    p->output = batch(6 * query->batch_size, 0, result, 6 * query->batch_size, TUPLE_SIZE);

    /* Start GPU and compile the query program*/
    gpu_init(query->operator_num, pipeline_depth, NULL);
    query_setup(query);

    /* Start scheduler */
    p->scheduler = scheduler_init(pipeline_depth);

    /* Start throughput monitoring */
    p->manager = event_manager_init(query->operator_num);

    /* Create tasks and add them to the task queue */
    for (int i=0; i<query->operator_num; i++) {
        p->dispatchers[i] = dispatcher_init(p->scheduler, query, i, p->manager);
        if (i>0) {
            dispatcher_set_downstream(p->dispatchers[i-1], p->dispatchers[i]);
        }
    }
    dispatcher_set_output_stream(p->dispatchers[query->operator_num-1], p->output);

    /* Start the actual monitoring */
    monitor_init(p->manager, p->scheduler, query->operator_num, p->dispatchers);

    return p;
}

void application_run(application_p p,
    int workload) {

    if (workload == 1) {
        int b=0;
        while (1) {
            usleep(DISPATCHER_INSERT_TIMEOUT);

            dispatcher_insert(p->dispatchers[0], p->buffers[b], p->buffer_size, event_get_mtime());

            b = (b+1) % p->buffer_num;
        }
    } else {
        int b=0;
        for (int i=0; i<workload; i++) {
            usleep(DISPATCHER_INSERT_TIMEOUT);

            dispatcher_insert(p->dispatchers[0], p->buffers[b], p->buffer_size, event_get_mtime());

            b = (b+1) % p->buffer_num;
        }

        while (p->scheduler->queue_size != 0) {
            sched_yield();
        }

        exit(1);
    }
}

void application_free(application_p p) {
    /* Wait for worker threads */
    pthread_join(scheduler_get_thread(), NULL);

    free(p->output);

    gpu_free();

    free(p);
}