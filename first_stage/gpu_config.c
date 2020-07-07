#include "gpu_config.h"

#include "../clib/openclerrorcode.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#define FILE_TAG "[gpu_context]"
#define error_print(format, ...) fprintf(stderr, FILE_TAG format, __VA_ARGS__)

/*
 * Create an opencl context
 * 
 * @post
 * Each context would would two command queues
 */ 
gpu_config_p gpu_config (
    int query_id, 
    cl_device_id device, 
    cl_context context, 
    cl_program program,
	int _kernels, 
    int _inputs, 
    int _outputs
) {

    /* Construct a gpu context */
	gpu_config_p config = (gpu_config_p) malloc (sizeof(gpu_config_t));
	if (! config) {
		error_print("fatal error: out of memory\n", NULL);
		exit(1);
	}

	config->query_id = query_id;

	config->device = device;
	config->context = context;
	config->program = program;

	config->kernel.count = _kernels;
	config->kernelInput.count = _inputs;
	config->kernelOutput.count = _outputs;

	/* Create command queues */
	int error;
	config->queue[0] = clCreateCommandQueue (
		config->context, 
		config->device, 
		CL_QUEUE_PROFILING_ENABLE, 
		&error);
	if (! config->queue[0]) {
		error_print("opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}
	config->queue[1] = clCreateCommandQueue (
		config->context, 
		config->device, 
		CL_QUEUE_PROFILING_ENABLE, 
		&error);
	if (! config->queue[1]) {
		error_print("opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}

	config->scheduled  = 0; /* No read or write events scheduled */
	config->readCount  = 0;
	config->writeCount = 0;

	return config;
}

// void gpu_context_writeInput (gpuContextP q,
// 	void (*callback)(gpuContextP, JNIEnv *, jobject, int, int),
// 	JNIEnv *env, jobject obj, int qid) {
// void gpu_context_writeInput (gpuContextP q,
// 	void (*callback)(gpuContextP, JNIEnv *, jobject, int, int),
// 	JNIEnv *env, jobject obj, int qid) {

// 	int idx;
// 	for (idx = 0; idx < q->kernelInput.count; idx++)
// 		(*callback) (q, env, obj, qid, idx);
// 	return;
// }