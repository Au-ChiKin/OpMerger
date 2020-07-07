#include "gpu_context.h"

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
gpu_context_p gpu_context (
    int qid, 
    cl_device_id device, 
    cl_context context, 
    cl_program program,
	int _kernels, 
    int _inputs, 
    int _outputs
) {

	gpu_context_p q = (gpu_context_p) malloc (sizeof(gpu_context_t));
	if (! q) {
		error_print("fatal error: out of memory\n", NULL);
		exit(1);
	}

	q->qid = qid;

	q->device = device;
	q->context = context;
	q->program = program;

	q->kernel.count = _kernels;
	q->kernelInput.count = _inputs;
	q->kernelOutput.count = _outputs;

	/* Create command queues */
	int error;
	q->queue[0] = clCreateCommandQueue (
		q->context, 
		q->device, 
		CL_QUEUE_PROFILING_ENABLE, 
		&error);
	if (! q->queue[0]) {
		error_print("opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}
	q->queue[1] = clCreateCommandQueue (
		q->context, 
		q->device, 
		CL_QUEUE_PROFILING_ENABLE, 
		&error);
	if (! q->queue[1]) {
		error_print("opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}

	q->scheduled  = 0; /* No read or write events scheduled */
	q->readCount  = 0;
	q->writeCount = 0;

	return q;
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