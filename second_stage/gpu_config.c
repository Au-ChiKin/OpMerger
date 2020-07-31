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

#define FILE_TAG "[gpu_config]"
#define error_print(format, ...) fprintf(stderr, FILE_TAG format, __VA_ARGS__)

/*
 * Create an opencl context
 * 
 * @return
 * Transfer the ownership of gpu configuration to the caller
 * 
 * @post
 * Each configuration would would two command queues
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

    /* Construct a gpu configuration */
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

	/* Create two command queues */
	int error;
	config->command_queue[0] = clCreateCommandQueue (
		config->context, 
		config->device, 
		CL_QUEUE_PROFILING_ENABLE, 
		&error);
	if (! config->command_queue[0]) {
		error_print("opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}
	config->command_queue[1] = clCreateCommandQueue (
		config->context, 
		config->device, 
		CL_QUEUE_PROFILING_ENABLE, 
		&error);
	if (! config->command_queue[1]) {
		error_print("opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}

	config->scheduled  = 0; /* No read or write events scheduled */
	config->readCount  = 0;
	config->writeCount = 0;

	return config;
}

void gpu_config_setInput (gpu_config_p q, int ndx, int size) {

	q->kernelInput.inputs[ndx] = getInputBuffer (q->context, q->command_queue[0], size);
}

void gpu_config_free (gpu_config_p config) {

	int i;
	if (config) { // TODO Remove comment
		/* Free input(s) */
		// for (i = 0; i < query->kernelInput.count; i++)
			// freeInputBuffer (query->kernelInput.inputs[i], query->command_queue[0]); 
		/* Free output(s) */
		// for (i = 0; i < query->kernelOutput.count; i++)
			// freeOutputBuffer (query->kernelOutput.outputs[i], query->command_queue[0]);
		/* Free kernel(s) */
		// for (i = 0; i < config->kernel.count; i++) {
		// 	clReleaseKernel (config->kernel.kernels[i]->kernel[0]);
		// 	clReleaseKernel (config->kernel.kernels[i]->kernel[1]);
		// 	free (config->kernel.kernels[i]);
		// }
		/* Release command queues */
		if (config->command_queue[0])
			clReleaseCommandQueue(config->command_queue[0]);
		if (config->command_queue[1])
			clReleaseCommandQueue(config->command_queue[1]);
		/* Free object */
		free(config);
	}
}

void gpu_config_setOutput (gpu_config_p q, int ndx, int size,

	int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {

	q->kernelOutput.outputs[ndx] =
			getOutputBuffer (q->context, q->command_queue[0], size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
}

// void gpu_config_setKernel (gpu_config_p q,
// 	int ndx,
// 	const char *name,
// 	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
// 	int *args1, long *args2) {

// 	int i;
// 	int error = 0;
// 	q->kernel.kernels[ndx] = (aKernelP) malloc (sizeof(a_kernel_t));
// 	for (i = 0; i < 2; i++) {
// 		q->kernel.kernels[ndx]->kernel[i] = clCreateKernel (q->program, name, &error);
// 		if (! q->kernel.kernels[ndx]->kernel[i]) {
// 			fprintf(stderr, "opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
// 			exit (1);
// 		} else {
// 			(*callback) (q->kernel.kernels[ndx]->kernel[i], q, args1, args2);
// 		}
// 	}
// 	return;
// }

// void gpu_config_configureKernel (gpu_config_p q,
// 	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
// 	int *args1, long *args2) {

// 	int i;
// 	for (i = 0; i < q->kernel.count; i++)
// 		(*callback) (q->kernel.kernels[i]->kernel[0], q, args1, args2);
// 	return;
// }