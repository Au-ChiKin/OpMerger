#include "gpu_config.h"

#include "../../clib/openclerrorcode.h"

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

void gpu_config_setOutput (gpu_config_p q, int ndx, int size,
	int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {

	q->kernelOutput.outputs[ndx] =
			getOutputBuffer (q->context, q->command_queue[0], size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
}

void gpu_config_free (gpu_config_p config) {

	int i;
	if (config) {
		/* Free input(s) */
		for (i = 0; i < config->kernelInput.count; i++)
			freeInputBuffer (config->kernelInput.inputs[i], config->command_queue[0]); 
		/* Free output(s) */
		for (i = 0; i < config->kernelOutput.count; i++)
			freeOutputBuffer (config->kernelOutput.outputs[i], config->command_queue[0]);
		/* Free kernel(s) */
		for (i = 0; i < config->kernel.count; i++) {
			clReleaseKernel (config->kernel.kernels[i]->kernel[0]);
			clReleaseKernel (config->kernel.kernels[i]->kernel[1]);
			free (config->kernel.kernels[i]);
		}
		/* Release command queues */
		if (config->command_queue[0])
			clReleaseCommandQueue(config->command_queue[0]);
		if (config->command_queue[1])
			clReleaseCommandQueue(config->command_queue[1]);
		/* Free object */
		free(config);
	}
}


void gpu_config_setKernel (gpu_config_p query,
	int ndx,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2) {

	int i;
	int error = 0;
	query->kernel.kernels[ndx] = (kernel_p) malloc (sizeof(kernel_t));
	for (i = 0; i < 2; i++) {
		query->kernel.kernels[ndx]->kernel[i] = clCreateKernel (query->program, name, &error);
		if (! query->kernel.kernels[ndx]->kernel[i]) {
			fprintf(stderr, "opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
			exit (1);
		} else {
			(*callback) (query->kernel.kernels[ndx]->kernel[i], query, args1, args2);
		}
	}
	return;
}

void gpu_config_resetKernel (gpu_config_p query,
	int ndx,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2) {

	printf("Enter resetKernel\n");

	int i;
	for (i = 0; i < 2; i++) {
		if (! query->kernel.kernels[ndx]->kernel[i]) {
			fprintf(stderr, "error: kernel %d has not been created (%s)\n", ndx, __FUNCTION__);
			exit (1);
		} else {
			(*callback) (query->kernel.kernels[ndx]->kernel[i], query, args1, args2);
		}
	}
	return;
}

void gpu_config_configureKernel (gpu_config_p q,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2) {

	int i;
	for (i = 0; i < q->kernel.count; i++)
		(*callback) (q->kernel.kernels[i]->kernel[0], q, args1, args2);
	return;
}

void gpu_config_submitKernel (gpu_config_p config, size_t *threads, size_t *threadsPerGroup) {
	int i;
	int error = 0;
	/* Execute */
	for (i = 0; i < config->kernel.count; i++) {
		dbg("[DBG] submit kernel %d: %10zu threads %10zu threads/group\n", i, threads[i], threadsPerGroup[i]);
#ifdef GPU_PROFILE
		error |= clEnqueueNDRangeKernel (
			config->command_queue[0],
			config->kernel.kernels[i]->kernel[0],
			1,
			NULL,
			&(threads[i]),
			&(threadsPerGroup[i]),
			0, NULL, &(config->exec_event[i]));
#else
		error |= clEnqueueNDRangeKernel (
			config->command_queue[0],
			config->kernel.kernels[i]->kernel[0],
			1,
			NULL,
			&(threads[i]),
			&(threadsPerGroup[i]),
			0, NULL, NULL);
#endif
		clFinish(config->command_queue[0]);
		if (error != CL_SUCCESS) {
			fprintf(stderr, "opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
			exit (1);
		}
	}
	return;
}

void gpu_config_flush (gpu_config_p config) {
	int error = 0;
	error |= clFlush (config->command_queue[0]);
	error |= clFlush (config->command_queue[1]);
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
		exit (1);
	}
}

void gpu_config_finish (gpu_config_p config) {
	if (config->scheduled < 1)
		return;
	/* There are tasks scheduled */
	int error = 0;
	error |= clFinish (config->command_queue[0]);
	error |= clFinish (config->command_queue[1]);
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s (%s), config=%d @%p\n", error, getErrorMessage(error), __FUNCTION__, config->query_id, config);
		exit (1);
	}
}

void gpu_config_moveInputBuffers (gpu_config_p config, void ** host_addr, size_t addr_size) {
	int i;
	int error = 0;
	/* Write */
	for (i = 0; i < config->kernelInput.count; i++) {
		if (i == config->kernelInput.count - 1) { // last input buffer
			error |= clEnqueueWriteBuffer (
				config->command_queue[0],
				config->kernelInput.inputs[i]->device_buffer,
				CL_FALSE,
				0,
				config->kernelInput.inputs[i]->size,
				*(host_addr + i * addr_size),
#ifdef GPU_PROFILE				
				0, NULL, &(config->write_event));
#else				
				0, NULL, NULL);
#endif
		} else {
			error |= clEnqueueWriteBuffer (
				config->command_queue[0],
				config->kernelInput.inputs[i]->device_buffer,
				CL_FALSE,
				0,
				config->kernelInput.inputs[i]->size,
				*(host_addr + i * addr_size),
				0, NULL, NULL);
		}

		if (error != CL_SUCCESS) {
			fprintf(stderr, "opencl error (%d): %s (%s)\n", error, getErrorMessage(error), __FUNCTION__);
			exit (1);
		}
	}

	config->writeCount += 1;
	config->scheduled = 1;

	return;
}

void gpu_config_moveOutputBuffers (gpu_config_p config, void ** host_addr, size_t addr_size) {
	int i;
	int error = 0;
	int moved = 0;
	/* Read */
	for (i = 0; i < config->kernelOutput.count; i++) {

		if (config->kernelOutput.outputs[i]->doNotMove && (! config->kernelOutput.outputs[i]->bearsMark))
			continue;

		if (config->kernelOutput.outputs[i]->readEvent) {
			error |= clEnqueueReadBuffer (
				config->command_queue[0],
				config->kernelOutput.outputs[i]->device_buffer,
				CL_FALSE,
				0,
				config->kernelOutput.outputs[i]->size,
				*(host_addr + moved * addr_size),
#ifdef GPU_PROFILE
				0, NULL, &(config->read_event));
#else
				0, NULL, NULL);
#endif
		} else {
			error |= clEnqueueReadBuffer (
				config->command_queue[0],
				config->kernelOutput.outputs[i]->device_buffer,
				CL_FALSE,
				0,
				config->kernelOutput.outputs[i]->size,
				*(host_addr + moved * addr_size),
				0, NULL, NULL);
		}

		moved += 1;

		dbg("[DBG] Moved: %d when i %d (%s)\n", moved, i, __FUNCTION__);

		if (error != CL_SUCCESS) {
			fprintf(stderr, "opencl error (%d): %s (%s) when move output %d\n",
				error, getErrorMessage(error), __FUNCTION__, i);
			exit (1);
		}
	}


	config->readCount += 1;
	config->scheduled = 1;

	return;
}

// void gpu_context_readOutput (gpuContextP q,
// 	void (*callback)(gpuContextP, JNIEnv *, jobject, int, int, int),
// 	JNIEnv *env, jobject obj, int qid) {

// 	int idx;
// 	/* Find mark */
// 	int mark = -1;
// 	for (idx = 0; idx < q->kernelOutput.count; idx++) {
// 		if (q->kernelOutput.outputs[idx]->bearsMark) {
// 			dbg("[DBG] output %d bears mark\n", idx);
// 			int N = q->kernelOutput.outputs[idx]->size / sizeof(int);
// 			// dbg("[DBG] %d values\n", N);
// 			int *values = (int *) (q->kernelOutput.outputs[idx]->mapped_buffer);
// 			int j;
// 			for (j = N - 1; j >= 0; j--) {
// 				// dbg("[DBG] value[%6d] = %d\n", j, values[j]);
// 				if (values[j] != 0) {
// 					mark = values[j];
// 					break;
// 				}
// 			}
// 			dbg("[DBG] mark is %d\n", mark);
// 			break;
// 		}
// 	}
// 	// mark = -1;
// 	// fprintf(stdout, "[DBG] mark is %10d\n", mark);
// 	// fflush(stdout);

// 	for (idx = 0; idx < q->kernelOutput.count; idx++)
// 		(*callback) (q, env, obj, qid, idx, mark);

// 	return;
// }


