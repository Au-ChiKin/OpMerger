#include "gpu_agg.h"

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif
#include <stdio.h>

#include "gpu_query.h"
#include "gpu_input_buffer.h"
#include "gpu_output_buffer.h"


static cl_platform_id platform = NULL;
static cl_device_id device = NULL;
static cl_context context = NULL;

static int QUERY_NUM;
static int freeIndex;
static gpu_query_p queries [MAX_QUERIES];

// static int D;
// static gpuContextP pipeline [MAX_DEPTH];

// static jclass class;
// static jmethodID writeMethod, readMethod;

// static resultHandlerP resultHandler = NULL;

/* Callback functions */
void callback_setKernelAggregate (cl_kernel, gpu_config_p, int *, long *);


int gpu_set_kernel (int qid, int ndx /* kernel index */,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int * args1, long * args2) {

	if (qid < 0 || qid >= QUERY_NUM) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpu_query_p p = queries[qid];
	return gpu_query_setKernel (p, ndx, name, callback, args1, args2);
}

void gpu_set_kernel_aggregate(int qid, int * args1, long * args2) {
    /**
     * TODO
     * Defence for wrong args format
     **/

	/* Set kernel(s) */
	gpu_set_kernel (qid, 0, "clearKernel",                    &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 1, "computeOffsetKernel",            &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 2, "computePointersKernel",          &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 3, "countWindowsKernel",             &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 4, "aggregateClosingWindowsKernel",  &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 5, "aggregateCompleteWindowsKernel", &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 6, "aggregateOpeningWindowsKernel",  &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 7, "aggregatePendingWindowsKernel",  &callback_setKernelAggregate, args1, args2);
	gpu_set_kernel (qid, 8, "packKernel",                     &callback_setKernelAggregate, args1, args2);
	
	return 0;
}

void callback_setKernelAggregate (cl_kernel kernel, gpu_config_p config, int *args1, long *args2) {

	int numberOfTuples = args1[0];

	int numberOfInputBytes  = args1[1];
	int numberOfOutputBytes = args1[2];

	int hashTableSize = args1[3];

	int maxNumberOfWindows = args1[4];

	int cache_size = args1[5];
	
	long previousPaneId = args2[0];
	long startOffset    = args2[1];
	
	int error = 0;
	
	error |= clSetKernelArg (kernel, 0, sizeof(int),  (void *)      &numberOfTuples);

	error |= clSetKernelArg (kernel, 1, sizeof(int),  (void *)  &numberOfInputBytes);
	error |= clSetKernelArg (kernel, 2, sizeof(int),  (void *) &numberOfOutputBytes);

	error |= clSetKernelArg (kernel, 3, sizeof(int),  (void *)       &hashTableSize);
	error |= clSetKernelArg (kernel, 4, sizeof(int),  (void *)  &maxNumberOfWindows);

	error |= clSetKernelArg (kernel, 5, sizeof(long), (void *)      &previousPaneId);
	error |= clSetKernelArg (kernel, 6, sizeof(long), (void *)         &startOffset);
	
	/* Set input buffers */
	error |= clSetKernelArg (
			kernel,
			7,
			sizeof(cl_mem),
			(void *) &(config->kernelInput.inputs[0]->device_buffer));
	
	
	/* Set output buffers */
	error |= clSetKernelArg (
			kernel,
			8,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[0]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			9,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[1]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			10,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[2]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			11,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[3]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			12,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[4]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			13,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[5]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			14,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[6]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			15,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[7]->device_buffer));
	
	error |= clSetKernelArg (
			kernel,
			16,
			sizeof(cl_mem),
			(void *) &(config->kernelOutput.outputs[8]->device_buffer));
	
	/* Set local memory */
	error |= clSetKernelArg (kernel, 17, (size_t) cache_size, (void *) NULL);
	
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	
	return;
}