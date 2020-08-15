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
#include "openclerrorcode.h"
// #include "resulthandler.h"

static cl_platform_id platform = NULL;
static cl_device_id device = NULL;
static cl_context context = NULL;

static int query_num;
static int free_index;
static gpu_query_p queries [MAX_QUERIES];

// static int D;
// static gpu_config_p pipeline [MAX_DEPTH];

// static jclass class;
// static jmethodID writeMethod, readMethod;

// static resultHandlerP resultHandler = NULL;

/* Callback functions */

void callback_setKernelAggregate (cl_kernel, gpu_config_p, int *, long *);
void callback_setKernelCompact (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);
void callback_setKernelReduce (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);
void callback_setKernelSelect (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);

void callback_resetConstReduce (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);
void callback_resetConstAggregate (cl_kernel kernel, gpu_config_p config, int *args1, long *args2);

void callback_configureReduce (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);
void callback_configureAggregate (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);

void callback_readOutput (gpu_config_p context, int qid, int ndx, int mark);

gpu_config_p callback_execKernel (gpu_config_p context);


static void set_platform () {
    int error = 0;
	cl_uint count = 0;

	error = clGetPlatformIDs (
        1,           // entry number of &platform
        &platform,   // on return, the platform id
        &count);     // on return, the number of platform
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	dbg("[GPU] Obtained 1/%u platforms available\n", count);
	return;
}

static void set_device () {
	int error = 0;
	cl_uint count = 0;

	error = clGetDeviceIDs (
        platform,             // platform ID
        CL_DEVICE_TYPE_GPU,   // look only for GPUs
        1,                    // return an ID for only one GPU &device_id
        &device,              // on return, the device ID
        &count);              // on return, the number of devices
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	dbg("[GPU] Obtained 1/%u devices available\n", count);
	return;
}

static void get_deviceInfo () {

	cl_int error = 0;

	cl_uint value = 0;
	size_t max_work_group = 0;
	size_t max_work_item[3];
	char extensions [2048];
	char name [256];

	error = 0;
	error |= clGetDeviceInfo (device, CL_DEVICE_MEM_BASE_ADDR_ALIGN, sizeof (cl_uint), &value,         NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_EXTENSIONS,                      2048, &extensions[0], NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_NAME,                            2048, &name[0],       NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_MAX_WORK_GROUP_SIZE, sizeof (size_t),  &max_work_group,NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_MAX_WORK_ITEM_SIZES, 3*sizeof (size_t),&max_work_item, NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	fprintf(stdout, "[GPU] GPU name: %s\n", name);
	fprintf(stdout, "[GPU] GPU supported extensions are: %s\n", extensions);
	fprintf(stdout, "[GPU] GPU memory addresses are %u bits aligned\n", value);
	fprintf(stdout, "[GPU] GPU maximum work group size is %zu\n", max_work_group);
	fprintf(stdout, "[GPU] GPU maximum work item size for the first dimentsion is %zu\n", max_work_item[0]);

	return ;
}

static void set_context () {
	int error = 0;
	context = clCreateContext (
		0,        // properties
		1,        // the number of devices in the devices parameter
		&device,  // a pointer to the list of device IDs from clGetDeviceIDs
		NULL,     // a pointer to an error notice callback function (if any)
		NULL,     // data to pass as a param to the callback function
		&error);  // on return, points to a result code
	if (! context) {
		fprintf(stderr, "[GPU] opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return ;
}

void gpu_init (int _queries) {

	set_platform ();

	set_device ();
	get_deviceInfo ();
	
    set_context ();
    
	query_num = _queries;
	free_index = 0;
	for (int i = 0; i < MAX_QUERIES; i++)
		queries[i] = NULL;

	// #ifdef GPU_HANDLER
	// /* Create result handler */
	// resultHandler = result_handler_init (env);
	// #else
	// resultHandler = NULL;
	// #endif

	return;
}

int gpu_get_query (const char *source, int _kernels, int _inputs, int _outputs) {
	
	int query_id = free_index++;
	if (query_id < 0 || query_id >= query_num) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", query_id);
		exit (1);
	}
	queries[query_id] = gpu_query_new (query_id, device, context, source, _kernels, _inputs, _outputs);
	// /* Set result handler */
	// gpu_query_setResultHandler (queries[ndx], resultHandler);
	
	return query_id;
}

void gpu_free () {
	int error = 0;

	for (int i = 0; i < MAX_QUERIES; i++)
		if (queries[i])
			gpu_query_free (queries[i]);
	if (context)
		error = clReleaseContext (context);
	if (error != CL_SUCCESS)
		fprintf(stderr, "error: failed to free context\n");
	
	return;
}

int gpu_set_input  (int qid, int input_id, int size) {
	if (qid < 0 || qid >= query_num) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpu_query_p query = queries[qid];
	return gpu_query_setInput (query, input_id, size);
}

int gpu_set_output (int qid, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {
	if (qid < 0 || qid >= query_num) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpu_query_p p = queries[qid];
	return gpu_query_setOutput(p, ndx, size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
}

int gpu_set_kernel (int qid, int ndx /* kernel index */,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int * args1, long * args2) {

	if (qid < 0 || qid >= query_num) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpu_query_p p = queries[qid];
	return gpu_query_setKernel (p, ndx, name, callback, args1, args2);
}

int gpu_reset_kernel (int qid, int ndx /* kernel index */,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int * args1, long * args2) {

	if (qid < 0 || qid >= query_num) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpu_query_p p = queries[qid];
	return gpu_query_resetKernel (p, ndx, name, callback, args1, args2);
}

int gpu_exec (int qid,
	size_t *threads, size_t *threadsPerGroup,
	query_operator_p operator,
	void ** input_batches, void ** output_batches, size_t addr_size) {

	if (qid < 0 || qid >= query_num) {
		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
		exit (1);
	}
	gpu_query_p query = queries[qid];
	return gpu_query_exec (query, threads, threadsPerGroup, operator, input_batches, output_batches, addr_size);
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
	
	return;
}

void gpu_reset_kernel_aggregate(int qid, int * args1, long * args2) {
    /**
     * TODO
     * Defence for wrong args format
     **/

	/* Set kernel(s) */
	gpu_reset_kernel (qid, 0, "clearKernel",                    &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 1, "computeOffsetKernel",            &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 2, "computePointersKernel",          &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 3, "countWindowsKernel",             &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 4, "aggregateClosingWindowsKernel",  &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 5, "aggregateCompleteWindowsKernel", &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 6, "aggregateOpeningWindowsKernel",  &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 7, "aggregatePendingWindowsKernel",  &callback_resetConstAggregate, args1, args2);
	gpu_reset_kernel (qid, 8, "packKernel",                     &callback_resetConstAggregate, args1, args2);
	
	return;
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

void callback_resetConstAggregate (cl_kernel kernel, gpu_config_p config, int *args1, long *args2) {
	int numberOfTuples = args1[0];

	int numberOfInputBytes  = args1[1];
	int numberOfOutputBytes = args1[2];

	int hashTableSize = args1[3];

	int maxNumberOfWindows = args1[4];

	// int cache_size = args1[5];
	
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
	
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	
	return;
}

void gpu_set_kernel_reduce(int qid, int * args1, long * args2) {

	/* Set kernel(s) */
	gpu_set_kernel (qid, 0, "clearKernel",           &callback_setKernelReduce, args1, args2);
	gpu_set_kernel (qid, 1, "computeOffsetKernel",   &callback_setKernelReduce, args1, args2);
	gpu_set_kernel (qid, 2, "computePointersKernel", &callback_setKernelReduce, args1, args2);
	gpu_set_kernel (qid, 3, "reduceKernel",          &callback_setKernelReduce, args1, args2);

	return;
}

void gpu_reset_kernel_reduce(int qid, int * args1, long * args2) {

	/* Set kernel(s) */
	gpu_reset_kernel (qid, 0, "clearKernel",           &callback_resetConstReduce, args1, args2);
	gpu_reset_kernel (qid, 1, "computeOffsetKernel",   &callback_resetConstReduce, args1, args2);
	gpu_reset_kernel (qid, 2, "computePointersKernel", &callback_resetConstReduce, args1, args2);
	gpu_reset_kernel (qid, 3, "reduceKernel",          &callback_resetConstReduce, args1, args2);

	return;
}

void gpu_execute (int qid, size_t * threads, size_t * threadsPerGroup, void ** input_batches, void ** output_batches, size_t addr_size) {

	/* Create operator */
	query_operator_p operator = (query_operator_p) malloc (sizeof(query_operator_t));
	if (! operator) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	/* Currently, we assume the same execution pattern for all queries */

	operator->args1 = NULL;
	operator->args2 = NULL;
	
	operator->configure = NULL;

	// operator->readOutput = callback_readOutput;
	operator->execKernel = callback_execKernel;

	gpu_exec (qid, threads, threadsPerGroup, operator, input_batches, output_batches, addr_size);

	/* Free operator */
	if (operator)
		free (operator);

	return;
}

void callback_setKernelReduce (cl_kernel kernel, gpu_config_p context, int *args1, long *args2) {

	int numberOfTuples = args1[0];
	int numberOfBytes  = args1[1];

	int maxNumberOfWindows = args1[2];

	int cache_size = args1[3]; /* Local buffer memory size */

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int),  (void *)     &numberOfTuples);
	error |= clSetKernelArg (kernel, 1, sizeof(int),  (void *)      &numberOfBytes);

	error |= clSetKernelArg (kernel, 2, sizeof(int),  (void *) &maxNumberOfWindows);

	error |= clSetKernelArg (kernel, 3, sizeof(long), (void *)     &previousPaneId);
	error |= clSetKernelArg (kernel, 4, sizeof(long), (void *)        &startOffset);

	/* Set I/O byte buffers */
	error |= clSetKernelArg (
			kernel,
			5,
			sizeof(cl_mem),
			(void *) &(context->kernelInput.inputs[0]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			6,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[0]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			7,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[1]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			8,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[2]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			9,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[3]->device_buffer));

	error |= clSetKernelArg (
			kernel,
			10,
			sizeof(cl_mem),
			(void *) &(context->kernelOutput.outputs[4]->device_buffer));

	/* Set local memory */
	error |= clSetKernelArg (kernel, 11, (size_t)  cache_size, (void *) NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}

void callback_resetConstReduce (cl_kernel kernel, gpu_config_p context, int *args1, long *args2) {

	int numberOfTuples = args1[0];
	int numberOfBytes  = args1[1];

	int maxNumberOfWindows = args1[2];

	// int cache_size = args1[3]; /* Local buffer memory size */

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int),  (void *)     &numberOfTuples);
	error |= clSetKernelArg (kernel, 1, sizeof(int),  (void *)      &numberOfBytes);

	error |= clSetKernelArg (kernel, 2, sizeof(int),  (void *) &maxNumberOfWindows);

	error |= clSetKernelArg (kernel, 3, sizeof(long), (void *)     &previousPaneId);
	error |= clSetKernelArg (kernel, 4, sizeof(long), (void *)        &startOffset);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}

void gpu_execute_reduce(int qid, size_t * threads, size_t * threads_per_group, long * args2, 
	void ** input_batches, void ** output_batches, size_t addr_size) {

	int const kernel_num = 4;
	for (int i=0; i<kernel_num; i++) {
		dbg("[DBG] kernel %d: %10zu threads %10zu threads/group\n", i, threads[i], threads_per_group[i]);
	}

	/* Create and setup operator */
	query_operator_p operator = (query_operator_p) malloc (sizeof(query_operator_t));
	if (! operator) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}
	operator->args1 = NULL;
	operator->args2 = args2;
	operator->configure = callback_configureReduce;
	// operator->readOutput = callback_readOutput;
	operator->execKernel = callback_execKernel;

	gpu_exec(qid, threads, threads_per_group, operator, input_batches, output_batches, addr_size);

	/* Free operator */
	if (operator)
		free (operator);

	return;
}

void callback_configureReduce (cl_kernel kernel, gpu_config_p config, int *args1, long *args2) {

	/* For reduce, they are NULL */
	(void) config;
	(void)   args1;

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 3, sizeof(long), (void *) &previousPaneId);
	error |= clSetKernelArg (kernel, 4, sizeof(long), (void *)    &startOffset);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}

void gpu_set_kernel_select (int qid, int * args) {

	gpu_set_kernel (qid, 0,  "selectKernel",  &callback_setKernelSelect, args, NULL);
	gpu_set_kernel (qid, 1, "compactKernel",  &callback_setKernelCompact, args, NULL);

	return ;
}

void callback_setKernelSelect (cl_kernel kernel, gpu_config_p context, int *args1, long *args2) {

	(void) args2;

	/* Get all constants */
	int numberOfBytes  = args1[0];
	int numberOfTuples = args1[1];
	int cache_size     = args1[2]; /* Local buffer size */

	int error = 0;
	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 0, sizeof(int), (void *)   &numberOfBytes);
	error |= clSetKernelArg (kernel, 1, sizeof(int), (void *)  &numberOfTuples);
	/* Set I/O byte buffers */
	error |= clSetKernelArg (
		kernel,
		2,
		sizeof(cl_mem),
		(void *) &(context->kernelInput.inputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		3,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[0]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		4,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[1]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		5,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[2]->device_buffer));
	error |= clSetKernelArg (
		kernel,
		6,
		sizeof(cl_mem),
		(void *) &(context->kernelOutput.outputs[3]->device_buffer));
	/* Set local memory */
	error |= clSetKernelArg (kernel, 7, (size_t) cache_size, (void *) NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return;
}

void callback_setKernelCompact (cl_kernel kernel, gpu_config_p context, int *args1, long *args2) {

	/* The configuration of this kernel is identical to the select kernel. */
	callback_setKernelSelect (kernel, context, args1, args2);
}

void gpu_execute_aggregate(int qid, size_t * threads, size_t * threads_per_group, long * args2, 
	void ** input_batches, void ** output_batches, size_t addr_size) {

	/* Create operator */
	query_operator_p operator = (query_operator_p) malloc (sizeof(query_operator_t));
	if (! operator) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	/* Currently, we assume the same execution pattern for all queries */

	operator->args1 = NULL;
	operator->args2 = args2;

	operator->configure = callback_configureAggregate;

	operator->execKernel = callback_execKernel;

	gpu_exec (qid, threads, threads_per_group, operator, input_batches, output_batches, addr_size);

	/* Free operator */
	if (operator)
		free (operator);

	return;
}

void callback_configureAggregate (cl_kernel kernel, gpu_config_p context, int *args1, long *args2) {

	(void) context;
	(void)   args1;

	long previousPaneId = args2[0];
	long startOffset    = args2[1];

	int error = 0;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel, 5, sizeof(long), (void *) &previousPaneId);
	error |= clSetKernelArg (kernel, 6, sizeof(long), (void *)    &startOffset);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	return;
}


/* Data movement callbacks */

// void callback_readOutput (gpu_config_p context, int qid, int ndx, int mark) {
	
// 	if (context->kernelOutput.outputs[ndx]->doNotMove)
// 		return;
	
// 	/* Use the mark */
// 	int size;
// 	if (mark >= 0 && (! context->kernelOutput.outputs[ndx]->ignoreMark))
// 		size = mark;
// 	 else
// 		size = context->kernelOutput.outputs[ndx]->size;
	
// 	if (size > context->kernelOutput.outputs[ndx]->size) {
// 		fprintf(stderr, "error: invalid mark for query's %d output buffer %d (marked %d bytes; size is %d bytes)\n",
// 			qid, ndx, size, context->kernelOutput.outputs[ndx]->size);
// 		exit(1);
// 	}
	
// 	/* Copy data across the JNI boundary */
// 	// readMethod = (*env)->GetMethodID (env, class, "outputDataMovementCallback",  "(IIJI)V");
// 	// (*env)->CallVoidMethod (
// 	// 		env, obj, readMethod,
// 	// 		qid,
// 	// 		ndx,
// 	// 		(long) (context->kernelOutput.outputs[ndx]->mapped_buffer),
// 	// 		size);

// 	return;
// }

gpu_config_p callback_execKernel(gpu_config_p config) {
	// int i;
	gpu_config_p p = NULL; // pipeline[0];
	// #ifdef GPU_VERBOSE
	// if (! p)
	// 	dbg("[DBG] (null) callback_execKernel(%p) \n", context);
	// else
	// 	dbg("[DBG] %p callback_execKernel(%p)\n", p, context);
	// #endif
	// /* Shift */
	// for (i = 0; i < D - 1; i++) {
	// 	pipeline[i] = pipeline [i + 1];
	// }
	// pipeline[D - 1] = config;
	return p;
}
