#include "gpu.h"

#include "../clib/debug.h"
#include "../clib/utils.h"

#include "../clib/openclerrorcode.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

static cl_platform_id platform = NULL;
static cl_device_id device = NULL;
static cl_context context = NULL;

// static int query_number;
// static int free_index;
// ... a bunch of varibales

/* some callback_functions */
// ...

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
        2,                    // return an ID for only one GPU &device_id
        &device,              // on return, the device ID
        &count);              // on return, the number of devices
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	dbg("[GPU] Obtained 1/%u devices available\n", count);
	return;
}

static void set_context () {
	int error = 0;
	context = clCreateContext (
		0,        // a reserved variable
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

static void get_deviceInfo () {

	cl_int error = 0;

	cl_uint value = 0;
	char extensions [2048];
	char name [256];

	error = 0;
	error |= clGetDeviceInfo (device, CL_DEVICE_MEM_BASE_ADDR_ALIGN, sizeof (cl_uint), &value,         NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_EXTENSIONS,                      2048, &extensions[0], NULL);
	error |= clGetDeviceInfo (device, CL_DEVICE_NAME,                            2048, &name[0],       NULL);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	fprintf(stdout, "[GPU] GPU name: %s\n", name);
	fprintf(stdout, "[GPU] GPU supported extensions are: %s\n", extensions);
	fprintf(stdout, "[GPU] GPU memory addresses are %u bits aligned\n", value);

	return ;
}

// void gpu_init (JNIEnv *env, int _queries, int _depth) {
void gpu_init () {

	// int i;
	// (void) env; 
    /* 
     * It works around some compiler warnings. Some compilers will warn if you don't use a function parameter. In 
     * such a case, you might have deliberately not used that parameter, not be able to change the interface for some 
     * reason, but still want to shut up the warning. That (void) casting construct is a no-op that makes the warning 
     * go away. Here's a simple example using clang:
     * int f1(int a, int b)
     * {
     *     (void)b; 
     *     return a; 
     * }
     * int f2(int a, int b) {
     *     return a; 
     * }
     * Build using the -Wunused-parameter flag and presto:
     * $ clang -Wunused-parameter   -c -o example.o example.c 
     * example.c:7:19: warning: unused parameter 'b' [-Wunused-parameter] 
     * int f2(int a, int b) 
     *                   ^ 
     * 1 warning generated.
     */ 

	set_platform ();
	set_device ();
	set_context ();
	get_deviceInfo ();

    // TODO: set a propert context variable and find out the program thing
    cl_program program;
    gpu_config_p context_p = gpu_config(1, device, context, program, 1, 1, 1);

	// Q = _queries; /* Number of queries */
	// freeIndex = 0;
	// for (i = 0; i < MAX_QUERIES; i++)
	// 	queries[i] = NULL;

	// D = _depth; /* Pipeline depth */
	// for (i = 0; i < MAX_DEPTH; i++)
	// 	pipeline[i] = NULL;

	// #ifdef GPU_HANDLER
	// /* Create result handler */
	// resultHandler = result_handler_init (env);
	// #else
	// resultHandler = NULL;
	// #endif

	return;
}

void gpu_free () {
	// int i;
	int error = 0;
	// for (i = 0; i < MAX_QUERIES; i++)
	// 	if (queries[i])
	// 		gpu_query_free (queries[i]);
	if (context)
		error = clReleaseContext (context);
	if (error != CL_SUCCESS)
		fprintf(stderr, "error: failed to free context\n");
	
	return;
}

// int gpu_query_exec (
//     gpuQueryP q, 
//     size_t *threads, 
//     size_t *threadsPerGroup, 
//     queryOperatorP operator, 
//     JNIEnv *env, 
//     jobject obj) {
	
// 	if (! q)
// 		return -1;

// 	if (NCONTEXTS == 1) { // NCONTEXTS defined in utils.h
// 		return gpu_query_exec_1 (q, threads, threadsPerGroup, operator, env, obj);
// 	} 
//     // else {
// 	// 	return gpu_query_exec_2 (q, threads, threadsPerGroup, operator, env, obj);
// 	// }
// }

// /*
//  * Only one pipeline
//  */
// static int gpu_query_exec_1 (
//     gpuQueryP query, 
//     size_t *threads, 
//     size_t *threadsPerGroup, 
//     queryOperatorP operator, 
//     JNIEnv *env, 
//     jobject obj) {
	
// 	gpuContextP context = gpu_context_switch (query);
	
// 	/* Write input */
// 	gpu_context_writeInput (context, operator->writeInput, env, obj, query->qid);

// 	gpu_context_moveInputBuffers (context);
	
// 	if (operator->configure != NULL) {
// 		gpu_context_configureKernel (context, operator->configure, operator->args1, operator->args2);
// 	}

// 	gpu_context_submitKernel (context, threads, threadsPerGroup);

// 	gpu_context_moveOutputBuffers (context);

// 	gpu_context_flush (context);
	
// 	gpu_context_finish(context);
	
// 	gpu_context_readOutput (context, operator->readOutput, env, obj, query->qid);

// 	return 0;
// }


// int gpu_exec (int qid,
// 	size_t *threads, 
//     size_t *threadsPerGroup,
// 	queryOperatorP operator,
// 	JNIEnv *env, 
//     jobject obj) {

// 	if (qid < 0 || qid >= Q) {
// 		fprintf(stderr, "error: query index [%d] out of bounds\n", qid);
// 		exit (1);
// 	}
// 	gpuQueryP p = queries[qid];
// 	return gpu_query_exec (p, threads, threadsPerGroup, operator, env, obj);
// }