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

#define MAX_SOURCE_SIZE (0x100000)

static cl_platform_id platform = NULL;
static cl_device_id device = NULL;
static cl_context context = NULL;
static cl_program program = NULL;

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


static void set_program(char const * file_name) {
    FILE *file_p;
    char * source_str;
    size_t source_size;
    cl_int error = 0;

    /* Load kernel source */
    file_p = fopen(file_name, "r");
    if (!file_p) {
        fprintf(stderr, "Failed to load kernel.\n");
        exit(1);
    }
    source_str = (char*)malloc(MAX_SOURCE_SIZE);
    source_size = fread( source_str, 1, MAX_SOURCE_SIZE, file_p);
    fclose(file_p);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
    dbg("[GPU] Loaded kernel source length of %d bytes\n", source_size);

    // Build a program from the loaded kernels
    program = clCreateProgramWithSource(
        context,
        1,         /* The number of strings */
        (const char**) &source_str,
        NULL,
        error);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
}

static void build_program() {
    cl_int error = 0;

    error = clBuildProgram(
        program,
        1,         /* number of devices in next param */
        &device,
        NULL,      /* Build options */
        NULL,      /* a pointer to  a notification callback function */
        NULL);     /* a data to be passed as a parameter to the callback funciton */

    if (error != CL_SUCCESS) {
        size_t lenght;
        char buffer[2048];

        printf(stderr, "error: failed to build the program");

        clGetProgramBuildInfo(
            program,
            device,
            CL_PROGRAM_BUILD_LOG,
            sizeof(buffer),
            buffer,
            &lenght);

        printf(stderr, "%s\n", buffer);
        exit(1);
    }
    dbg("[GPU] Building program succeed!\n", NULL);
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
	get_deviceInfo ();
	
    set_context ();
    
    set_program ("filters_multiple_operators.cl");
    build_program ();

    // TODO: set a propert context variable
    gpu_config_p config = gpu_config(
        0,         // Only one query, idex 0
        device,
        context, 
        program,
        1,         
        1, 
        1);

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