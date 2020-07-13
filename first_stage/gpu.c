#include "gpu.h"

#include "../clib/debug.h"
#include "../clib/utils.h"
#include "../clib/openclerrorcode.h"

#include "gpu_query.h"

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
static cl_kernel kernel = NULL;

static gpu_config_p config = NULL;

static cl_mem input_mem = NULL;
static cl_mem flags_mem = NULL;
static cl_mem num_mem = NULL;

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

    // if (error != CL_SUCCESS) {
        size_t lenght;
        char msg [32768]; /* Compiler messager */

        // printf(stderr, "error: failed to build the program");

        clGetProgramBuildInfo(
            program,
            device,
            CL_PROGRAM_BUILD_LOG,
            sizeof(msg),
            msg,
            &lenght);

        // printf(stderr, "%s\n", msg);
        // fflush(stderr);
        // exit(1);
    // }
    dbg(msg, NULL);
    dbg("[GPU] Building program succeed!\n", NULL);
}

void set_kernel_input(int batch_size, int tuple_size, void const * data) {
    cl_int error = 0;

    /* arg:input */
    input_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_ONLY, 
        batch_size * tuple_size * sizeof(unsigned char), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement input", NULL);
        exit(1);
    }

    /* Copy the input buffer on the host to the memory buffer on device */
    error = clEnqueueWriteBuffer(
        config->command_queue[0], 
        input_mem, 
        CL_TRUE,         /* blocking write */
        0, 
        batch_size * tuple_size * sizeof(unsigned char), 
        data,            /* data in the host memeory */
        0, NULL, NULL);  /* event related */
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to enqueue write buffer command", NULL);
        exit(1);
    }
    dbg("[GPU] Succeed to set input\n", NULL);

    /* arg:flags */
    int * flags = (int *)malloc(sizeof(int)*batch_size);
    for (int i=0; i<batch_size; i++) {
        flags[i] = 1; /* default to be selected */
    }
    flags_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_WRITE, 
        batch_size * sizeof(int), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement: flags", NULL);
        exit(1);
    }    

    error = clEnqueueWriteBuffer(
        config->command_queue[0], /* all put into the first queue for now */
        input_mem, 
        CL_TRUE,         /* blocking write */
        0, 
        batch_size * tuple_size * sizeof(unsigned char), 
        data,            /* data in the host memeory */
        0, NULL, NULL);  /* event related */
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to enqueue write buffer command", NULL);
        exit(1);
    }
    dbg("[GPU] Succeed to set flags\n", NULL);

    
}

/* Below are public functions */
// void gpu_init (JNIEnv *env, int _queries, int _depth) {
void gpu_init (char const * filename) {

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
    
    set_program (filename);
    build_program ();

    // [TODO] For multiple queries?
    config = gpu_config(
        0,         /* query_id - query index, only one so 0 */
        device,
        context, 
        program,
        2,         /* _kerenels - kernel number, somehow there are two opencl 
                      kernels within one Saber kernel */
        1,         /* _inputs - input number */
        4);        /* _outputs - output number */
    dbg("[GPU] GPU configuration has finished\n");

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

void gpu_set_kernel(int batch_size, int tuple_size, void const * data) {
    char const kernel_name [64] = "selectKernel";
    cl_int error = 0;

    set_kernel_input(batch_size, tuple_size, data);
    
    /* output args */
    cl_mem offsets_mem = clCreateBuffer(context, CL_MEM_WRITE_ONLY, sizeof(int), NULL, &error);
    cl_mem partitions_mem = clCreateBuffer(context, CL_MEM_WRITE_ONLY, sizeof(int), NULL, &error);
    cl_mem output_mem = clCreateBuffer(context, CL_MEM_WRITE_ONLY, sizeof(unsigned char), NULL, &error);
    cl_mem local_pos_mem = clCreateBuffer(context, CL_MEM_WRITE_ONLY, sizeof(int), NULL, &error);

    kernel = clCreateKernel(program, kernel_name, &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: fail to build the %s\n", kernel_name);
        exit(1);
    }

    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &input_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &flags_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &offsets_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &partitions_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &output_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &local_pos_mem);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: fail to set arguments\n", NULL);
        exit(1);
    }

    dbg("[GPU] Set kernel succeed!\n");

}

void gpu_set_kernel_sim(int batch_size, int tuple_size, void const * data, void const * result) {
    char const kernel_name [64] = "selectf_sim";
    cl_int error = 0;

    /* input arguements */
    /* input and flags */
    set_kernel_input(batch_size, tuple_size, data);
    /* tuple number */
    num_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_ONLY, 
        sizeof(int), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement num", NULL);
        exit(1);
    }
    error = clEnqueueWriteBuffer(
        config->command_queue[0], 
        num_mem, 
        CL_TRUE,         /* blocking write */
        0, 
        sizeof(int), 
        &tuple_size,     /* data in the host memeory */
        0, NULL, NULL);  /* event related */
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to enqueue write buffer command", NULL);
        exit(1);
    }
    dbg("[GPU] Succeed to set input\n", NULL);
    
    /* output args */
    cl_mem output_mem = clCreateBuffer(
        context, 
        CL_MEM_WRITE_ONLY, 
        batch_size * sizeof(int), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement output", NULL);
        exit(1);
    }

    /* retrieve a kernel entry */
    kernel = clCreateKernel(program, kernel_name, &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: fail to build the %s\n", kernel_name);
        exit(1);
    }

    /* set arguments */
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &input_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &flags_mem);
    error = clSetKernelArg( kernel, 0, sizeof(cl_mem), &output_mem);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: fail to set arguments\n", NULL);
        exit(1);
    }

    dbg("[GPU] Set kernel succeed!\n");

}

// void gpu_exec(int batch_size, int tuple_size) {



// }

// int gpu_query_setOutput (gpu_query_p q, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {
// 	if (! q)
// 		return -1;
// 	if (ndx < 0 || ndx > q->contexts[0]->kernelOutput.count) {
// 		fprintf(stderr, "error: output buffer index [%d] out of bounds\n", ndx);
// 		exit (1);
// 	}
// 	int i;
// 	for (i = 0; i < NCONTEXTS; i++)
// 		gpu_context_setOutput (q->contexts[i], ndx, size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
// 	return 0;
// }


// outputBufferP getOutputBuffer (cl_context context, cl_command_queue queue, int size,
// 	int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {

// 	outputBufferP p = malloc(sizeof(output_buffer_t));
// 	if (! p) {
// 		fprintf(stderr, "fatal error: out of memory\n");
// 		exit (1);
// 	}
// 	p->size = size;

// 	p->writeOnly = (unsigned char) writeOnly;
// 	p->doNotMove = (unsigned char) doNotMove;
// 	p->bearsMark = (unsigned char) bearsMark;
// 	p->readEvent = (unsigned char) readEvent;
// 	p->ignoreMark= (unsigned char) ignoreMark;

// 	int error;
// 	cl_mem_flags flags;
// 	if (writeOnly)
// 		flags = CL_MEM_WRITE_ONLY;
// 	else
// 		flags = CL_MEM_READ_WRITE;
// 	/* Set p->device_buffer */
// 	p->device_buffer = clCreateBuffer (
// 		context,
// 		flags,
// 		p->size,
// 		NULL,
// 		&error);
// 	if (! p->device_buffer) {
// 		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
// 		exit (1);
// 	}
// 	/* Allocate buffers */
// 	p->pinned_buffer = clCreateBuffer (
// 		context,
// 		CL_MEM_READ_WRITE | CL_MEM_ALLOC_HOST_PTR,
// 		p->size,
// 		NULL,
// 		&error);
// 	if (! p->pinned_buffer) {
// 		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
// 		exit (1);
// 	}
// 	p->mapped_buffer = (void *) clEnqueueMapBuffer (
// 		queue,
// 		p->pinned_buffer,
// 		CL_TRUE,
// 		CL_MAP_READ,
// 		0,
// 		p->size,
// 		0, NULL, NULL,
// 		&error);
// 	if (! p->mapped_buffer) {
// 		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
// 		exit (1);
// 	}
// 	return p;
// }

// void gpu_context_setOutput (gpu_config_p c, int ndx, int size,

// 	int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {

// 	c->kernelOutput.outputs[ndx] =
// 			getOutputBuffer (c->context, c->queue[0], size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
// }



// void write_input(int batch_size, int tuple_size, char const * data) {
//     cl_int error = 0;

//     error = clEnqueueWriteBuffer(config->queue[0], input_mem, CL_TRUE, 0, batch_size * tuple_size, data, 0, NULL, NULL);
//     if (error != CL_SUCCESS) {
//         fprintf(stderr, "error: fialed to enqueue write buffer", NULL);
//     }
// }



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
//     gpu_query_p q, 
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
//     gpu_query_p query, 
//     size_t *threads, 
//     size_t *threadsPerGroup, 
//     queryOperatorP operator, 
//     JNIEnv *env, 
//     jobject obj) {
	
// 	gpu_context_p context = gpu_context_switch (query);
	
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
// 	gpu_query_p p = queries[qid];
// 	return gpu_query_exec (p, threads, threadsPerGroup, operator, env, obj);
// }