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
static cl_kernel kernel[3];
static cl_kernel compact_kernel;

static gpu_config_p config = NULL;

static cl_mem input_mem = NULL;
static cl_mem flags_mem = NULL;
static cl_mem goffsets_mem = NULL;
static cl_mem partitions_mem = NULL;
static cl_mem output_mem = NULL;

static int batch_size = 0;
static int const tuple_size = 32; /* byte */ 
static int const tuple_per_thread = 2;
static int const max_threads_per_group = 64;

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

static void set_program(char const * file_name) {
    FILE *file_p;
    char * source_str;
    size_t source_size;
    cl_int error = 0;

    /* Load kernel source */
    file_p = fopen(file_name, "rb");
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
    dbg("[GPU] Loaded kernel source length of %zu bytes\n", source_size);

    // Build a program from the loaded kernels
    program = clCreateProgramWithSource(
        context,
        1,         /* The number of strings */
        (const char**) &source_str,
        NULL,
        &error);
	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
    dbg("[GPU] Created a program from the loaded source\n", NULL);
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

        // fprintf(stderr, "error: failed to build the program");

        clGetProgramBuildInfo(
            program,
            device,
            CL_PROGRAM_BUILD_LOG,
            sizeof(msg),
            msg,
            &lenght);

        fprintf(stderr, "%s", msg);
        // exit(1);
    // }
    dbg("[GPU] Building program succeed!\n", NULL);
}

void create_kernel_input() {
    cl_int error = 0;
    
    /* arg:input */
    input_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_WRITE, 
        batch_size * tuple_size * sizeof(unsigned char), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement input\n", NULL);
        exit(1);
    }

    dbg("[GPU] Succeed to create input buffer\n", NULL);
}

void create_kernel_inter() {
    cl_int error = 0;

    /* arg:flags */
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

    partitions_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_WRITE, 
        batch_size * sizeof(int), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement: partitions", NULL);
        exit(1);
    }    

    goffsets_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_WRITE, 
        batch_size * tuple_size * sizeof(unsigned char), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement: goffsets", NULL);
        exit(1);
    }

    dbg("[GPU] Succeed to create intermediate buffers\n", NULL);
}

void create_kernel_output() {
    cl_int error = 0;

    output_mem = clCreateBuffer(
        context, 
        CL_MEM_READ_WRITE, 
        batch_size * tuple_size * sizeof(unsigned char), 
        NULL, 
        &error);
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to set arguement output\n", NULL);
        exit(1);
    }
    dbg("[GPU] Succeed to create output buffer\n", NULL);
}

void set_kernel_args(cl_kernel kernel_) {
    cl_int error = 0;

    /* 
     * threads number = batch_size / tuples per threads
     * threads per group = number of threads per group(64) or threads number (if threads number is smaller)
     * number of groups = threads number / threads per group
     * 
     * cache size = 4 (up to 4 loacl cache) * threads per group * tuples per threads
     */
    int const cache_size = 4 * max_threads_per_group * tuple_per_thread;

	/* Set constant arguments */
	error |= clSetKernelArg (kernel_, 0, sizeof(int), (void *)   &tuple_size);
	error |= clSetKernelArg (kernel_, 1, sizeof(int), (void *)  &batch_size);
	/* Set I/O byte buffers */
	error |= clSetKernelArg (
		kernel_,
		2,
		sizeof(cl_mem),
		&input_mem);
	error |= clSetKernelArg (
		kernel_,
		3,
		sizeof(cl_mem),
		&flags_mem);
	error |= clSetKernelArg (
		kernel_,
		4,
		sizeof(cl_mem),
		&goffsets_mem);    
	error |= clSetKernelArg (
		kernel_,
		5,
		sizeof(cl_mem),
		&partitions_mem);
	error |= clSetKernelArg (
		kernel_,
		6,
		sizeof(cl_mem),
		&output_mem);
	/* Set local memory */
	error |= clSetKernelArg (
        kernel_, 
        7, 
        (size_t) cache_size, 
        (void *) NULL);
    
    if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

}

void read_count(int group_size, int * count) {
    cl_int error = 0;

    int * partitions = (int *) malloc(group_size * sizeof(int));

    error = clEnqueueReadBuffer(
        config->command_queue[0], 
        partitions_mem, 
        CL_TRUE, 
        0, 
        group_size * sizeof(int), 
        partitions, 
        0, NULL, NULL);
    if (error != CL_SUCCESS) {
		fprintf(stderr, "error: fail to read count\n");
		exit (1);
	}

    *count = 0;
    for (int i=0; i<group_size; i++) {
        *count += partitions[i];
    }

    // printf("Current group size: %d and count : %d\n", group_size, *count);
}

/* Below are public functions */
void gpu_init (char const * filename, int size, int kernel_num) {

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

    batch_size = size;

    // [TODO] For multiple queries?
    config = gpu_config(
        0,         /* query_id - query index, only one so 0 */
        device,
        context, 
        program,
        kernel_num,  /* _kerenels - kernel number, somehow there are two opencl 
                        kernels within one Saber kernel */
        1,           /* _inputs - input number */
        4);          /* _outputs - output number */
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

void gpu_set_kernel() {
    cl_int error = 0;

    /* input arguements */
    create_kernel_input();
    
    /* inter-arguments i.e. arguments that holds the shared information between select and compact */
    create_kernel_inter();

    /* output args */
    create_kernel_output();

    /* set kernels */
    for (int k=0; k<config->kernel.count; k++) {
        char const compact_kernel_name [64] = "compactKernel";

        char select_kernel_name [64] = "selectKernel";
        char num [2];
        sprintf(num, "%d", k+1);
        strcat(select_kernel_name, num);

        /* retrieve a kernel entry */
        kernel[k] = clCreateKernel(program, select_kernel_name, &error);
        if (error != CL_SUCCESS) {
            fprintf(stderr, "error: fail to build the %s\n", select_kernel_name);
            exit(1);
        }

        /* set arguments for select kernel */
        set_kernel_args(kernel[k]);

        compact_kernel = clCreateKernel(program, compact_kernel_name, &error);
        if (error != CL_SUCCESS) {
            fprintf(stderr, "error: fail to build the %s\n", compact_kernel_name);
            exit(1);
        }

        set_kernel_args(compact_kernel);
    }

    dbg("[GPU] Set kernel succeed!\n");
}

void gpu_read_input(void const * data, bool profiling, long * start, long * end) {
    cl_int error = 0;
    cl_event perf_event;
    cl_event * perf_event_p = NULL;
    if (profiling) {
        perf_event_p = &perf_event;
    }

    /* Copy the input buffer on the host to the memory buffer on device */
    error = clEnqueueWriteBuffer(
        config->command_queue[0], 
        input_mem, 
        CL_TRUE,         /* blocking write */
        0, 
        batch_size * tuple_size * sizeof(unsigned char), 
        data,            /* data in the host memeory */
        0, NULL, perf_event_p);  /* event related */
    if (error != CL_SUCCESS) {
        fprintf(stderr, "error: failed to enqueue write buffer command\n", NULL);
        exit(1);
    }

    dbg("[GPU] Succeed to read input\n", NULL);

    cl_ulong cl_start = 0, cl_end = 0;
    if (profiling) {
        
        clWaitForEvents(1, perf_event_p);
        
        clGetEventProfilingInfo(perf_event, CL_PROFILING_COMMAND_START, sizeof(cl_ulong), &cl_start, NULL);
        clGetEventProfilingInfo(perf_event, CL_PROFILING_COMMAND_END, sizeof(cl_ulong), &cl_end, NULL);
    }
    if (start) {
        *start = cl_start;
    }
    if (end) {
        *end = cl_end;
    }
}

void gpu_write_output(void * output, int tuple_num, bool profiling, long * start, long * end) {
    cl_int error = 0;
    cl_event perf_event;
    cl_event * perf_event_p = NULL;
    if (profiling) {
        perf_event_p = &perf_event;
    }

    error = clEnqueueReadBuffer(
        config->command_queue[0], 
        output_mem, 
        CL_TRUE, 
        0, 
        tuple_num * tuple_size * sizeof(unsigned char), 
        output, 
        0, NULL, perf_event_p);

    cl_ulong cl_start = 0, cl_end = 0;
    if (profiling) {
        
        clWaitForEvents(1, perf_event_p);
        
        clGetEventProfilingInfo(perf_event, CL_PROFILING_COMMAND_START, sizeof(cl_ulong), &cl_start, NULL);
        clGetEventProfilingInfo(perf_event, CL_PROFILING_COMMAND_END, sizeof(cl_ulong), &cl_end, NULL);
    }
    if (start) {
        *start = cl_start;
    }
    if (end) {
        *end = cl_end;
    }
}

int gpu_exec() {
    size_t local_item_size = 64; /* minimum threads per group */
    size_t global_item_size = batch_size / tuple_per_thread;

    int count = 0;

    for (int k=0; k<config->kernel.count; k++) {
        cl_int error = 0;

        error = clEnqueueNDRangeKernel(
            config->command_queue[0],
            kernel[k],
            1,
            NULL,
            &global_item_size,
            &local_item_size,
            0, NULL, NULL);
        if (error != CL_SUCCESS) {
            fprintf(stderr, "error: fail to enqueue the kernel with error(%d): %s\n", error, getErrorMessage(error));
            exit(1);
        }

        error = clEnqueueNDRangeKernel(
            config->command_queue[0],
            compact_kernel,
            1,
            NULL,
            &global_item_size,
            &local_item_size,
            0, NULL, NULL);
        if (error != CL_SUCCESS) {
            fprintf(stderr, "error: fail to enqueue the compact kernel with error(%d): %s\n", error, getErrorMessage(error));
            exit(1);
        }

        clFinish(config->command_queue[0]);

        // count output by suming partitions
        read_count(global_item_size/local_item_size, &count);

        if (config->kernel.count > 1 && k < config->kernel.count-1) {

            // make a copy of output
            cl_int error = 0;

            /* Copy the input buffer on the host to the memory buffer on device */
            unsigned char * copy = (unsigned char *) malloc(sizeof(unsigned char) * tuple_size * count);

            error = clEnqueueReadBuffer(
                config->command_queue[0], 
                output_mem, 
                CL_TRUE, 
                0, 
                count * tuple_size * sizeof(unsigned char), 
                copy, 
                0, NULL, NULL);
            if (error != CL_SUCCESS) {
                fprintf(stderr, "error: fail to read count\n");
                exit (1);
            }

            clFinish(config->command_queue[0]);

            // copy the created copy to input
            error = clEnqueueWriteBuffer(
                config->command_queue[0], 
                input_mem, 
                CL_TRUE,         /* blocking write */
                0, 
                count * tuple_size * sizeof(unsigned char), 
                copy,            /* pointer to data in the host memeory */
                0, NULL, NULL);  /* event related */
            if (error != CL_SUCCESS) {
                fprintf(stderr, "error: failed to enqueue write buffer command\n", NULL);
                exit(1);
            }

            clFinish(config->command_queue[0]);

            if (copy) {
                free(copy);
            }

            // run again with new item size
            global_item_size = count / tuple_per_thread;
        }
    }

    dbg("[GPU] Running kernel finishes!\n", NULL);

    return count;
}

void gpu_free () {
	// int i;
	int error = 0;
	// for (i = 0; i < MAX_QUERIES; i++)
	// 	if (queries[i])
	// 		gpu_query_free (queries[i]);

    error = clFlush(config->command_queue[0]);
    error |= clFlush(config->command_queue[1]);
    error |= clFinish(config->command_queue[0]);
    error |= clFinish(config->command_queue[1]);
    error |= clReleaseKernel(kernel[0]);
    error |= clReleaseProgram(program);
    error |= clReleaseMemObject(input_mem);
    error |= clReleaseMemObject(output_mem);
    error |= clReleaseCommandQueue(config->command_queue[0]);
    error |= clReleaseCommandQueue(config->command_queue[1]);
	if (error != CL_SUCCESS)
		fprintf(stderr, "error: failed to free objects\n");
    
	if (context)
		error = clReleaseContext (context);
	if (error != CL_SUCCESS)
		fprintf(stderr, "error: failed to free context\n");

	return;
}