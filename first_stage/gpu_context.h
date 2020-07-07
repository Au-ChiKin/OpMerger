#ifndef __GPU_CONTEXT_H_
#define __GPU_CONTEXT_H_

#include "../clib/utils.h"

#include "../clib/debug.h"

// #include "inputbuffer.h"
// #include "outputbuffer.h"

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

typedef struct gpu_kernel_input {
	int count;
	// inputBufferP inputs [MAX_INPUTS]; // TODO
} gpu_kernel_input_t;

typedef struct gpu_kernel_output {
	int count;
	// outputBufferP outputs [MAX_OUTPUTS]; // TODO
} gpu_kernel_output_t;

typedef struct kernel *kernel_p;
typedef struct kernel {
	cl_kernel kernel [2];
} kernel_t;

typedef struct gpu_kernel {
	int count;
	kernel_p kernels [MAX_KERNELS]; /* Every query has one or more kernels */
} gpu_kernel_t;

typedef struct gpu_context *gpu_context_p;
typedef struct gpu_context {
	int qid;
	cl_device_id device;
	cl_context context;
	cl_program program;
	gpu_kernel_t kernel;
	gpu_kernel_input_t kernelInput;
	gpu_kernel_output_t kernelOutput;
	cl_command_queue queue [2];
	int scheduled;
	cl_event  read_event;
	cl_event write_event;
#ifdef GPU_PROFILE
	cl_event exec_event [MAX_KERNELS];
#endif
	long long  readCount;
	long long writeCount;
} gpu_context_t;

gpu_context_p gpu_context(int qid, cl_device_id device, cl_context context, cl_program program, int _kernels, int _inputs, int _outputs);



#endif /* __GPU_CONTEXT_H_ */