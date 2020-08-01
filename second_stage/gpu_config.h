#ifndef __GPU_CONFIG_H_
#define __GPU_CONFIG_H_

#include "../clib/utils.h"

#include "../clib/debug.h"
#include "gpu_input_buffer.h"
#include "gpu_output_buffer.h"

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

typedef struct gpu_kernel_input {
	int count;
	input_buffer_p inputs [MAX_INPUTS];
} gpu_kernel_input_t;

typedef struct gpu_kernel_output {
	int count;
	output_buffer_p outputs [MAX_OUTPUTS];
} gpu_kernel_output_t;

typedef struct kernel *kernel_p;
typedef struct kernel {
	cl_kernel kernel [2];
} kernel_t;

typedef struct gpu_kernel {
	int count;
	kernel_p kernels [MAX_KERNELS]; /* Every query has one or more kernels */
} gpu_kernel_t;

typedef struct gpu_config *gpu_config_p;
typedef struct gpu_config {
	int query_id;
	cl_device_id device;
	cl_context context;
	cl_program program;
	gpu_kernel_t kernel;
	gpu_kernel_input_t kernelInput;
	gpu_kernel_output_t kernelOutput;
	cl_command_queue command_queue [2];
	int scheduled;
	cl_event  read_event;
	cl_event write_event;
#ifdef GPU_PROFILE
	cl_event exec_event [MAX_KERNELS];
#endif
	long long  readCount;
	long long writeCount;
} gpu_config_t;

gpu_config_p gpu_config(int query_id, cl_device_id device, cl_context context, cl_program program, int _kernels, int _inputs, int _outputs);

void gpu_config_free (gpu_config_p query);

void gpu_config_setInput  (gpu_config_p, int, int);

void gpu_config_setOutput (gpu_config_p, int, int, int, int, int, int, int);

void gpu_config_setKernel (gpu_config_p,
		int,
		const char *,
		void (*callback)(cl_kernel, gpu_config_p, int *, long *),
		int *, long *);

// void gpu_config_configureKernel (gpu_config_p,
// 		void (*callback)(cl_kernel, gpu_config_p, int *, long *),
// 		int *, long *);

// void gpu_config_waitForReadEvent (gpu_config_p);

// void gpu_config_waitForWriteEvent (gpu_config_p);

// void gpu_config_profileQuery (gpu_config_p);

// void gpu_config_flush (gpu_config_p);

// void gpu_config_finish (gpu_config_p);

// void gpu_config_submitKernel (gpu_config_p, size_t *, size_t *);

// void gpu_config_moveOutputBuffers (gpu_config_p);

// void gpu_config_writeInput (gpu_config_p,
// 		void (*callback)(gpu_config_p, int, int),
// 		int);

/**
 * host_addr - an array of addresses to input batches
 **/ 
void gpu_config_moveInputBuffers (gpu_config_p config, void ** host_addr, size_t addr_size);

// void gpu_config_readOutput (gpu_config_p,
// 		void (*callback)(gpu_config_p, int, int, int),
// 		int);

#endif /* __GPU_CONFIG_H_ */