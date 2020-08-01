#ifndef __INPUT_BUFFER_H_
#define __INPUT_BUFFER_H_

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

typedef struct input_buffer *input_buffer_p;
typedef struct input_buffer {
	int size;
	cl_mem device_buffer;
	// cl_mem pinned_buffer;
	// void  *mapped_buffer;
} input_buffer_t;

input_buffer_p getInputBuffer (cl_context, cl_command_queue, int);

void freeInputBuffer (input_buffer_p, cl_command_queue);

int getInputBufferSize (input_buffer_p);

#endif /* __INPUT_BUFFER_H_ */
