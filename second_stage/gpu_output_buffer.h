#ifndef __OUTPUT_BUFFER_H_
#define __OUTPUT_BUFFER_H_

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

typedef struct output_buffer *output_buffer_p;
typedef struct output_buffer {
	int size;
	unsigned char writeOnly;
	unsigned char doNotMove;
	unsigned char bearsMark; /* The last integer is the mark */
	unsigned char readEvent;
	unsigned char ignoreMark;
	cl_mem device_buffer;
	cl_mem pinned_buffer;
	void  *mapped_buffer;
} output_buffer_t;

output_buffer_p getOutputBuffer (cl_context, cl_command_queue, int, int, int, int, int, int);

void freeOutputBuffer (output_buffer_p, cl_command_queue);

int getOutputBufferSize (output_buffer_p);

#endif /* __OUTPUT_BUFFER_H_ */

