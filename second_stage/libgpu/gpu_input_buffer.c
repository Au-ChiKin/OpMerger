#include "gpu_input_buffer.h"

#include "openclerrorcode.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stdint.h>

input_buffer_p getInputBuffer (cl_context context, cl_command_queue queue, int size) {

	input_buffer_p buffer = malloc(sizeof(input_buffer_t));
	if (! buffer) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}
	buffer->size = size;
	int error;
	/* Set p->device_buffer */
	buffer->device_buffer = clCreateBuffer (
		context,
		CL_MEM_READ_ONLY,
		buffer->size,
		NULL,
		&error);
	if (! buffer->device_buffer) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}
	return buffer;
}

int getInputBufferSize (input_buffer_p b) {
	return b->size;
}

void freeInputBuffer (input_buffer_p b, cl_command_queue queue) {
	if (b) {
		if (b->device_buffer)
			clReleaseMemObject(b->device_buffer);

		free (b);
	}
}

