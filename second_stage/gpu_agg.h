#ifndef __GPU_H_
#define __GPU_H_

#include "gpu_config.h"

/* call opencl api to create kernels */
int gpu_set_kernel (int qid, int ndx,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2);

/* set kernel for aggregate operator
 * args1:int[] 
 * args2:long[] [0]previous pane id, [1]input stream start pointer
 */ 
void gpu_set_kernel_aggregate(int qid, int * _args1, long * _args2);

void callback_setKernelAggregate (cl_kernel kernel, gpu_config_p context, int *args1, long *args2);

/* Initialise OpenCL device */
void gpu_init(int query_num);

/* Release gpu memory */
void gpu_free();

#endif