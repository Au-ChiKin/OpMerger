#ifndef __GPU_H_
#define __GPU_H_

#include "gpu_config.h"

typedef struct query_operator *query_operator_p;
typedef struct query_operator {

	int  *args1;
	long *args2;

	// void (*writeInput) (gpu_config_p, int, int);
	void (*readOutput) (gpu_config_p, int, int, int);

	void (*configure) (cl_kernel, gpu_config_p, int *, long *);

	gpu_config_p (*execKernel) (gpu_config_p);

} query_operator_t;

/* call opencl api to create kernels */
int gpu_set_kernel (int qid, int ndx,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2);

/* set kernel for aggregate operator
 * args1:int[] [0]tuples, inputSize, outputSize, tableSize, SystemConf.PARTIAL_WINDOWS, keyLength * numberOfThreadsPerGroup
 * args2:long[] [0]previous pane id, [1]input stream start pointer
 */ 
void gpu_set_kernel_aggregate(int qid, int * _args1, long * _args2);

/* set kernel for reduce operator
 * args1:int[] [0]tuples, [1]inputSize, [2]SystemConf.PARTIAL_WINDOWS, [3]outputSize * numberOfThreadsPerGroup
 * args2:long[] [0]previous pane id, [1]input stream start pointer
 */ 
void gpu_set_kernel_reduce(int qid, int * args1, long * args2);

/* Initialise OpenCL device */
void gpu_init(int query_num);

/* Creates and returns a new query */
int gpu_get_query (const char *source, int _kernels, int _inputs, int _outputs);

/* Creats a new input buffer */
int gpu_set_input(int qid, int input_id, int size);

/* Creats a new output buffer */
int gpu_set_output(int qid, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark);

/* Release gpu memory */
void gpu_free();

/* Execute operators*/

/* Reduce */
void gpu_execute_reduce(int qid, size_t * threads, size_t * threads_per_group, long * args2, void ** input_batches, void ** output_batches, size_t addr_size);

#endif