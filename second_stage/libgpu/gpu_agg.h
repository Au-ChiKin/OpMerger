#ifndef __GPU_H_
#define __GPU_H_

#include "gpu_config.h"

#include "../monitor/monitor.h"
#include "../monitor/event_manager.h"

typedef struct query_operator *query_operator_p;
typedef struct query_operator {

	int  *args1;
	long *args2;

	/* For moving data acros JNI, so no use */
	// void (*writeInput) (gpu_config_p, int, int);
	void (*readOutput) (gpu_config_p, int, int, int);
	void (*notifyEnd) (query_event_p);

	void (*configure) (cl_kernel, gpu_config_p, int *, long *);

	gpu_config_p (*execKernel) (gpu_config_p);

} query_operator_t;

/* call opencl api to create kernels */
int gpu_set_kernel (int qid, int ndx,
	const char *name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2);

/* Calls opencl api to reset kernel constants */
int gpu_reset_kernel (int qid, int ndx,
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

/* Resets kernel constants for reduce operator
 * args1:int[] [0]tuples, [1]inputSize, [2]SystemConf.PARTIAL_WINDOWS, [3]outputSize * numberOfThreadsPerGroup
 * args2:long[] [0]previous pane id, [1]input stream start pointer
 */
void gpu_reset_kernel_reduce(int qid, int * args1, long * args2);

void gpu_set_kernel_select(int qid, int * args);

/* Initialise OpenCL device */
void gpu_init(int query_num, int pipeline_depth, event_manager_p event_manager);

/* Creates and returns a new query */
int gpu_get_query (const char *source, int _kernels, int _inputs, int _outputs);

/* Creats a new input buffer */
int gpu_set_input(int qid, int input_id, int size);

/** 
 * Creats a new output buffer 
 * 
 * bearsMark --> "bears" means "supports"
 **/
int gpu_set_output(int qid, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark);

/* Release gpu memory */
void gpu_free();

/* Execute operators*/

void gpu_execute (int qid, 
	size_t * threads, size_t * threadsPerGroup, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event);

/* Reduce */
void gpu_execute_reduce(int qid, 
	size_t * threads, size_t * threads_per_group, 
	long * args2, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event);

void gpu_execute_aggregate(int qid, 
	size_t * threads, size_t * threads_per_group, 
	long * args2, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event);

#endif