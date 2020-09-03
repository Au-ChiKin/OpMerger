#include "gpu_query.h"

#include "openclerrorcode.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>
#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

/* w/o  pipelining */
static int gpu_query_exec_1 (
	gpu_query_p, 
	size_t *, size_t *, 
	query_operator_p, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event);

/* with pipelining */
static int gpu_query_exec_2 (
	gpu_query_p query, 
	size_t *threads, size_t *threadsPerGroup, 
	query_operator_p operator, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event);

gpu_query_p gpu_query_new (int qid, cl_device_id device, cl_context context, const char *source,
	int _kernels, int _inputs, int _outputs) {
	
	int i;
	int error = 0;
	char msg [32768]; /* Compiler message */
	size_t length;

	/*
	 * TODO
	 *
	 * Remove the following macro and select -cl-nv-verbose
	 * based on the type of GPU device (i.e. NVIDIA or not)
	 */
#ifdef __APPLE__
	const char *flags = "-cl-fast-relaxed-math -Werror";
#else
	const char *flags = "-cl-fast-relaxed-math -Werror -cl-nv-verbose";
#endif

	gpu_query_p query = (gpu_query_p) malloc (sizeof(gpu_query_t));
	if (! query) {
		fprintf(stderr, "fatal error: out of memory\n");
		exit(1);
	}

	query->qid = qid;

	query->device = device;
	query->context = context;

	/* Create program */
	query->program = clCreateProgramWithSource (
		query->context, 
		1, 
		(const char **) &source, 
		NULL, 
		&error);
	if (! query->program) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	/* Build program */
	error = clBuildProgram (
		query->program, 
		1, 
		&device, 
		flags, 
		NULL, 
		NULL);

	/* Get compiler info (or error) */
	clGetProgramBuildInfo (
		query->program, 
		query->device, 
		CL_PROGRAM_BUILD_LOG, 
		sizeof(msg), 
		msg, 
		&length);
	if (length > /* Minimum output length */ 2) {
		fprintf(stderr, "%s (%zu chars)\n", msg, length);
	}
	fflush (stderr);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	// query->handler = NULL;

	query->cur_config = -1;
	for (i = 0; i < NCONTEXTS; i++) {
		query->configs[i] = gpu_config(query->qid, query->device, query->context, query->program, _kernels, _inputs, _outputs);
	}

	return query;
}

// void gpu_query_setResultHandler (gpu_query_p q, resultHandlerP handler) {
// 	if (! q)
// 		return ;
// 	q->handler = handler;
// }

void gpu_query_free (gpu_query_p query) {
	int i;
	if (query) {
		for (i = 0; i < NCONTEXTS; i++)
			gpu_config_free (query->configs[i]);
		if (query->program)
			clReleaseProgram (query->program);
		free (query);
	}
}

int gpu_query_setInput (gpu_query_p query, int input_id, int size) {
	if (! query)
		return -1;
	if (input_id < 0 || input_id > query->configs[0]->kernelInput.count) {
		fprintf(stderr, "error: input buffer index [%d] out of bounds\n", input_id);
		exit (1);
	}
	int i;
	for (i = 0; i < NCONTEXTS; i++)
		gpu_config_setInput (query->configs[i], input_id, size);
	return 0;
}

int gpu_query_setOutput (gpu_query_p q, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {
	if (! q)
		return -1;
	if (ndx < 0 || ndx > q->configs[0]->kernelOutput.count) {
		fprintf(stderr, "error: output buffer index [%d] out of bounds\n", ndx);
		exit (1);
	}
	int i;
	for (i = 0; i < NCONTEXTS; i++)
		gpu_config_setOutput (q->configs[i], ndx, size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
	return 0;
}

int gpu_query_setKernel (gpu_query_p query,
	int kernel_id,
	const char * name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2) {

	if (! query)
		return -1;

	if (kernel_id < 0 || kernel_id > query->configs[0]->kernel.count) {
		fprintf(stderr, "error: kernel index [%d] out of bounds\n", kernel_id);
		exit (1);
	}
	int i;
	for (i = 0; i < NCONTEXTS; i++) {
		gpu_config_setKernel(query->configs[i], kernel_id, name, callback, args1, args2);
	}
	return 0;
}

int gpu_query_resetKernel (gpu_query_p query,
	int kernel_id,
	const char * name,
	void (*callback)(cl_kernel, gpu_config_p, int *, long *),
	int *args1, long *args2) {

	if (! query)
		return -1;

	if (kernel_id < 0 || kernel_id > query->configs[0]->kernel.count) {
		fprintf(stderr, "error: kernel index [%d] out of bounds\n", kernel_id);
		exit (1);
	}
	int i;
	for (i = 0; i < NCONTEXTS; i++) {
		gpu_config_resetKernel(query->configs[i], kernel_id, name, callback, args1, args2);
	}
	return 0;
}

gpu_config_p gpu_switch_config(gpu_query_p query) {
	if (! query) {
		fprintf (stderr, "error: null query\n");
		return NULL;
	}
#ifdef GPU_VERBOSE
	int current = (query->cur_config) % NCONTEXTS;
#endif
	int next = (++query->cur_config) % NCONTEXTS;
#ifdef GPU_VERBOSE
	if (current >= 0)
	// (%lld read(s), %lld write(s))
		dbg ("[DBG] switch from %d to context %d\n",
			current, next);
#endif
	return query->configs[next];
}

int gpu_query_exec (
	gpu_query_p query, 
	size_t *threads, size_t *threadsPerGroup, 
	query_operator_p operator, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event) {
	
	if (! query)
		return -1;

	if (NCONTEXTS == 1) {
		return gpu_query_exec_1 (
			query, 
			threads, threadsPerGroup, 
			operator, 
			input_batches, output_batches, addr_size,
			event);
	} else {
		return gpu_query_exec_2 (
			query, 
			threads, threadsPerGroup, 
			operator, 
			input_batches, output_batches, addr_size,
			event);
	}
}

static int gpu_query_exec_1 (
	gpu_query_p query, 
	size_t *threads, size_t *threadsPerGroup, 
	query_operator_p operator, 
	void ** input_batches, void ** output_batches, size_t addr_size,
	query_event_p event) {
	
	/* There is only one config for this prototype */
	gpu_config_p config = gpu_switch_config(query);

	/* Write input */
	gpu_config_moveInputBuffers (config, input_batches, addr_size);
	
	/* Execute */
	if (operator->configure != NULL) {
		gpu_config_configureKernel (config, operator->configure, operator->args1, operator->args2);
	}
	gpu_config_submitKernel (config, threads, threadsPerGroup);

	/* Output */
	gpu_config_moveOutputBuffers (config, output_batches, addr_size);
	
	/* Clean up */
	gpu_config_flush (config);
	gpu_config_finish(config);

	if (event) { // With event, i.e. the last operator
		gpu_config_notifyEnd(config, operator->notifyEnd, event);
	}

#ifdef GPU_PROFILE
	/* Profiling */
	gpu_config_profileQuery (config);
#endif

	return 0;
}

static int gpu_query_exec_2 (
	gpu_query_p query, 
	size_t *threads, size_t *threadsPerGroup, 
	query_operator_p operator, 
	void ** input_batches, 
	void ** output_batches, size_t addr_size,
	query_event_p event) {
	
	/* The current config might still running, get another config */
	gpu_config_p config = gpu_switch_config (query);
	
	/* Queue this config into the pipeline end and save the pop out config */
	gpu_config_p out_config = (operator->execKernel(config));
	if (config == out_config) {
		fprintf(stderr, "error: invalid pipelined query context switch\n");
		exit (1);
	}

	/* Wait for the pop out config to finish */
	if (out_config) {

		/* Wait for the finish of the previous query in the out_config */
		// gpu_config_finish(out_config);
		
		/* Handle results */
		// if (query->handler) {
		// 	/* Configure and notify output result handler */
		// 	result_handler_readOutput (query->handler, query->qid, out_config, operator->readOutput);
		// } else {
		// 	/* Read output */
		// 	gpu_config_readOutput (out_config, operator->readOutput, query->qid);
		// }

		gpu_config_moveOutputBuffers (out_config, output_batches, addr_size);

		gpu_config_flush (out_config);

		if (event) { // With event, i.e. the last operator
			gpu_config_notifyEnd(config, operator->notifyEnd, event);
		}
	}
	
	/* We don't need writeInput */
	// gpu_config_writeInput (config, operator->writeInput, env, obj, q->qid);

	/* Begin to use this config to process data */

	gpu_config_moveInputBuffers (config, input_batches, addr_size);
	gpu_config_flush (config);
	
	if (operator->configure != NULL) {
		gpu_config_configureKernel (config, operator->configure, operator->args1, operator->args2);
	}
	
	gpu_config_submitKernel (config, threads, threadsPerGroup);
	
	// gpu_config_moveOutputBuffers (config, output_batches, addr_size);
	
	gpu_config_flush (config);
	
	/* Wait until read output from the swapped out query config has finished */
	if (out_config) {
		gpu_config_finish(out_config);

#ifdef GPU_PROFILE
		gpu_config_profileQuery (out_config);
#endif
	}

	return 0;
}

