#include "gpu_query.h"

#include "../clib/openclerrorcode.h"
#include "../clib/debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include <unistd.h>
#include <sched.h>

// static int gpu_query_exec_1 (gpuQueryP, size_t *, size_t *, queryOperatorP, JNIEnv *, jobject); /* w/o  pipelining */
// static int gpu_query_exec_2 (gpuQueryP, size_t *, size_t *, queryOperatorP, JNIEnv *, jobject); /* with pipelining */

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
	fprintf(stderr, "%s (%zu chars)\n", msg, length);
	fflush (stderr);

	if (error != CL_SUCCESS) {
		fprintf(stderr, "opencl error (%d): %s\n", error, getErrorMessage(error));
		exit (1);
	}

	// query->handler = NULL;

	// query->ndx = -1;
	for (i = 0; i < NCONTEXTS; i++) {
		query->configs[i] = gpu_config(query->qid, query->device, query->context, query->program, _kernels, _inputs, _outputs);
	}

	return query;
}

// void gpu_query_setResultHandler (gpuQueryP q, resultHandlerP handler) {
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

// int gpu_query_setInput (gpuQueryP q, int ndx, int size) {
// 	if (! q)
// 		return -1;
// 	if (ndx < 0 || ndx > q->contexts[0]->kernelInput.count) {
// 		fprintf(stderr, "error: input buffer index [%d] out of bounds\n", ndx);
// 		exit (1);
// 	}
// 	int i;
// 	for (i = 0; i < NCONTEXTS; i++)
// 		gpu_context_setInput (q->contexts[i], ndx, size);
// 	return 0;
// }

// int gpu_query_setOutput (gpuQueryP q, int ndx, int size, int writeOnly, int doNotMove, int bearsMark, int readEvent, int ignoreMark) {
// 	if (! q)
// 		return -1;
// 	if (ndx < 0 || ndx > q->contexts[0]->kernelOutput.count) {
// 		fprintf(stderr, "error: output buffer index [%d] out of bounds\n", ndx);
// 		exit (1);
// 	}
// 	int i;
// 	for (i = 0; i < NCONTEXTS; i++)
// 		gpu_context_setOutput (q->contexts[i], ndx, size, writeOnly, doNotMove, bearsMark, readEvent, ignoreMark);
// 	return 0;
// }

// int gpu_query_setKernel (gpuQueryP q,
// 	int ndx,
// 	const char * name,
// 	void (*callback)(cl_kernel, gpuContextP, int *, long *),
// 	int *args1, long *args2) {

// 	if (! q)
// 		return -1;

// 	if (ndx < 0 || ndx > q->contexts[0]->kernel.count) {
// 		fprintf(stderr, "error: kernel index [%d] out of bounds\n", ndx);
// 		exit (1);
// 	}
// 	int i;
// 	for (i = 0; i < NCONTEXTS; i++) {
// 		gpu_context_setKernel (q->contexts[i], ndx, name, callback, args1, args2);
// 	}
// 	return 0;
// }

// gpuContextP gpu_context_switch (gpuQueryP p) {
// 	if (! p) {
// 		fprintf (stderr, "error: null query\n");
// 		return NULL;
// 	}
// #ifdef GPU_VERBOSE
// 	int current = (p->ndx) % NCONTEXTS;
// #endif
// 	int next = (++p->ndx) % NCONTEXTS;
// #ifdef GPU_VERBOSE
// 	if (current >= 0)
// 		dbg ("[DBG] switch from %d (%lld read(s), %lld write(s)) to context %d\n",
// 			current, p->contexts[current]->readCount, p->contexts[current]->writeCount, next);
// #endif
// 	return p->contexts[next];
// }

// int gpu_query_exec (gpuQueryP q, size_t *threads, size_t *threadsPerGroup, queryOperatorP operator, JNIEnv *env, jobject obj) {
	
// 	if (! q)
// 		return -1;

// 	if (NCONTEXTS == 1) {
// 		return gpu_query_exec_1 (q, threads, threadsPerGroup, operator, env, obj);
// 	} else {
// 		return gpu_query_exec_2 (q, threads, threadsPerGroup, operator, env, obj);
// 	}
// }

// /* */
// static int gpu_query_exec_1 (gpuQueryP q, size_t *threads, size_t *threadsPerGroup, queryOperatorP operator, JNIEnv *env, jobject obj) {
	
// 	gpuContextP p = gpu_context_switch (q);
	
// 	/* Write input */
// 	gpu_context_writeInput (p, operator->writeInput, env, obj, q->qid);

// 	gpu_context_moveInputBuffers (p);
	
// 	if (operator->configure != NULL) {
// 		gpu_context_configureKernel (p, operator->configure, operator->args1, operator->args2);
// 	}

// 	gpu_context_submitKernel (p, threads, threadsPerGroup);

// 	gpu_context_moveOutputBuffers (p);

// 	gpu_context_flush (p);
	
// 	gpu_context_finish(p);
	
// 	gpu_context_readOutput (p, operator->readOutput, env, obj, q->qid);

// 	return 0;
// }

// static int gpu_query_exec_2 (gpuQueryP q, size_t *threads, size_t *threadsPerGroup, queryOperatorP operator, JNIEnv *env, jobject obj) {
	
// 	gpuContextP p = gpu_context_switch (q);
	
// 	gpuContextP theOther = (operator->execKernel(p));
	
// 	if (p == theOther) {
// 		fprintf(stderr, "error: invalid pipelined query context switch\n");
// 		exit (1);
// 	}

// 	if (theOther) {

// 		/* Wait for read event from previous query */
// 		gpu_context_finish(theOther);
		
// #ifdef GPU_PROFILE
// 		gpu_context_profileQuery (theOther);
// #endif

// 		/* Configure and notify output result handler */
// 		if (q->handler) {
// 			result_handler_readOutput (q->handler, q->qid, theOther, operator->readOutput, obj);
// 		} else {
// 			/* Read output */
// 			gpu_context_readOutput (theOther, operator->readOutput, env, obj, q->qid);
// 		}
// 	}
	
// 	gpu_context_writeInput (p, operator->writeInput, env, obj, q->qid);

// 	gpu_context_moveInputBuffers (p);
	
// 	if (operator->configure != NULL) {
// 		gpu_context_configureKernel (p, operator->configure, operator->args1, operator->args2);
// 	}
	
// 	gpu_context_submitKernel (p, threads, threadsPerGroup);
	
// 	gpu_context_moveOutputBuffers (p);
	
// 	gpu_context_flush (p);
	
// 	/* Wait until read output from other query context has finished */
// 	if (theOther && q->handler) {
// 		result_handler_waitForReadEvent (q->handler);
// 	}

// 	return 0;
// }

