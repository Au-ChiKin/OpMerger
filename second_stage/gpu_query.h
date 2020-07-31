#ifndef __GPU_QUERY_H_
#define __GPU_QUERY_H_

#include "gpu_config.h"

#include "gpu.h"

typedef struct gpu_query *gpu_query_p;
typedef struct gpu_query {
	
	int qid;

	cl_device_id device;
	cl_context  context;
	cl_program  program;
	
	// resultHandlerP handler;

	// int ndx; // TODO figure out what is the funcitonality of it
	gpu_config_p configs [NCONTEXTS]; /* keeps the configuration for a device that runs this query */

} gpu_query_t;

/* Constractor */
gpu_query_p gpu_query_new (int, cl_device_id, cl_context, const char *, int, int, int);

// void gpu_query_init (gpuQueryP, JNIEnv *, int);

// void gpu_query_setResultHandler (gpuQueryP, resultHandlerP);

void gpu_query_free (gpu_query_p query);

/* Execute query in another context */
// gpu_config_p gpu_context_switch (gpuQueryP);

// int gpu_query_setInput (gpuQueryP, int, int);

// int gpu_query_setOutput (gpuQueryP, int, int, int, int, int, int, int);

// int gpu_query_setKernel (gpuQueryP,
// 		int,
// 		const char *,
// 		void (*callback)(cl_kernel, gpu_config_p, int *, long *),
// 		int *, long *);

/* Execute task */
// int gpu_query_exec (gpuQueryP, size_t *, size_t *, queryOperatorP, JNIEnv *, jobject);

#endif /* __GPU_QUERY_H_ */
