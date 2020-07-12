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

	int ndx;
	gpu_config_p contexts [NCONTEXTS];

} gpu_query_t;

/* Constractor */
// gpuQueryP gpu_query_new (int, cl_device_id, cl_context, const char *, int, int, int);

// void gpu_query_init (gpuQueryP, JNIEnv *, int);

// void gpu_query_setResultHandler (gpuQueryP, resultHandlerP);

// void gpu_query_free (gpuQueryP);

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
