#ifndef __GPU_QUERY_H_
#define __GPU_QUERY_H_

#include "gpu_config.h"
#include "gpu_agg.h"
// #include "resulthandler.h"

typedef struct gpu_query *gpu_query_p;
typedef struct gpu_query {
	
	int qid;

	cl_device_id device;
	cl_context  context;
	cl_program  program;
	
	/* It seems that resultHandler is a thread created to monitor the performance of GPU but
	   it is only used when there is a join (i.e. the exce2 is used) */
	// resultHandlerP handler;

	int ndx; // which config this query is currently using
	gpu_config_p configs [NCONTEXTS]; // each config (i.e. context in Saber) has two command queues. 
	// Therefore when there are multiple configs, the higher layer can arrange (execute) the input and 
	// output while the other congfigs is processing the data. Because this prototype does not aim for
	// maximum efficiency, we could disable this functionality for now.

} gpu_query_t;

/* Constractor */
gpu_query_p gpu_query_new (int, cl_device_id, cl_context, const char *, int, int, int);

// void gpu_query_setResultHandler (gpu_query_p, resultHandlerP);

void gpu_query_free (gpu_query_p query);

/* Execute query in another context */
// gpu_config_p gpu_context_switch (gpu_query_p);

int gpu_query_setInput (gpu_query_p query, int input_id, int size);

int gpu_query_setOutput (gpu_query_p, int, int, int, int, int, int, int);

int gpu_query_setKernel (gpu_query_p,
		int,
		const char *,
		void (*callback)(cl_kernel, gpu_config_p, int *, long *),
		int *, long *);

/* Reset ndx kernel of this query */
int gpu_query_resetKernel (gpu_query_p,
		int ndx,
		const char *,
		void (*callback)(cl_kernel, gpu_config_p, int *, long *),
		int *, long *);

/* Process batch */
int gpu_query_exec (
	gpu_query_p, 
	size_t *, size_t *, 
	query_operator_p, 
	void ** input_batches, void ** output_batches, size_t addr_size);

#endif /* __GPU_QUERY_H_ */
