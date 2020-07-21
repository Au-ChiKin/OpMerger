#ifndef __GPU_H_
#define __GPU_H_

#include "gpu_config.h"

typedef struct query_operator *queryOperatorP;
typedef struct query_operator {

	int  *args1;
	long *args2;

} query_operator_t;

// void gpu_init (JNIEnv *, int, int);
void gpu_init (char const * filename, int size, int kernel_num);

void gpu_set_kernel_sim(void const * data, void * result);

void gpu_exec_sim(void * result);

void gpu_free ();

#endif /* SEEP_GPU_H_ */
