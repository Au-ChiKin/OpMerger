#ifndef __RESULT_HANDLER_H_
#define __RESULT_HANDLER_H_

#include "gpu_config.h"

#include <pthread.h>

typedef struct result_handler *resultHandlerP;
typedef struct result_handler {
	int qid;
	gpu_config_p context;
	void (*readOutput) (gpu_config_p, int, int, int);

	pthread_mutex_t *mutex;
	pthread_cond_t *reading, *waiting;

	volatile unsigned count;
	volatile unsigned start;

} result_handler_t;

resultHandlerP result_handler_init ();

void result_handler_readOutput (resultHandlerP, int, gpu_config_p,
		void (*callback)(gpu_config_p, int, int, int));

void result_handler_waitForReadEvent (resultHandlerP);

#endif /* __RESULT_HANDLER_H_ */
