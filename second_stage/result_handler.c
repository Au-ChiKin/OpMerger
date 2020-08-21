#include "result_handler.h"

#include <stdlib.h>
#include <stdio.h>


result_handler_p result_handler() {
	result_handler_p p = (result_handler_p) malloc(sizeof(result_handler_t));

	return p;
}

void result_handler_add_task (result_handler_p p, task_p t) {

}
