#ifndef OPERATOR_H
#define OPERATOR_H

#include <stdbool.h>

#include "batch.h"
#include "window.h"
#include "monitor/event_manager.h"

#define OPERATOR_CODE_FILENAME_LENGTH 256

enum operator_types {
    OPERATOR_SELECT,
    OPERATOR_REDUCE,
    OPERATOR_AGGREGATE
};

/* A collection of operator callbacks */
typedef struct opmerger_operator * operator_p;
typedef struct opmerger_operator {
    void (* setup) (void * operator, int batch_size, window_p window, char const * patch);
    void (* process) (void * operator, batch_p input, window_p window, u_int8_t ** processed_output, query_event_p event);
    void (* process_output) (void * operator, batch_p output);
    void (* reset) (void * operator, int new_batch_size);
    void (* generate_patch) (void * operator, char * patch);
    int (* get_output_schema_size) (void * operator);
    u_int8_t ** (* get_output_buffer) (void * operator, batch_p output);

    enum operator_types type;

    char code_name [OPERATOR_CODE_FILENAME_LENGTH];
} operator_t;

#endif
