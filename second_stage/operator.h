#ifndef OPERATOR_H
#define OPERATOR_H

#include <stdbool.h>

#include "batch.h"

#define OPERATOR_CODE_FILENAME_LENGTH 256

enum operator_types {
    OPERATOR_SELECT,
    OPERATOR_REDUCE,
    OPERATOR_AGGREGATION
};

/* A collection of operator callbacks */
typedef struct opmerger_operator * operator_p;
typedef struct opmerger_operator {
    void (* setup) (void * operator, int batch_size);
    void (* process) (void * operator, batch_p input, int pane_size, bool is_range, batch_p output);
    void (* process_output) (void * operator, batch_p output);
    void (* reset_threads) (void * operator, int new_batch_size);
    void (* print) ();

    enum operator_types type;

    char code_name [OPERATOR_CODE_FILENAME_LENGTH];
} operator_t;

#endif