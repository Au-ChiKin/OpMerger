#ifndef QUERY_H
#define QUERY_H

#include <stdbool.h>

#include "batch.h"
#include "operator.h"

/* TODO to support more than one operator */
#define QUERY_MAX_OPERATOR_NUM 2

enum window_types {
    RANGE_BASE,  /* Time based */
    COUNTER_BASE
};

typedef struct query * query_p;
typedef struct query {
    int id;
    int batch_size;
    bool has_setup;

    int window_size;
    int window_slide;
    enum window_types window_type;

    int pane_size;

    int operator_num;
    void * operators[QUERY_MAX_OPERATOR_NUM];
    operator_p callbacks[QUERY_MAX_OPERATOR_NUM];
    bool is_merging;
} query_t;

query_p query(int id, int batch_size, int window_size, int window_side, bool is_merging);

void query_add_operator(query_p query, void * new_operator, operator_p operator_callbacks);

void query_setup(query_p query);

void query_process(query_p query, batch_p input, batch_p output);

#endif