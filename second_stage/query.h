#ifndef QUERY_H
#define QUERY_H

#include <stdbool.h>

#include "batch.h"
#include "window.h"
#include "operators/operator.h"

/* TODO to support more than one operator */
#define QUERY_MAX_OPERATOR_NUM 2

typedef struct query * query_p;
typedef struct query {
    int id;
    int batch_size;
    int batch_count;
    bool has_setup;

    window_p window;

    int operator_num;
    void * operators[QUERY_MAX_OPERATOR_NUM];
    operator_p callbacks[QUERY_MAX_OPERATOR_NUM];
    bool is_merging;
} query_t;

query_p query(int id, int batch_size, window_p window, bool is_merging);

void query_add_operator(query_p query, void * new_operator, operator_p operator_callbacks);

void query_setup(query_p query);

void query_process(query_p query, int oid, batch_p input, batch_p output);

void query_free(query_p query);

void query_process_output(query_p query, int oid, batch_p output);

int query_get_operator_num(query_p query);

#endif