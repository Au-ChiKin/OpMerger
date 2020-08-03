#include "query.h"

#include <stdio.h>

#include "helpers.h"


query_p query(int id, int batch_size, int window_size, int window_side, bool is_merging) {
    query_p query = (query_p) malloc(sizeof(query_t));

    query->id = id;

    query->batch_size = batch_size;

    query->window_size = window_size;
    query->window_slide = window_side;
    query->window_type = COUNTER_BASE;

    query->pane_size = gcd(window_size, window_side);

    query->operator_num = 0;
    query->is_merging = is_merging;

    return query;
}

void query_add_operator(query_p query, void * new_operator, operator_p operator_callbacks) {
    if (query->operator_num >= QUERY_MAX_OPERATOR_NUM) {
        fprintf(stderr, "error: Exceed query maximum operator limit (%s)\n", __FUNCTION__);
        exit(1);
    } 
    query->operators[query->operator_num] = new_operator;
    query->callbacks[query->operator_num] = operator_callbacks;
    query->operator_num += 1;
}

void query_setup(query_p query) {
    if (query->operator_num == 0) {
        fprintf(stderr, "error: No operator has been added to this query (%s)\n", __FUNCTION__);
        exit(1);
    }

    /* TODO: there is no checking of whether the operators[i] matches the callbacks[i] */
    for (int i=0; i<query->operator_num; i++) {
        (* query->callbacks[i]->setup) (query->operators[i], query->batch_size);
    }
}

void query_process(query_p query, batch_p input, batch_p output) {
    if (query->operator_num == 0) {
        fprintf(stderr, "error: No operator has been added to this query (%s)\n", __FUNCTION__);
        exit(1);
    }

    if (input->size != query->batch_size) {
        fprintf(stderr, "error: input batch size (%d) does not match the set batch_size (%d) of the query (%s)\n", input->size, query->batch_size, __FUNCTION__);
        exit(1);        
    }

    /* TODO: there is no checking of whether the operators[i] matches the callbacks[i] */
    for (int i=0; i<query->operator_num; i++) {
        (* query->callbacks[i]->process) (query->id, query->operators[i], input, output);
    }


}