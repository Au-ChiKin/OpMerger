#include "query.h"

#include <stdio.h>

#include "helpers.h"
#include "selection.h"


query_p query(int id, int batch_size, int window_size, int window_side, bool is_merging) {
    query_p query = (query_p) malloc(sizeof(query_t));

    query->id = id;
    query->has_setup = false;

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

    if (!new_operator) {
        fprintf(stderr, "error: new_operator cannot be NULL (%s)\n", __FUNCTION__);
        exit(1);
    }

    query->operators[query->operator_num] = new_operator;
    query->callbacks[query->operator_num] = operator_callbacks;
    query->operator_num += 1;

    /* TODO: If new_operator is not the first operator, its input schema should be generated by 
       the last operator. For example, operator like projection would change the data schema and 
       therefore proces an output with different schema from the input data. However, since aggregation
       type of operator would be placed at the end of the operator queue, we could safely ignore them
       for now */
}

void query_setup(query_p query) {
    if (query->operator_num == 0) {
        fprintf(stderr, "error: No operator has been added to this query (%s)\n", __FUNCTION__);
        exit(1);
    }

    if (query->is_merging) {
        /* If merging the operator, all operators except for the last would be used to generated a patch 
           function. The patch function would then be inserted into the code generated by the last operator */

        /* Check whether pipeline break locates at the end of operator queue and whether there are 
           multiple pipeline breaker */
        for (int i=0; i<query->operator_num; i++) {
            operator_p op = query->callbacks[i];
            if (op->type == OPERATOR_SELECT) {
                if (i != query->operator_num-1) {
                    fprintf(stderr, "error: OpMerge currently does not support query with multiple pipeline breakers \
                        or with one which is not at the end\n (%s)", __FUNCTION__);
                    exit(1);
                }
            }
        }

        /* TODO: merging operators apart from the first one and generate "patch" function */
        char patch_func[10*1024];
        for (int i=0; i<query->operator_num - 1; i++) {
            
        }

        /* Set up the last operator with the patch func */
        int last_idx = query->operator_num-1;
        (* query->callbacks[last_idx]->setup) (query->operators[last_idx], query->batch_size);
    } else {
        /* If not merging, then set up the operator one by one */

        /* TODO: there is no checking of whether the operators[i] matches the callbacks[i] */
        for (int i=0; i<query->operator_num; i++) {
            (* query->callbacks[i]->setup) (query->operators[i], query->batch_size);
        }
    }

    query->has_setup = true;

}

void query_process(query_p query, batch_p input, batch_p output) {
    if (!query->has_setup) {

        fprintf(stderr, "error: This query has not been setup (%s)\n", __FUNCTION__);
        exit(1);
    }

    if (input->size != query->batch_size) {
        fprintf(stderr, "error: input batch size (%d) does not match the set batch_size (%d) of the query (%s)\n", input->size, query->batch_size, __FUNCTION__);
        exit(1);        
    }

    if (query->is_merging) {

    } else {
        /* Create an intermediate buffer for data from operator to operator */
        u_int8_t * inter_buffer = malloc(4 * query->batch_size * 64 /* TODO: a more reasonable way to define the size */);
        batch_t inter_batch;
        {
            inter_batch.start = 0;
            inter_batch.end = 4 * query->batch_size * 64;
            inter_batch.size = 0; /* output buffer, default to be empty */
            inter_batch.buffer = inter_buffer;
        }
        batch_p inter = &inter_batch;

        /* Another buffer for more than two operators */
        u_int8_t * inter_buffer_swap = malloc(4 * query->batch_size * 64);
        batch_t inter_batch_swap;
        {
            inter_batch_swap.start = 0;
            inter_batch_swap.end = 4 * query->batch_size * 64;
            inter_batch_swap.size = 0; /* output buffer, default to be empty */
            inter_batch_swap.buffer = inter_buffer_swap;
        }
        /* The buffer pointed by inter_swap should not be used (since it is also pointed to by input */
        batch_p inter_swap = &inter_batch_swap;

        batch_p tmp;

        /* TODO: there is no checking of whether the operators[i] matches the callbacks[i] */
        for (int i=0; i<query->operator_num; i++) {
            printf("[QUERY] Excecuting operator %d\n", i);
            if (i != query->operator_num-1) {
                (* query->callbacks[i]->reset_threads) (query->operators[i], input->size);

                (* query->callbacks[i]->process) (
                    query->operators[i], 
                    input, query->pane_size, query->window_type == RANGE_BASE, 
                    inter);

                /* Move the inter to input */
                (* query->callbacks[i]->process_output) (query->operators[i], inter);
                input = inter; /* Shallow copy. input batch does not belong to query class, no need to free */

                /* Use the other buffer as input */
                tmp = inter;
                { /* Reset the used input batch */
                    inter_swap->start = 0;
                    inter_swap->size = 0;
                }
                inter = inter_swap;
                inter_swap = tmp;
            } else  {
                (* query->callbacks[i]->reset_threads) (query->operators[i], input->size);

                (* query->callbacks[i]->process) (
                    query->operators[i], 
                    input, query->pane_size, query->window_type == RANGE_BASE,
                    output);
            }
        }
    }

}