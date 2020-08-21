#include "query.h"

#include <stdio.h>
#include <time.h>

#include "helpers.h"

query_p query(int id, int batch_size, window_p window, bool is_merging) {
    query_p query = (query_p) malloc(sizeof(query_t));

    query->id = id;
    query->has_setup = false;

    query->batch_count = 0;
    query->batch_size = batch_size;

    query->window = window;

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
            if (op->type == OPERATOR_REDUCE) {
                if (i != query->operator_num-1) {
                    fprintf(stderr, "error: OpMerge currently does not support query with multiple pipeline \
                        breakers or with one which is not at the end\n (%s)", __FUNCTION__);
                    exit(1);
                }
            }
        }

        /* Merge operators apart from the last one and generate "patch" function */
        char patch_func[10*1024] = "";
        for (int i=0; i<query->operator_num - 1; i++) {
            (* query->callbacks[i]->generate_patch) (query->operators[i], patch_func);
        }

        /* Set up the last operator with the patch func */
        int last_idx = query->operator_num-1;
        (* query->callbacks[last_idx]->setup) (query->operators[last_idx], query->batch_size, query->window, patch_func);

        query->operators[0] = query->operators[last_idx];
        query->callbacks[0] = query->callbacks[last_idx];
        query->operator_num = 1;
    } else {
        /* If not merging, then set up the operator one by one */

        /* TODO: there is no checking of whether the operators[i] matches the callbacks[i] */
        for (int i=0; i<query->operator_num; i++) {
            (* query->callbacks[i]->setup) (query->operators[i], query->batch_size, query->window, NULL);
        }
    }

    query->has_setup = true;
}

void query_process(query_p query, int oid, batch_p input, batch_p output) {
    if (!query->has_setup) {

        fprintf(stderr, "error: This query has not been setup (%s)\n", __FUNCTION__);
        exit(1);
    }

    if (input->size != query->batch_size) {
        fprintf(stderr, "error: input batch size (%d) does not match the set batch_size (%d) of the query (%s)\n",
            input->size, query->batch_size, __FUNCTION__);
        exit(1);        
    }

    /* Execute */
    (* query->callbacks[oid]->reset) (query->operators[oid], input->size);

    (* query->callbacks[oid]->process) (query->operators[oid], 
        input,
        query->window,
        output,
        NULL); // No passing event
}

void query_process_output(query_p query, int oid, batch_p output) {
    (* query->callbacks[oid]->process_output) (query->operators[oid], output);
}

void query_free(query_p query) {
    free(query->window);
}