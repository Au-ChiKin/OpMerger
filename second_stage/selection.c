#include "selection.h"

#include <stdio.h>
#include <string.h>

#include "helpers.h"
#include "libgpu/gpu_agg.h"
#include "config.h"
#include "generators.h"
#include "string.h"

#define _sprintf(format, __VA_ARGS__...) \
{\
    sprintf(s, format, __VA_ARGS__);\
    strcat(ret, s);\
}

static int free_id = 0;

ref_value_p ref_value() {

    ref_value_p value = (ref_value_p) malloc(sizeof(ref_value_t));
    value->i = NULL;
    value->l = NULL;
    value->f = NULL;
    value->c = NULL;

    return value;
}

void ref_value_free(ref_value_p value) {

    if (value) {
        if (value->i) {
            free(value->i);
        }
        if (value->l) {
            free(value->l);
        }
        if (value->f) {
            free(value->f);
        }
        if (value->c) {
            free(value->c);
        }
        free(value);
    }
} 

selection_p selection(
    schema_p input_schema, 
    int ref, ref_value_p value,
    enum comparor com) {

    selection_p p = (selection_p) malloc(sizeof(selection_t));
    p->operator = (operator_p) malloc(sizeof (operator_t));
    {
        p->operator->setup = selection_setup;
        p->operator->process = selection_process;
        p->operator->process_output = selection_process_output;
        p->operator->reset = selection_reset;
        p->operator->print = selection_print_output;

        p->operator->type = OPERATOR_SELECT;

        strcpy(p->operator->code_name, SELECTION_CODE_FILENAME);
    }

    p->id = free_id++;

    p->input_schema = input_schema;

    p->ref = ref;
    p->value = value;
    p->com = com;

    /* For selection, the input and output schema are the same */
    p->output_schema = input_schema;

    return p;
}

static char * generate_selecf(selection_p select) {
    char * ret = (char *) malloc(1024 * sizeof(char)); *ret = '\0';
    char s [128] = "";

    /* TODO: support multiple predicates */
    int const predicate_num = 1;

    _sprintf("inline int selectf (__global input_t *p) {\n", NULL);
    _sprintf("    int value = 1;\n", NULL);
    for (int i = 0; i < predicate_num; i++) {

        if (select->value->i != NULL) {
            _sprintf("    int attr_%d = p->tuple._%d;\n", select->id, select->ref);
        } else if (select->value->l != NULL) {
            _sprintf("    long attr_%d = p->tuple._%d;\n", select->id, select->ref);
        } else if (select->value->f != NULL) {
            _sprintf("    float attr_%d = p->tuple._%d;\n", select->id, select->ref);
        } else if (select->value->c != NULL) {
            _sprintf("    char attr_%d = p->tuple._%d;\n", select->id, select->ref);
        } else {
            exit(1);
        }
        
        /* Conditions */
        {
            _sprintf("    value = value & ", NULL);

            _sprintf("(attr_%d ", select->id);

            switch (select->com)
            {
            case GREATER:
                _sprintf(">", NULL);
                break;
            case EQUAL:
                _sprintf("==", NULL);
                break;
            case LESS:
                _sprintf("<", NULL);
                break;
            case GREATER_EQUAL:
                _sprintf(">=", NULL);
                break;
            case LESS_EQUAL:
                _sprintf("<=", NULL);
                break;
            case UNEQUAL:
                _sprintf("!=", NULL);
                break;
            default:
                break;
            }

            if (select->value->i != NULL) {
                _sprintf(" %d)", *(select->value->i));
            } else if (select->value->l != NULL) {
                _sprintf(" %ld)", *(select->value->l));
            } else if (select->value->f != NULL) {
                _sprintf(" %f)", *(select->value->f));
            } else if (select->value->c != NULL) {
                _sprintf(" %c)", *(select->value->c));
            } else {
                exit(1);
            }

            if (i == predicate_num - 1) {
                _sprintf(";\n", NULL);
            } else {
                _sprintf(" & ", NULL);
            }
        }
    }
    _sprintf("    return value;\n", NULL);

    _sprintf("}\n\n", NULL);

    return ret;    
}

static char * generate_source(selection_p select) {
    char * source = (char *) malloc(1024 * 10 * sizeof(char));

    char * extensions = read_file("cl/templates/extensions.cl");
    char * headers = read_file("cl/templates/headers.cl");

    /* Input and output vector sizes */
    char * tuple_size = generate_tuple_size(select->input_schema->size, select->output_schema->size, 16);

    /* Input and output tuple struct */
    char * input_tuple = generate_input_tuple(select->output_schema, NULL, 16);

    char * output_tuple = generate_output_tuple(select->output_schema, NULL, 16);

    /* TODO: generate according to select */
//     char selecf[1024] =
// "// select condition\n\
// inline int selectf (__global input_t *p) {\n\
//     /* r */ int value = 1;\n\
// \n\
//     int attribute_value = p->tuple._6; /* where category == 0*/\n\
//     value = value & (attribute_value == 0);\n\
// \n\
//     /* r */ return value;\n\
// }\n";
    char * selecf = generate_selecf(select);

    /* Template funcitons */
    char * template = read_file(SELECTION_CODE_TEMPLATE);

    /* Assembling */
    strcpy(source, extensions);

    strcat(source, headers);

    strcat(source, tuple_size);
    
    strcat(source, input_tuple);
    strcat(source, output_tuple);
    
    strcat(source, selecf);
    
    strcat(source, template);

    free(extensions);
    free(headers);
    free(tuple_size);
    free(input_tuple);
    free(output_tuple);
    free(selecf);
    free(template);

    return source;
}

void selection_setup(void * select_ptr, int batch_size, window_p window) {
    selection_p select = (selection_p) select_ptr;

    int tuple_size = select->input_schema->size;

    /* Operator setup */
    for (int i=0; i<SELECTION_KERNEL_NUM; i++) {
        select->threads[i] = batch_size / SELECTION_TUPLES_PER_THREADS;

        if (select->threads[i] < MAX_THREADS_PER_GROUP) {
            select->threads_per_group[i] = select->threads[0];
        } else {
            select->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Since the outputs have been set at this stage, their size in libgpu cannot be alterred afterwards(during 
       processing, we might want to change to number of threads used to compute according to the input batch).
       Therefore, we need to calculate these entries here instead of when needed. Otherwise they might change
       according to the new settings and do not match up the libgpu settings */
    select->output_entries[0] = 0;
    select->output_entries[1] = 4 * batch_size;
    select->output_entries[2] = 4 * batch_size + 4 * work_group_num;

    /* Source generation */
    char * source = generate_source(select);

    /* Output generated code */
    char * filename = generate_filename(select->id, SELECTION_CODE_FILENAME);
    printf("[SELECTION] Printing the generated source code to file: %s\n", filename);
    print_to_file(filename, source);

    /* Build opencl program */
    int qid = gpu_get_query(source, 2, 1, 4);
    select->qid = qid;
    
    /* GPU inputs and outputs setup */
    gpu_set_input(qid, 0, batch_size * tuple_size);
    
    gpu_set_output (qid, 0, 4 * batch_size,           0, 1, 1, 0, 1); /*      Flags */
    gpu_set_output (qid, 1, 4 * batch_size,           0, 1, 0, 0, 1); /*    Offsets */
    gpu_set_output (qid, 2, 4 * work_group_num,       0, 0, 0, 0, 1); /* Partitions */
    gpu_set_output (qid, 3, batch_size * tuple_size,  1, 0, 0, 1, 0); /*    Results */
    
    /* GPU kernels setup */
    int args[3];
    args[0] = batch_size * tuple_size;
    args[1] = batch_size;
    args[2] = 4 * select->threads_per_group[0] * SELECTION_TUPLES_PER_THREADS;

    gpu_set_kernel_select (qid, args);

    /* Free the inputed files */
    free(source);
}

void selection_reset(void * select_ptr, int new_batch_size) {
    selection_p select = (selection_p) select_ptr;

    for (int i=0; i<SELECTION_KERNEL_NUM; i++) {
        select->threads[i] = new_batch_size / SELECTION_TUPLES_PER_THREADS;

        if (select->threads[i] < MAX_THREADS_PER_GROUP) {
            select->threads_per_group[i] = select->threads[0];
        } else {
            select->threads_per_group[i] = MAX_THREADS_PER_GROUP;
        }
    }
}

void selection_process(void * select_ptr, batch_p input, int pane_size, bool is_range, batch_p output) {
    selection_p select = (selection_p) select_ptr;

    int batch_size = input->size;
    int tuple_size = select->input_schema->size;
    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Set input buffer addresses and entries */
    u_int8_t * inputs [1] = {input->buffer + input->start};

    /* Set output buffer addresses and entries */
    u_int8_t * outputs [3] = {
        output->buffer + output->start + select->output_entries[0],  /* flags */
        output->buffer + output->start + select->output_entries[1],  /* partitions */
        output->buffer + output->start + select->output_entries[2]}; /* output */

    /* Validate whether the given output buffer is big enough */
    if ((output->end - output->start) < (long) (4 * batch_size + 4 * work_group_num + batch_size * tuple_size)) {
        fprintf(stderr, "error: Expected output size has exceeded the given output buffer size (%s)\n", __FUNCTION__);
        exit(1);
    }

    /* Execute */
    gpu_execute(select->qid, select->threads, select->threads_per_group,
        (void *) inputs, (void *) outputs, sizeof(u_int8_t));
}

void selection_print_output(selection_p select, batch_p outputs) {

    int work_group_num = select->threads[0] / select->threads_per_group[0];

    typedef struct {
        long t;
        long _1;
        long _2;
        long _3;
        int _4;
        int _5;
        int _6;
        int _7;
        float _8;
        float _9;
        float _10;
        int _11;
    } output_tuple_t __attribute__((aligned(1)));

    /* Deserialise output buffer */
    int * flags = (int *) (outputs->buffer + outputs->start + select->output_entries[0]);

    int * partitions = (int *) (outputs->buffer + outputs->start + select->output_entries[1]);

    output_tuple_t * output = (output_tuple_t *) (outputs->buffer + outputs->start + select->output_entries[2]);

    /* Calculate output tuples */
    int count = 0;
    for (int i=0; i<work_group_num; i++) {
        count += partitions[i];
    }

    /* print */
    printf("[Results] Output tuple numbers: %d\n", count);
    printf("[Results] Tuple    Timestamp    user-id     event-type    category    priority    cpu\n");
    for (int i=0; i<10; i++) {
        printf("[Results] %-8d %-12ld %-11d %-13d %-11d %-11d %-6.5f\n", 
            i, output->t, output->_4, output->_5, output->_6, output->_7, output->_8);
        output += 1;
    }
}

void selection_process_output (void * select_ptr, batch_p outputs) {
    printf("[SELECTION] Processing output\n");

    selection_p select = (selection_p) select_ptr;

    int work_group_num = select->threads[0] / select->threads_per_group[0];

    /* Calculate output tuples */
    int * partitions = (int *) (outputs->buffer + select->output_entries[1]);
    int count = 0;
    for (int i=0; i<work_group_num; i++) {
        count += partitions[i];
    }
    printf("[SELECTION] This output has tuples %d\n", count);
    /* TODO: replace it with the least power of 2 number that bigger than count */
    // if ()

    /* TODO: initialise the extra tuples */

    /* Update pointers to use only the output array (tuples) and exclude flags and partitions */
    outputs->start += select->output_entries[2];

    // outputs->size = count;
    if (count > 4096) {
        outputs->size = 4096;
    } else if (count > 2048) {
        outputs->size = 2048;
    } else if (count > 1024) {
        outputs->size = 1024;
    } else if (count > 512) {
        outputs->size = 512;
    } else if (count > 256) {
        outputs->size = 256;
    } else if (count > 128) {
        outputs->size = 128;
    } else if (count > 64) {
        outputs->size = 64;
    }
}
