/* 
 * The main logic to run the experiment of merged operators
 */
#include "gpu.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "libcirbuf/circular_buffer.h"
#include "config.h"

void write_input_buffer(cbuf_handle_t buffer);

void run_processing_gpu(cbuf_handle_t buffer, int size, int * result, int load, enum test_cases mode) {
    input_t * batch = (input_t *) malloc(size * sizeof(tuple_t));

    switch (mode) {
        case MERGED_SELECT: 
            fprintf(stdout, "========== Running merged select test ===========\n");
            gpu_init("filters_merged.cl", size, 1); 
            break;
        case SEPARATE_SELECT: 
            fprintf(stdout, "========== Running sepaerate select test ===========\n");
            gpu_init("filters_separate.cl", size, 3); 
            break;
        default: 
            break; 
    }

    gpu_set_kernel();

    // TODO: cicular buffer should only return the pointer to the underline buffer
    circular_buf_read_bytes(buffer, batch->vectors, size * TUPLE_SIZE);
    for (int l=0; l<load; l++) {
        gpu_read_input(batch);
    
        int count = gpu_exec();

        gpu_write_output(result, count);

        printf("[GPU] Batch %d output size is: %d\n", l, count);
    }

    gpu_free();
}

void run_processing_cpu(cbuf_handle_t buffer, int size, tuple_t * result, int * output_size) {
    *output_size = 0;
    for (int i = 0; i < size; i++) {
        /* get one tuple */
        input_t * tuple = (input_t *) malloc(sizeof(input_t));
        circular_buf_read_bytes(buffer, tuple->vectors, TUPLE_SIZE);

        /* apply predicates */
        int value = 1;
	    int attribute_value = tuple->tuple.i1;
        printf("attr1: %d\n", attribute_value);
	    value = value & (attribute_value < VALUE_RANGE/2); /* if attribute < 50? */

	    attribute_value = tuple->tuple.i2;
        printf("attr2: %d\n", attribute_value);
	    value = value & (attribute_value != 0); /* if attribute != 0? */

	    attribute_value = tuple->tuple.i3;
        printf("attr3: %d\n", attribute_value);
	    value = value & (attribute_value >= VALUE_RANGE/4); /* if attribute > 25? */

        /* output the tuple if valid */
        if (value) {
            *output_size += 1;
            result[*output_size] = tuple->tuple;
        }
    }
}

int main(int argc, char * argv[]) {

    int work_load = 1; // default to be 1MB
    enum test_cases mode = MERGED_SELECT;

    parse_arguments(argc, argv, &mode, &work_load);

    uint8_t * buffer  = malloc(BUFFER_SIZE * TUPLE_SIZE * sizeof(uint8_t));
    cbuf_handle_t cbuf = circular_buf_init(buffer, BUFFER_SIZE * TUPLE_SIZE);
    write_input_buffer(cbuf);

    /* output the result size */
    /*
     * 32768 tuples in total
     * attr1 50% selectivity
     * attr2 50% selectivity
     * attr3 50% selectivity
     * 
     * output should be 32768 x (50%)^3 = 4096
     */
    int results_size = 0;
    tuple_t results_tuple[BUFFER_SIZE];

    if (mode == CPU) {
        run_processing_cpu(cbuf, BUFFER_SIZE, results_tuple, &results_size);
        
        printf("[CPU] The output from cpu is %d\n\n", results_size);
    } else {
        int results[BUFFER_SIZE];
        for (int i=0; i<BUFFER_SIZE; i++) {
            results[i] = 1;
        }

        run_processing_gpu(cbuf, BUFFER_SIZE, results, work_load, mode);
    }

    free(buffer);
    circular_buf_free(cbuf);

    return 0;
}

void write_input_buffer(cbuf_handle_t buffer) {
    input_t data;

    int value = 0;
    bool flipper = false;
    int cur = 0;
    while (cur < BUFFER_SIZE) {
        // #0
        data.tuple.time_stamp = 1;

        // #1
        data.tuple.i1 = value;

        // #2
        if (flipper) {
          data.tuple.i2 = 0;
        } else {
          data.tuple.i2 = 1;
        }
        flipper = !flipper;

        // #3
        data.tuple.i3 = value; 

        // #4 #5 #6
        data.tuple.i4 = 1;
        data.tuple.i5 = 1;
        data.tuple.i6 = 1;

        value = (value + 1) % VALUE_RANGE;
        cur += 1;

        // byte by byte
        circular_buf_put_bytes(buffer, data.vectors, TUPLE_SIZE);
    }
}

