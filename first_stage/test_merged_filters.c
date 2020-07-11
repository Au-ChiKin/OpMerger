/* 
 * The main logic to run the experiment of seperated operators
 */
#include "gpu.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#define BUFFER_SIZE 32768 /* in tuple */
#define VALUE_RANGE 128

void run_processing_gpu(tuple_t * buffer, int size, tuple_t * result, int * output_size) {
    gpu_init("filters_merged.cl");

    gpu_free();
}

void run_processing_cpu(tuple_t * buffer, int size, tuple_t * result, int * output_size) {
    /* if not using gpu */
    *output_size = 0;
    for (int i = 0; i < size; i++) {
        /* get one tuple */
        tuple_t * tuple = buffer + i;

        /* apply predicates */
        int value = 1;
	    int attribute_value = tuple->i1;
	    value = value & (attribute_value < VALUE_RANGE/2); /* if attribute < 50? */

	    attribute_value = tuple->i2;
	    value = value & (attribute_value != 0); /* if attribute != 0? */

	    attribute_value = tuple->i3;
	    value = value & (attribute_value >= VALUE_RANGE/4); /* if attribute > 25? */

        /* output the tuple if valid */
        if (value) {
            *output_size += 1;
            result[*output_size] = *tuple;
        }
    }
}

int main() {

    // TODO: Create a buffer of tuples with a circular buffer
    // 32768 x 32 = 1MB
    tuple_t buffer[BUFFER_SIZE];

    // TODO: Fill the buffer according to parameters
    int value = 0;
    bool flipper = false;
    int cur = 0;
    while (cur < BUFFER_SIZE) {
        // #0
        buffer[cur].time_stamp = 1;

        // #1
        buffer[cur].i1 = value;
        value = (value + 1) % VALUE_RANGE;

        // #2
        if (flipper) {
          buffer[cur].i2 = 0;
        } else {
          buffer[cur].i2 = 1;
        }
        flipper = !flipper;

        // #3
        buffer[cur].i3 = value; 

        // #4 #5 #6
        buffer[cur].i4 = 1;
        buffer[cur].i5 = 1;
        buffer[cur].i6 = 1;

        cur += 1;
    }

    // TODO: Start processing
    tuple_t results[BUFFER_SIZE];
    int results_size = 0;
    run_processing_cpu(buffer, BUFFER_SIZE, results, &results_size);

    /* output the result size */
    /*
     * 32768 tuples in total
     * attr1 50% selectivity
     * attr2 50% selectivity
     * attr3 50% selectivity
     * 
     * output should be 32768 x (50%)^3 = 4096
     */
    printf("The output from cpu is %d\n", results_size);

    return 0;
}