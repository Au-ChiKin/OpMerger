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

void write_input_buffer(cbuf_handle_t buffer, int batch_size);

void run_processing_gpu(cbuf_handle_t buffer, int size, tuple_t * result, int load, enum test_cases mode) {
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
    long start_time = 0;
    long end_time = 0;
    long start, end;
    long read_time = 0, write_time = 0, inter_time = 0;
    circular_buf_read_bytes(buffer, batch->vectors, size * TUPLE_SIZE);
    for (int l=0; l<load; l++) {
        gpu_read_input(batch, true, &start, &end);
        if (l == 0) {
            start_time = start;
        }
        read_time += end - start;
    
        bool is_profiling = true;
        int count = gpu_exec(is_profiling, &inter_time);

        gpu_write_output(result, count, true, &start, &end);
        if (l == load-1) {
            end_time = end;
        }
        write_time += end - start;

        dbg("[GPU] Batch %d output size is: %d\n", l, count);
    }
    printf("Total time consumption is: %ld ms\n", (end_time - start_time) / 1000000);
    printf("Read time consumption is: %ld ms\n", read_time / 1000000);
    printf("Write time consumption is: %ld ms\n", write_time / 1000000);
    printf("Inter-query copy time consumption is: %ld ms\n", inter_time / 1000000);

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

void write_input_buffer(cbuf_handle_t buffer, int batch_size) {
    input_t data;

    int value = 0;
    bool flipper = false;
    int cur = 0;
    while (cur < batch_size) {
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

int main(int argc, char * argv[]) {

    int work_load = 32; // default to be 32 MB
    int batch_size = 32; // default to be 32MB per batch
    enum test_cases mode = MERGED_SELECT;

    parse_arguments(argc, argv, &mode, &work_load, &batch_size);

    if (work_load < batch_size) {
        printf("Reset batch size to be %d\n", work_load);
        batch_size = work_load;
        work_load = 1;
    } else {
        if (work_load % batch_size != 0) {
            fprintf(stderr, "error: workload shoud be muiltiple of %d if >= %d\n",
                batch_size, batch_size);
            exit(1);
        }
        work_load /= batch_size;
    }
    /* convert into in tuples */
    batch_size *= ((1024 * 1024) / TUPLE_SIZE); 

    uint8_t * buffer  = (uint8_t *) malloc(batch_size * TUPLE_SIZE * sizeof(uint8_t));
    if (!buffer) {
        fprintf(stderr, "error: out of memory\n");
        exit(1);
    }
    cbuf_handle_t cbuf = circular_buf_init(buffer, batch_size * TUPLE_SIZE);
    // fprintf(stderr, "stll\n");
    write_input_buffer(cbuf, batch_size);

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
    tuple_t * results_tuple = (tuple_t *) malloc(batch_size * sizeof(tuple_t));

    if (mode == CPU) {
        run_processing_cpu(cbuf, batch_size, results_tuple, &results_size);
        
        printf("[CPU] The output from cpu is %d\n\n", results_size);
    } else {
        run_processing_gpu(cbuf, batch_size, results_tuple, work_load, mode);
    }

    free(buffer);
    free(results_tuple);
    circular_buf_free(cbuf);

    return 0;
}


