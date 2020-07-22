/* 
 * The main logic to run the experiment of merged operators
 */
#include "gpu.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>

#include "libcirbuf/circular_buffer.h"

#define BUFFER_SIZE 32768 /* in tuple */
#define VALUE_RANGE 128

/*
 * To add case:
 *     add one enum here and
 *     add coresponding string tag in set_test_case
 */
enum test_cases {
    CPU,
    MERGED_SELECT, 
    SEPARATE_SELECT,
    ERROR
};

void set_test_case(char const * mname, enum test_cases * mode);
void parse_arguments(int argc, char * argv[], enum test_cases * mode, int * work_load);
void write_input_buffer(cbuf_handle_t buffer);

void circular_buf_put_bytes(cbuf_handle_t cbuf, uint8_t * data, int bytes);
int circular_buf_read_bytes(cbuf_handle_t cbuf, uint8_t * data, int bytes);

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
    
        gpu_exec();

        gpu_write_output(result);

        int output_size = size;
        for (int i=0; i<size; i++) {
            if (!result[i]) {
                output_size -= 1;
            }
        }
        // printf("Batch %d output size is: %d\n", l, output_size);
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

    // TODO: Create a buffer of tuples with a circular buffer
    // 32768 x 32 = 1MB
    // tuple_t buffer[BUFFER_SIZE];

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

        printf("[GPU] The output from gpu is %d\n", results_size);
    }

    free(buffer);
    circular_buf_free(cbuf);

    return 0;
}

// TODO: Fill the buffer according to parameters
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

void set_test_case(char const * mname, enum test_cases * mode) {
    if (strcmp(mname, "merged-select") == 0) {
        *mode = MERGED_SELECT; 
    } else if (strcmp(mname, "separate-select") == 0) {
        *mode = SEPARATE_SELECT;
    } else if (strcmp(mname, "cpu") == 0) {
        *mode = CPU;
    } else {
        *mode = ERROR;
    }
    return;
}

void parse_arguments(int argc, char * argv[], enum test_cases * mode, int * work_load) {
	extern char *optarg;
	extern int optind;
	int c, err = 0; 
    int debug = 0;
	int lflag=0, mflag=0;
	char *mname = "merged-select";
	static char usage[] = "usage: %s [-d] -m test-case [-l work-load-in-bytes]\n";

	while ((c = getopt(argc, argv, "dm:l:")) != -1) {
		switch (c) {
            case 'd':
                debug = 1;
                break;
            case 'm':
                mflag = 1;
                mname = optarg;
                set_test_case(mname, mode);
                if (*mode == ERROR) {
                    // TODO: Move this error detection to outsied of the loop
                    fprintf(stderr, "Mode \"%s\" has not yet been defined\n", mname);
                }
                break;
            case 'l':
                lflag = 1;
                *work_load = atoi(optarg);
                break;
            case '?':
                err = 1;
                break;
		}
    }
	if (mflag == 0) {	/* -m was mandatory */
		fprintf(stderr, "%s: missing -m option\n", argv[0]);
		fprintf(stderr, usage, argv[0]);
		exit(1);
	} else if ((optind) > argc) {	
		/* need at least one argument (change +1 to +2 for two, etc. as needeed) */

		fprintf(stderr, "optind = %d, argc = %d\n", optind, argc);
		fprintf(stderr, "%s: missing test case name\n", argv[0]);
		fprintf(stderr, usage, argv[0]);
		exit(1);
	} else if (err) {
		fprintf(stderr, usage, argv[0]);
		exit(1);
	}
    if (debug) {
        /* see what we have */
        printf("debug = %d\n", debug);
        printf("mflag = %d\n", mflag);
        
        if (optind < argc)	/* these are the arguments after the command-line options */
            for (; optind < argc; optind++)
                printf("argument: \"%s\"\n", argv[optind]);
        else {
            printf("no arguments left to process\n");
        }
    }
}

void circular_buf_put_bytes(cbuf_handle_t cbuf, uint8_t * data, int bytes) {

    for (int j=0; j<bytes; j++) {
        circular_buf_put(cbuf, data[j]);
    }
}

int circular_buf_read_bytes(cbuf_handle_t cbuf, uint8_t * data, int bytes) {
    int r = 0;

    for (int j=0; j<bytes; j++) {
        r = circular_buf_read(cbuf, &(data[j]));
        if (r == -1) {
            return r;
        }
    }

    return r;
}