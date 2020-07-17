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

#define BUFFER_SIZE 32768 /* in tuple */
#define TUPLE_SIZE 32
#define VALUE_RANGE 128

enum test_cases { 
    MERGED_SELECT, 
    SEPARATE_SELECT,
    ERROR
};

void set_test_case(char const * mname, enum test_cases * mode);
void parse_arguments(int argc, char * argv[], enum test_cases * mode);

void run_processing_gpu(tuple_t * buffer, int size, int * result, int * output_size, enum test_cases mode) {
    switch (mode) {
        case MERGED_SELECT: 
            fprintf(stdout, "========== Running merged select test ===========\n");
            gpu_init("filters_merged.cl", BUFFER_SIZE, 1); 
            break;
        case SEPARATE_SELECT: 
            fprintf(stdout, "========== Running sepaerate select test ===========\n");
            gpu_init("filters_separate.cl", BUFFER_SIZE, 3); 
            break;
        default: 
            break; 
    }

    // gpu_set_kernel_sim(buffer, result);

    // gpu_exec_sim(result);

    gpu_set_kernel(buffer, result);
    
    // gpu_exec(result);

    *output_size = size;
    for (int i=0; i<size; i++) {
        if (!result[i]) {
            *output_size -= 1;
        }
    }

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

int main(int argc, char * argv[]) {

    enum test_cases mode = MERGED_SELECT;

    parse_arguments(argc, argv, &mode);

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

        value = (value + 1) % VALUE_RANGE;
        cur += 1;
    }

    
    int results_size = 0;
    tuple_t results_tuple[BUFFER_SIZE];
    run_processing_cpu(buffer, BUFFER_SIZE, results_tuple, &results_size);

    /* output the result size */
    /*
     * 32768 tuples in total
     * attr1 50% selectivity
     * attr2 50% selectivity
     * attr3 50% selectivity
     * 
     * output should be 32768 x (50%)^3 = 4096
     */
    printf("[CPU] The output from cpu is %d\n\n", results_size);


    int results[BUFFER_SIZE];
    for (int i=0; i<BUFFER_SIZE; i++) {
        results[i] = 1;
    }
    results_size = 0; /* start from begining for GPU */
    run_processing_gpu(buffer, BUFFER_SIZE, results, &results_size, mode);

    printf("[GPU] The output from gpu is %d\n", results_size);

    return 0;
}

void set_test_case(char const * mname, enum test_cases * mode) {
    if (strcmp(mname, "merged-select") == 0) {
        *mode = MERGED_SELECT; 
    } else if (strcmp(mname, "separate-select") == 0) {
        *mode = SEPARATE_SELECT;
    } else {
        *mode = ERROR;
    }
    return;
}

void parse_arguments(int argc, char * argv[], enum test_cases * mode) {
	extern char *optarg;
	extern int optind;
	int c, err = 0; 
    int debug = 0;
	int mflag=0;
	char *mname = "merged-select";
	static char usage[] = "usage: %s [-d] -m test-case\n";

	while ((c = getopt(argc, argv, "dm:")) != -1) {
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
