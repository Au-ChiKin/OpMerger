#include"config.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

void parse_arguments(int argc, char * argv[], 
    enum test_cases * mode, int * work_load, int * batch_size, int * buffer_num, int * pipeline_num, bool * is_merging, bool * is_debug) {

	extern char *optarg;
	extern int optind;
	int c, err = 0; 
    int debug = 0;
	int lflag=0, mflag=0, fflag=0, iflag=0; /* f --> fused */
	char *mname = "merged-aggregation";
	static char usage[] = "usage: %s [-d] -m test-case [-i input-buffers-to-read] [-l work-load-in-bytes] [-b batch-size-in-bytes] [-f]\n";

	while ((c = getopt(argc, argv, "dm:l:fi:b:p:")) != -1) {
		switch (c) {
            case 'd':
                // debug = 1;
                *is_debug = true;
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
            case 'b':
                *batch_size = atoi(optarg);
                break;
            case 'i':
                iflag = 1;
                *buffer_num = atoi(optarg);
                break;
            case 'p':
                *pipeline_num = atoi(optarg);
                break;
            case 'f':
                fflag = 1;
                *is_merging = true;
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
        printf("fflag = %d\n", fflag);
        printf("iflag = %d\n", fflag);
        
        if (optind < argc)	/* these are the arguments after the command-line options */
            for (; optind < argc; optind++)
                printf("argument: \"%s\"\n", argv[optind]);
        else {
            printf("no arguments left to process\n");
        }
    }
}

void set_test_case(char const * mname, enum test_cases * mode) {
    if (strcmp(mname, "aggregation") == 0) {
        *mode = AGGREGATION; 
    } else if (strcmp(mname, "reduction") == 0) {
        *mode = REDUCTION;
    } else if (strcmp(mname, "selection") == 0) {
        *mode = SELECTION;
    } else if (strcmp(mname, "two-selection") == 0) {
        *mode = TWO_SELECTION;
    } else if (strcmp(mname, "query2") == 0) {
        *mode = QUERY2;
    } else if (strcmp(mname, "cpu") == 0) {
        *mode = CPU;
    } else if (strcmp(mname, "query1") == 0) {
        *mode = QUERY1;
    } else {
        *mode = ERROR;
    }
    return;
}
