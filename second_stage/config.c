#include"config.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

void parse_arguments(int argc, char * argv[], enum test_cases * mode, int * work_load) {
	extern char *optarg;
	extern int optind;
	int c, err = 0; 
    int debug = 0;
	int lflag=0, mflag=0;
	char *mname = "merged-aggregation";
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

void set_test_case(char const * mname, enum test_cases * mode) {
    if (strcmp(mname, "merged-aggregation") == 0) {
        *mode = MERGED_AGGREGATION; 
    } else if (strcmp(mname, "reduction") == 0) {
        *mode = MERGED_REDUCTION;
    } else if (strcmp(mname, "merged-selection") == 0) {
        *mode = MERGED_SELECTION;
    } else if (strcmp(mname, "separate-selection") == 0) {
        *mode = MERGED_SELECTION;
    } else if (strcmp(mname, "query2") == 0) {
        *mode = MERGED_SELECTION;
    } else if (strcmp(mname, "cpu") == 0) {
        *mode = CPU;
    } else {
        *mode = ERROR;
    }
    return;
}
