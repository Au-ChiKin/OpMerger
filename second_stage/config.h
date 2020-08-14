#ifndef CONFIG_H
#define CONFIG_H

#define MAX_SOURCE_LENGTH 100 * 1024

#define BUFFER_SIZE 16384 /* in tuple */
#define MAX_THREADS_PER_GROUP 256 /* Should be queried from the device */
#define PARTIAL_WINDOWS 1024 * 1024 /* window limit in a batch */

#undef OPMERGER_DEBUG
// #define OPMERGER_DEBUG

#include "stdbool.h"

/*
 * To add case:
 *     add one enum here and
 *     add coresponding string tag in set_test_case
 */
enum test_cases {
    CPU,
    MERGED_AGGREGATION, 
    REDUCTION,
    SELECTION,
    TWO_SELECTION,
    QUERY2,
    ERROR
};

void set_test_case(char const * mname, enum test_cases * mode);
void parse_arguments(int argc, char * argv[], enum test_cases * mode, int * work_load, int * buffer_num, bool * is_merging, bool * is_debug);

#endif // CONFIG_H