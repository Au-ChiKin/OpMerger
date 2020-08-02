#ifndef CONFIG_H
#define CONFIG_H

#define BUFFER_SIZE 16384 /* in tuple */
#define MAX_THREADS_PER_GROUP 256 /* Should be queried from the device */
#define PARTIAL_WINDOWS 1024 * 16 /* window limit in a batch */

/*
 * To add case:
 *     add one enum here and
 *     add coresponding string tag in set_test_case
 */
enum test_cases {
    CPU,
    MERGED_AGGREGATION, 
    REDUCTION,
    MERGED_SELECTION,
    SEPARATE_SELECTION,
    QUERY2,
    ERROR
};

void set_test_case(char const * mname, enum test_cases * mode);
void parse_arguments(int argc, char * argv[], enum test_cases * mode, int * work_load);

#endif // CONFIG_H