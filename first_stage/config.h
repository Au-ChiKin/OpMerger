#ifndef CONFIG_H
#define CONFIG_H

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

#endif // CONFIG_H