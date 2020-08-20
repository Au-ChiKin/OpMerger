/* Default query is:
*
* SELECT *
* FROM S1
* WHERE S1.1 < 128/2 AND S1.2 = 1 AND S1.3 >= 128/4
* 
* This attempt is to apply these predicates together
*/

#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics: enable
#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics: enable
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_int64_extended_atomics: enable
#pragma OPENCL EXTENSION cl_khr_fp64: enable

#pragma OPENCL EXTENSION cl_khr_byte_addressable_store: enable

#include "../clib/templates/byteorder.h"
#define INPUT_VECTOR_SIZE 2
#define OUTPUT_VECTOR_SIZE 2

typedef struct {
    long t;
    int _1;
    int _2;
    int _3;
    int _4;
    int _5;
    int _6;
} input_tuple_t __attribute__((aligned(1)));

typedef union {
    input_tuple_t tuple;
    uchar16 vectors[INPUT_VECTOR_SIZE];
} input_t;

typedef struct {
    long t;
    int _1;
    int _2;
    int _3;
    int _4;
    int _5;
    int _6;
} output_tuple_t __attribute__((aligned(1)));

typedef union {
    output_tuple_t tuple;
    uchar16 vectors[OUTPUT_VECTOR_SIZE];
} output_t;

/* Simplified version of merged select */
__kernel void selectf1_sim (__global input_t * input, __global int * num, __global int * output) {
    int gid = get_global_id(0);
    int value = 1;

    if (gid < 0 || gid >= *num) {
        // TODO: to exit properly
    } else {
        /* if attribute < 128/2? */
        int attribute_value = input[gid].tuple._1;
        value = value & (attribute_value < 128 / 2);

        /* if attribute != 0? */
        attribute_value = input[gid].tuple._2;
        value = value & (attribute_value != 0);

        /* if attribute >= 128/4? */
        attribute_value = input[gid].tuple._3;
        value = value & (attribute_value >= 128 / 4);

        output[gid] = value;
    }
}
