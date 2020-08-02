#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics: enable
#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics: enable
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_int64_extended_atomics: enable
#pragma OPENCL EXTENSION cl_khr_fp64: enable

#pragma OPENCL EXTENSION cl_khr_byte_addressable_store: enable

#include "../clib/templates/byteorder.h"
#define INPUT_VECTOR_SIZE 4
#define OUTPUT_VECTOR_SIZE 4

typedef struct {
    long t;
    long _1;
    long _2;
    long _3;
    int _4;
    int _5;
    int _6;
    int _7;
    float _8;
    float _9;
    float _10;
    int _11;
} input_tuple_t __attribute__((aligned(1)));

typedef union {
    input_tuple_t tuple;
    uchar16 vectors[INPUT_VECTOR_SIZE];
} input_t;

typedef struct {
    long t;
    long _1;
    long _2;
    long _3;
    int _4;
    int _5;
    int _6;
    int _7;
    float _8;
    float _9;
    float _10;
    int _11;
} output_tuple_t __attribute__((aligned(1)));

typedef union {
    output_tuple_t tuple;
    uchar16 vectors[OUTPUT_VECTOR_SIZE];
} output_t;

// select condition
// Actually have been merged together
// What we need to do is to change this one
inline int selectf (__global input_t *p) {
    int value = 1;
    int attribute_value = p->tuple._7; /* where category == 1*/
    value = value & (attribute_value == 9); 
    return value;
}

// What is the purpose of them?
/* Scan based on the implementation of [...] */

/*
 * Up-sweep (reduce) on a local array `data` of length `length`.
 * `length` must be a power of two.
 */
inline void upsweep (__local int *data, int length) {

    int lid  = get_local_id (0);
    int b = (lid * 2) + 1;
    int depth = 1 + (int) log2 ((float) length);

    for (int d = 0; d < depth; d++) {

        barrier(CLK_LOCAL_MEM_FENCE);
        int mask = (0x1 << d) - 1;
        if ((lid & mask) == mask) {

            int offset = (0x1 << d);
            int a = b - offset;
            data[b] += data[a];
        }
    }
}

/*
 * Down-sweep on a local array `data` of length `length`.
 * `length` must be a power of two.
 */
inline void downsweep (__local int *data, int length) {

    int lid = get_local_id (0);
    int b = (lid * 2) + 1;
    int depth = (int) log2 ((float) length);
    for (int d = depth; d >= 0; d--) {

        barrier(CLK_LOCAL_MEM_FENCE);
        int mask = (0x1 << d) - 1;
        if ((lid & mask) == mask) {

            int offset = (0x1 << d);
            int a = b - offset;
            int t = data[a];
            data[a] = data[b];
            data[b] += t;
        }
    }
}

/*
 * Assumes:
 *
 * - N tuples
 * - Every thread handles two tuples, so #threads = N / 2
 * - L threads/group (expected or maximum number of threads in a group)
 * - Every thread group handles (2 * L) tuples
 * - Let, W be the number of groups
 * - N = 2L * W => W = N / 2L
 *
 */

__kernel void selectKernel (
    const int size, /* Seems to be no use but keep it first */
    const int tuples, /* Seems to be no use but keep it first */
    __global const uchar *input,
    __global int *flags, /* The output of select (0 or 1) */
    __global int *goffsets, /* If flags == 1, refers to the global offset of the corresponding tuple in output */
    __global int *partitions, /* Refers to the global block offset of the work group in output */
    __global uchar *output, /* Output tuples */
    __local  int *loffsets /* Refers to local offset of the corresponding tuples in output */
)
{
    int lgs = get_local_size (0); // local group size (thread number in the group)
    int tid = get_global_id (0);  // global thread id

    int  left = (2 * tid); // tuple1 id
    int right = (2 * tid) + 1; // tuple2 id

    int lid = get_local_id(0); // local thread id

    /* Local memory indices */
    int  _left = (2 * lid); // tuple1 memory indice on local memory
    int _right = (2 * lid) + 1; // tuple2 memory indice

    int gid = get_group_id (0); // group id
    /* A thread group processes twice as many tuples as the work items at the group */
    int l_tuple_num = 2 * lgs;

    /* Fetch tuple and apply selection filter */
    const int lidx =  left * sizeof(input_t);
    const int ridx = right * sizeof(input_t);

    // opencl syntax
    __global input_t *lp = (__global input_t *) &input[lidx];
    __global input_t *rp = (__global input_t *) &input[ridx];

    // apply predicates
    flags[ left] = selectf (lp);
    flags[right] = selectf (rp);

    /* Initialise the local offsets with flags */
    loffsets[ _left] = (left  < tuples) ? flags[ left] : 0; // left < tuples means not exceeding the total amount of tuples
    loffsets[_right] = (right < tuples) ? flags[right] : 0; // if exceeding -> 0 -> means not taking this tuple

    /* upsweep + reduce */
    upsweep(loffsets, l_tuple_num);

    // if this thread is the last in this group, pass the result of reduce (loffsets[_right]) to the partition array and 
    // set loffsets[_right] to 0
    if (lid == (lgs - 1)) { // _left = lid * 2 = 2 * (lgs - 1) = (2 * lgs - 1) - 1
        partitions[gid] = loffsets[_right];
        loffsets[_right] = 0; // So that in the downsweep, the last x[] element will be added to all the other elements
    }

    downsweep(loffsets, l_tuple_num);

    /* Write results to global memory */
    goffsets[ left] = ( left < tuples) ? loffsets[ _left] : -1;
    goffsets[right] = (right < tuples) ? loffsets[_right] : -1;
}


inline void compact_tuple(
    __global const uchar *input,
    __global int *goffsets,
    __global int *flags,
    __global uchar *output, 
    __local int *pivot,
    int tuple_id
) {
    /* Compact left and right */
    if (flags[tuple_id] == 1) {

        const int l_in_byte = tuple_id * sizeof(input_t); // left tuple of input memory location
        const int l_out_byte = (goffsets[tuple_id] + *pivot) * sizeof(output_t); // left tuple of output memory location
        flags[tuple_id] = l_out_byte + sizeof(output_t);
            __global  input_t *l_in  = (__global  input_t *) &  input[l_in_byte];
            __global output_t *l_out = (__global output_t *) & output[l_out_byte];

        for (int i=0; i<OUTPUT_VECTOR_SIZE; i++) {
            l_out->vectors[i] = l_in->vectors[i];
        }
    }   
}

__kernel void compactKernel (
    const int size, /* Seems to be no use but keep it first */
    const int tuples, /* Seems to be no use but keep it first */
    __global const uchar *input,
    __global int *flags,
    __global int *goffsets,
    __global int *partitions,
    __global uchar *output,
    __local  int *loffsets
) {

    int tid = get_global_id (0);

    int  left = (2 * tid);
    int right = (2 * tid) + 1;

    int lid = get_local_id (0);

    int gid = get_group_id (0);

    /* Compute pivot value */
    __local int pivot;
    if (lid == 0) { // if this the first thread in the group
        pivot = 0;
        if (gid > 0) {
            for (int i = 0; i < gid; i++) {
                pivot += (partitions[i]);
            }
        }
    }
    barrier(CLK_LOCAL_MEM_FENCE);

    /* Compact left and right */
    compact_tuple(input, goffsets, flags, output, &pivot, left);
    compact_tuple(input, goffsets, flags, output, &pivot, right);
}
