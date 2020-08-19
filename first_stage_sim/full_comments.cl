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


/* Full select */
inline int selectf1 (__global input_t *input) {
    int value = 1;
    /* if attribute < 128/2? */
    int attribute_value = input->tuple._1;
    value = value & (attribute_value < 128 / 2);

    /* if attribute != 0? */
    attribute_value = input->tuple._2;
    value = value & (attribute_value != 0);

    /* if attribute >= 128/4? */
    attribute_value = input->tuple._3;
    value = value & (attribute_value >= 128 / 4);

    return value;
}

/* studs */
inline int selectf2 (__global input_t * input) {return 1;}
inline int selectf3 (__global input_t * input) {return 1;}

/* Scan based on the implementation of [...] */

/*
 * Up-sweep (reduce) on a local array `data` of length `length`.
 * `length` must be a power of two.
 */
inline void upsweep (__local int *data, int length) {

    int lid  = get_local_id (0); /* local thread id */
    int b = (lid * 2) + 1; /* pointer to the right tuple of the current thread */
    int depth = 1 + (int) log2 ((float) length); /* how many digits of the length in binary takes up */

    for (int d = 0; d < depth; d++) {
        /* when d = depth - 1, mask = 2 ^ (depth - 1) - 1 = length - 1 but in a work group there are only
            length / 2 threads so actually d = depth - 1 makes no influence */

        barrier(CLK_LOCAL_MEM_FENCE); /* wait for all the other local threads */
        int mask = (0x1 << d) - 1; /* recalling the mask in Saber java part, a way of doing mod 
                                        mask equals to 0, 1, 3, ... , (2^(depth - 1) - 1) i.e. & mask = % 
                                        1, 2, 4, ..., (2^(depth - 1) - 1) */
        if ((lid & mask) == mask) { /* if the current thread id equals to the mask, lid = 
                                        (2^(depth - 2) - 1) to (2 ^ 0 - 1) i.e. (length / 2 - 1), 
                                        ... , 3, 1, 0 
                    
                                        Note that (2^(depth - 1) - 1) is theorectically legit but there 
                                        would not be such a thread in the group (only within 2^(depth - 2) = 
                                        length / 2)

                                        Also one lid can meet multiple masks e.g. 3 can meet 3 (%4), 1 (%2), 0 (%1)
                                        When mask == 0, any lid & mask == mask, so every lid is legit*/

            int offset = (0x1 << d); /* i.e. length = 2 ^ (depth - 1), 2 ^ (depth - 2), ... , 2, 1 */
            int a = b - offset;
            data[b] += data[a];
        }
    }
    /* Taking a legth = 16, depth = 5, lid = 0 to 7 case as example:
        d = 0 will have 0 1 2 3 4 5 6 7
        d = 1 will have 1 3 5 7
        d = 2 will have 3 7
        d = 3 will have 7
        d = 4 will have none

        so
        1st round: data[1] += data[0] data[3] += data[2]  data[5] += data[4] data[7] += data[6] data[9] += data[8] ... data[15] += data[14]
        2nd round:                    data[3] += data[1]                     data[7] += data[5]                    ... data[15] += data[13]
        3rd round:                                                           data[7] += data[3]                    ... data[15] += data[11]
        4th round:                                                                                                     data[15] += data[ 7]

        Therfore all data would "reduced" to the last data element
    */
    /* Actually quite apparently the folding upsweep mentioned in cuda file */
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

    /* Again depth = 4 and length = 16 so lid = 0 to 7 case as an example
        d = 4 will have none
        d = 3 will have 7
        d = 2 will have 3 7
        d = 1 will have 1 3 5 7
        d = 0 will have 0 1 2 3 4 5 6 7

        1st round: data[15] <-> data [7]
                    data[15] += data[7]
        2nd round: data[15] <-> data[11]    data[7] <-> data[3]
                    data[15] += data[11]     data[7] += data[3]
        3rd round: data[15] <-> data[13]    data[11] <-> data[9]    data[7] <-> data[5]    data[3] <-> data[1]
                    data[15] += data[13]     data[11] += data[9]     data[7] += data[5]     data[3] += data[1]
        4nd round: data[15] <-> data[14]    data[13] <-> data[12]   data[11] <-> data[10]  data[9] <-> data[8] ...
                    data[15] += data[14]     data[13] += data[12]    data[11] += data[10]   data[9] += data[8]  ...

        let say the data is directly coming from the upsweep
        then we would got (depicted as the original data index):
        0 + ... + 15
        0 + 0 + ... + 15
        0 + 1 + 0 + ... + 15
        ...
        0 + 1 + 2 + ... + 7 + 0 + ... + 15
        0 + ... + 15 + 0 + ... + 15
    */
}

/* Scan */
// inline void scan (__local int *data, int length) {

//     int lid = get_local_id (0);
//     int lane = (lid * 2) + 1;

//     upsweep (data, length);

//     if (lane == (length - 1))
//             data[lane] = 0;

//     downsweep (data, length);
//     return ;
// }

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

__kernel void selectKernel1 (
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
    flags[ left] = selectf1 (lp);
    flags[right] = selectf1 (rp);

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

__kernel void selectKernel2 (
    const int operator, /* Seems to be no use but keep it first */
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
    flags[ left] = selectf2 (lp);
    flags[right] = selectf2 (rp);

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

__kernel void selectKernel3 (
    const int operator, /* Seems to be no use but keep it first */
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
    flags[ left] = selectf3 (lp);
    flags[right] = selectf3 (rp);

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

            l_out->vectors[0] = l_in->vectors[0];
            l_out->vectors[1] = l_in->vectors[1];
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
