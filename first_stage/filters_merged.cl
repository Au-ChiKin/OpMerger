/* Default query is:
*
* SELECT *
* FROM S1
* WHERE S1.1 < 50 AND S1.2 = 1 AND S1.3 >= 25
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

// input schema
typedef struct {
	long t;
	int _1;
	int _2;
	int _3;
	int _4;
	int _5;
	int _6;
} input_tuple_t __attribute__((aligned(1)));

// input batch
typedef union {
	input_tuple_t tuple;
	uchar16 vectors[INPUT_VECTOR_SIZE];
} input_t;

// output schema
typedef struct {
	long t;
	int _1;
	int _2;
	int _3;
	int _4;
	int _5;
	int _6;
} output_tuple_t __attribute__((aligned(1)));

// output batch
typedef union {
	output_tuple_t tuple;
	uchar16 vectors[OUTPUT_VECTOR_SIZE];
} output_t;

// predicates
inline int selectf (__global input_t *p) {
	int value = 1;
	int attribute_value = __bswap32(p->tuple._1);
	value = value & (attribute_value < 50); // if attribute < 50?

	attribute_value = __bswap32(p->tuple._2);
	value = value & (attribute_value != 0); // if attribute != 0?

	attribute_value = __bswap32(p->tuple._3);
	value = value & (attribute_value >= 25); // if attribute >= 25?

	return value;
}

__kernel void selectf_sim (__global input_t *p, __global int * flags, __global int * num, __global int * output) {
        int value = 1;
	int attribute_value = __bswap32(p->tuple._1);
	value = value & (attribute_value < 50); // if attribute < 50?

	attribute_value = __bswap32(p->tuple._2);
	value = value & (attribute_value != 0); // if attribute != 0?

	attribute_value = __bswap32(p->tuple._3);
	value = value & (attribute_value >= 25); // if attribute >= 25?
}

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
        barrier(CLK_LOCAL_MEM_FENCE);int b = (lid * 2) + 1;
        int depth = (int) log2 ((float) length); /* Without plus 1 */
        for (int d = depth; d >= 0; d--) { /* from depth to 0 */

                barrier(CLK_LOCAL_MEM_FENCE); /* wait until all the other threads reach this point */
                int mask = (0x1 << d) - 1;
                if ((lid & mask) == mask) {

                        int offset = (0x1 << d);
                        int a = b - offset;
                        int t = data[a]; /* swap data[a] and data[b] then data[b] += data[a] */
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

//         int lid = get_local_id (0);
//         int lane = (lid * 2) + 1;

//         upsweep (data, length);

//         if (lane == (length - 1))
//                 data[lane] = 0;

//         downsweep (data, length);
//         return ;
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

__kernel void selectKernel (
        const int operator,
        const int size,
        const int tuples,
        __global const uchar *input,
        __global int *flags, /* The output of select (0 or 1) */ // [input & output]
        __global int *offsets, // [output] maps the local memory position of the tuples to global memory
        __global int *partitions, // [output] ???
        __global uchar *output, // [output] ???
        __local  int *x // [output] local memory position of tuples
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
        /* A thread group processes twice as many tuples */
        int L = 2 * lgs; // total tuples in the group

        /* Fetch tuple and apply selection filter */
        const int lidx =  left * sizeof(input_t);
        const int ridx = right * sizeof(input_t);

        // opencl syntax
        __global input_t *lp = (__global input_t *) &input[lidx];
        __global input_t *rp = (__global input_t *) &input[ridx];

        // apply predicates
        flags[ left] = selectf (lp);
        flags[right] = selectf (rp);

        /* Copy flag to local memory */
        x[ _left] = (left  < tuples) ? flags[ left] : 0; // left < tuples means not exceeding the total amount of tuples
        x[_right] = (right < tuples) ? flags[right] : 0; // if exceeding -> 0 -> means not taking this tuple

        upsweep(x, L);

        // if this thread is the last in this group, save the flag of right tuple in partitions 
        // array corresponding to the group id and put a 0 to its flag  
        if (lid == (lgs - 1)) { // _left = lid * 2 = 2 * (lgs - 1) = (2 * lgs - 1) - 1
                partitions[gid] = x[_right];
                x[_right] = 0; // So that in the downsweep, the last x[] element will be added to all the other elements
        }

        downsweep(x, L);

        /* Write results to global memory */
        offsets[ left] = ( left < tuples) ? x[ _left] : -1;
        offsets[right] = (right < tuples) ? x[_right] : -1;
}


__kernel void compactKernel (
        const int size,
        const int tuples,
        __global const uchar *input,
        __global int *flags,
        __global int *offsets,
        __global int *partitions, // [input now]
        __global uchar *output,
        __local  int *x
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
        barrier(CLK_LOCAL_MEM_FENCE); // From now on protect the local memory

        /* Compact left and right */
        if (flags[left] == 1) {

                const int lq = (offsets[left] + pivot) * sizeof(output_t); // left tuple of output memory location
                const int lp = left * sizeof(input_t); // left tuple of input memory location
                flags[left] = lq + sizeof(output_t); // ???
                 __global  input_t *lx = (__global  input_t *) &  input[lp];
                 __global output_t *ly = (__global output_t *) & output[lq];

                 /* TODO: replace with generic function */

                 ly->vectors[0] = lx->vectors[0];
                 ly->vectors[1] = lx->vectors[1];
                //  ly->vectors[2] = lx->vectors[2];
                //  ly->vectors[3] = lx->vectors[3];
        }

        if (flags[right] == 1) {

                const int rq = (offsets[right] + pivot) * sizeof(output_t);
                const int rp = right * sizeof(input_t);
                flags[right] = rq + sizeof(output_t);
                 __global  input_t *rx = (__global  input_t *) &  input[rp];
                 __global output_t *ry = (__global output_t *) & output[rq];

                 /* TODO: replace with generic function */

                 ry->vectors[0] = rx->vectors[0];
                 ry->vectors[1] = rx->vectors[1];
                 // ry->vectors[2] = rx->vectors[2];
                 // ry->vectors[3] = rx->vectors[3];

        }
}
