#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics: enable
#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics: enable
#pragma OPENCL EXTENSION cl_khr_int64_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_int64_extended_atomics: enable
#pragma OPENCL EXTENSION cl_khr_fp64: enable

#pragma OPENCL EXTENSION cl_khr_byte_addressable_store: enable

#include "../clib/templates/byteorder.h"
#define INPUT_VECTOR_SIZE 4
#define OUTPUT_VECTOR_SIZE 1

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
    long t; /* timestamp */
    float _1; /* sum */
    int _2; /* count */
} output_tuple_t __attribute__((aligned(1)));

typedef union {
    output_tuple_t tuple;
    uchar16 vectors[OUTPUT_VECTOR_SIZE];
} output_t;


#define RANGE_BASED

#define PANES_PER_WINDOW 1L
#define PANES_PER_SLIDE  1L
#define PANE_SIZE        1024L

inline void initf (__local output_t *p) {
    p->tuple.t = 0;
    p->tuple._1 = 0;
    p->tuple._2 = 0;
}

/* Projection */
/* r */ inline float transformf (float input) {
    /* r */ return input;
/* r */ }

/* r */ inline void reducef (__local output_t *out, __global input_t *in) {

    /* r */ int flag = 1;

            /* Selection */
            // int attribute_value = in->tuple._7; /* where category == 1*/
            // flag = flag & (attribute_value == 9);

    /* Reduce */
    /* r */ out->tuple.t = (out->tuple.t == 0) ? : in->tuple.t;
            out->tuple._1 += transformf(in->tuple._8) * flag;
    /* r */ out->tuple._2 += 1 * flag;
/* r */ }

inline void cachef (__local output_t *p, __local output_t *q) {
    q->vectors[0] = p->vectors[0];
}

inline void mergef (__local output_t *p, __local output_t *q) {
    p->tuple._1 += q->tuple._1;
    p->tuple._2 += q->tuple._2;
}

inline void copyf (__local output_t *p, __global output_t *q) {
    q->vectors[0] = p->vectors[0];
}


__kernel void clearKernel (

    const int tuples,
    const int bytes,

    const int max_windows,

    const long previous_pane_id,
    const long start_pointer,

    __global const uchar *input,
    __global       int   *window_start_pointers,
    __global       int   *window_end_pointers,
    __global       long  *offset,
    __global       int   *window_counts,
    __global       uchar *output,
    __local        uchar *scratch
) {

    int tid = (int) get_global_id (0);

    /* Thread 0 clears the offsets */
    if (tid == 0) {
        offset[0] = LONG_MAX;
        offset[1] = 0;
    }

    /* Initialise window counters: closing, pending, complete, opening, +1 */
    if (tid < 5)
        window_counts[tid] = 0;

    if (tid >= max_windows)
        return;

    window_start_pointers [tid] = -1;
    window_end_pointers [tid] = -1;

    return;
}

__kernel void computeOffsetKernel (

    const int tuples,
    const int bytes,

    const int max_windows,

    const long previous_pane_id,
    const long start_pointer,

    __global const uchar *input,
    __global       int   *window_start_pointers,
    __global       int   *window_end_pointers,
    __global       long  *offset,
    __global       int   *window_counts,
    __global       uchar *output,
    __local        uchar *scratch
) {

    int tid = (int) get_global_id (0);

    long wid;
    long paneId, normalisedPaneId;

    long currPaneId;
    long prevPaneId;

    if (start_pointer == 0) {
        if (tid == 0)
            offset[0] = 0L;
        return;
    }

    /* Every thread is assigned a tuple */

#ifdef RANGE_BASED
    __global input_t *curr = (__global input_t *) &input[tid * sizeof(input_t)];
    currPaneId = curr->tuple.t / PANE_SIZE;
#else
    currPaneId = ((start_pointer + (tid * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif

    if (tid > 0) {

#ifdef RANGE_BASED
    __global input_t *prev = (__global input_t *) &input[(tid - 1) * sizeof(input_t)];
    prevPaneId = prev->tuple.t / PANE_SIZE;
#else
    prevPaneId = ((start_pointer + ((tid - 1) * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif

    } else {

        prevPaneId = previous_pane_id;
    }

    if (prevPaneId < currPaneId) {

        /* Compute offset based on the first closing window */

        while (prevPaneId < currPaneId) {

            paneId = prevPaneId + 1;

            normalisedPaneId = paneId - PANES_PER_WINDOW;

            if (normalisedPaneId >= 0 && normalisedPaneId % PANES_PER_SLIDE == 0) {

                wid = normalisedPaneId / PANES_PER_SLIDE;

                if (wid >= 0) {

#ifdef __APPLE__ /* build-in gpu does not support khr_int64_extended_atomics extension */ 
                    atomic_min((__global int *) &offset[0], (int) wid);
#else
                    atom_min(&offset[0], wid);
#endif
                    break;
                }
            }
            prevPaneId += 1;
        }
    }

    return;
}

__kernel void computePointersKernel (

    const int tuples,
    const int bytes,

    const int max_windows,

    const long previous_pane_id,
    const long start_pointer,

    __global const uchar *input,
    __global       int   *window_start_pointers,
    __global       int   *window_end_pointers,
    __global       long  *offset,
    __global       int   *window_counts,
    __global       uchar *output,
    __local        uchar *scratch
) {

    int tid = (int) get_global_id (0);

    long wid;
    long paneId, normalisedPaneId;

    long currPaneId;
    long prevPaneId;

    /* Every thread is assigned a tuple */

#ifdef RANGE_BASED
    __global input_t *curr = (__global input_t *) &input[tid * sizeof(input_t)];
    currPaneId = curr->tuple.t / PANE_SIZE;
    currPaneId = ((start_pointer + (tid * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif

    if (tid > 0) {

#ifdef RANGE_BASED
    __global input_t *prev = (__global input_t *) &input[(tid - 1) * sizeof(input_t)];
    prevPaneId = prev->tuple.t / PANE_SIZE;
#else
    prevPaneId = ((start_pointer + ((tid - 1) * sizeof(input_t))) / sizeof(input_t)) / PANE_SIZE;
#endif

    } else {

        prevPaneId = previous_pane_id;
    }

    long windowOffset = offset[0];
    int index;

    if (prevPaneId < currPaneId) {

        while (prevPaneId < currPaneId) {

            paneId = prevPaneId + 1;
            normalisedPaneId = paneId - PANES_PER_WINDOW;

            /* Check for closing windows */

            if (normalisedPaneId >= 0 && normalisedPaneId % PANES_PER_SLIDE == 0) {

                wid = normalisedPaneId / PANES_PER_SLIDE;

                if (wid >= 0) {

                    index = convert_int_sat(wid - windowOffset);

#ifdef __APPLE__
                    atomic_max((__global int *) &offset[1], (int) (wid - windowOffset));
#else
                    atom_max(&offset[1], (wid - windowOffset));
#endif

                    window_end_pointers [index] = tid * sizeof(input_t);
                }
            }

            /* Check for opening windows */

            if (paneId % PANES_PER_SLIDE == 0) {

                wid = paneId / PANES_PER_SLIDE;

                index = convert_int_sat(wid - windowOffset);

#ifdef __APPLE__
                    atomic_max((__global int *) &offset[1], (int) (wid - windowOffset));
#else
                    atom_max(&offset[1], (wid - windowOffset));
#endif

                window_start_pointers [index] = tid * sizeof(input_t);
            }

            prevPaneId += 1;
        }
    }

    return;
}

__kernel void reduceKernel (

    const int tuples,
    const int bytes,

    const int max_windows,

    const long previous_pane_id,
    const long start_pointer,

    __global const uchar *input,
    __global       int   *window_start_pointers,
    __global       int   *window_end_pointers,
    __global       long  *offset,
    __global       int   *window_counts,
    __global       uchar *output,
    __local        uchar *scratch
) {

    int tid = (int) get_global_id  (0);
    int lid = (int) get_local_id   (0);
    int gid = (int) get_group_id   (0);
    int lgs = (int) get_local_size (0); /* Local group size */
    int nlg = (int) get_num_groups (0);

    int group_offset = lgs * sizeof(input_t);

    __local int num_windows;

    if (lid == 0)
        num_windows = convert_int_sat(offset[1]);

    barrier(CLK_LOCAL_MEM_FENCE);

    if (tid == 0)
        window_counts[4] = (num_windows + 1) * sizeof(output_t);

    int wid = gid;

    /* A group may process more than one windows */

    while (wid <= num_windows) {

        /* Window start and end pointers */

        int start = window_start_pointers [wid];
        int end   = window_end_pointers [wid];

        /* Check if a window is closing, opening, pending, or complete */

        if (start < 0 && end >= 0) {

            /* A closing window; set start offset */
            start = 0;
            if (lid == 0)
                atomic_inc(&window_counts[0]);

        } else if (start >= 0 && end < 0) {

            /* An opening window; set end offset */
            end = bytes;
            if (lid == 0)
                atomic_inc(&window_counts[3]);

        } else if (start < 0 && end < 0) {

            /* A pending window */
            int old = atomic_cmpxchg(&window_counts[1], 0, 1);
            if (old > 0) {
                /* Compute pending windows once */
                wid += nlg;
                continue;
            }
            start = 0;
            end = bytes;

        } else {

            /* A complete window */
            if (lid == 0)
                atomic_inc(&window_counts[2]);
        }

        if (start == end) {

            /* Skip empty windows */
            wid += nlg;
            continue;
        }

        int idx = lid * sizeof(input_t) + start;

        __local output_t tuple;
        initf (&tuple);

        /* The sequential part */
        while (idx < end && idx < bytes) {

            /* Get tuple from main memory */
            __global input_t *p = (__global input_t *) &input[idx];

            reducef (&tuple, p);

            idx += group_offset;
        }

        /* Write value to scratch memory */
        __local output_t *cached_tuple = (__local output_t *) &scratch[lid * sizeof(output_t)]; 
        /* ZO: each thread has a position to store the temp result */
        cachef (&tuple, cached_tuple);

        barrier(CLK_LOCAL_MEM_FENCE);

        /* Parallel reduction */

        for (int pos = lgs / 2; pos > 0; pos = pos / 2) {

            if (lid < pos) {

                __local output_t *mine  = (__local output_t *) &scratch[lid         * sizeof(output_t)];
                __local output_t *other = (__local output_t *) &scratch[(lid + pos) * sizeof(output_t)];

                mergef (mine, other);
            }

            barrier(CLK_LOCAL_MEM_FENCE);
        }

        /* Write result */
        if (lid == 0) {

            __local  output_t *cached = (__local  output_t *) &scratch[lid * sizeof(output_t)];
            __global output_t *result = (__global output_t *) &output [wid * sizeof(output_t)];

            copyf (cached, result);
        }

        /* Try next window */
        wid += nlg;
    }
    return;
}