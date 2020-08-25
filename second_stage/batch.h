#ifndef __BATCH_H_
#define __BATCH_H_

#include <stdlib.h>

typedef struct batch * batch_p;
typedef struct batch {
    long start;
    long end;   /* end - start gives the allocated size of this buffer */
    int size;   /* variable, in tuples, marking the number of stored meaning tuples */
    int tuple_size;
    u_int8_t * buffer;

    int closing_windows;
    int pending_windows;
    int complete_windows;
    int opening_windows;
} batch_t;

batch_p batch(int size, long start, u_int8_t * buffer, int buffer_size, int tuple_size);

long batch_get_first_tuple_timestamp64(batch_p batch, int offset);

void batch_free(batch_p b);

void batch_free_all(batch_p b);

#endif