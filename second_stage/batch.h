#ifndef __BATCH_H_
#define __BATCH_H_

#include <stdlib.h>

typedef struct batch * batch_p;
typedef struct batch {
    long start;
    long end;   /* end - start gives the allocated size of this buffer */
    int size;   /* variable, in tuples, marking the number of stored meaning tuples */
    u_int8_t * buffer;
} batch_t;

long batch_get_timestamp(batch_p batch, int offset);

#endif