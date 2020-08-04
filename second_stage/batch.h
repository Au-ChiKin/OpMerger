#ifndef __BATCH_H_
#define __BATCH_H_

#include <stdlib.h>

typedef struct batch * batch_p;
typedef struct batch {
    long start;
    long end;
    int size;   /* size in tuples */
    u_int8_t * buffer;
} batch_t;


#endif