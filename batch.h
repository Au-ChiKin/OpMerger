#ifndef __BATCH_H_
#define __BATCH_H_

#include <stdlib.h>

typedef struct batch * batch_p;
typedef struct batch {
    long start;
    long end;
    u_int8_t * buffer;
    long pane_size;
} batch_t;


#endif