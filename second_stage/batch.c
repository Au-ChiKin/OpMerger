#include "batch.h"

#include <stdlib.h>

batch_p batch(int size, long start, long end, u_int8_t * buffer) {
    batch_p batch = (batch_p) malloc(sizeof(batch_t));

    batch->start = start;
    batch->end = end;
    batch->size = size;
    batch->buffer = buffer;

    return batch;    
}

long batch_get_timestamp(batch_p batch, int offset) {
    union long_u {
        long value;
        u_int8_t vectors [8];
    };

    union long_u timestamp;
    for (int i=0; i<8; i++) {
        timestamp.vectors[i] = *(batch->buffer + offset + i);
    }

    return timestamp.value;
}