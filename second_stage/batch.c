#include "batch.h"

#include <stdlib.h>
#include <stdio.h>

batch_p batch(int size, long start, u_int8_t * buffer, int buffer_size, int tuple_size) {
    batch_p batch = (batch_p) malloc(sizeof(batch_t));

    if (buffer_size % size != 0) {
        fprintf(stderr, "error: Currently OpMerger does not support batch size that is not diviser of the buffer size\n");
        exit(1);
    }
    batch->size = size;

    batch->start = start;

    if (start + size * tuple_size > buffer_size * tuple_size) {
        fprintf(stderr, "error: Currently OpMerger does not support batch crossing the tail of the buffer\n");
        exit(1);
    }
    batch->end = start + size * tuple_size;

    batch->tuple_size = tuple_size;

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

void batch_free(batch_p b) {
    free(b);
}

void batch_free_all(batch_p b) {
    free(b->buffer);
    free(b);
}