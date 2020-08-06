#include "batch.h"

#include <stdlib.h>

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