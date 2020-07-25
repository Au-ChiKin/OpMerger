#include "circular_buffer.h"

#include <stdbool.h>
#include <assert.h>

#define EXAMPLE_BUFFER_SIZE 100 // bytes

int main() {
    uint8_t * buffer  = malloc(EXAMPLE_BUFFER_SIZE * sizeof(uint8_t));
    cbuf_handle_t cbuf = circular_buf_init(buffer, EXAMPLE_BUFFER_SIZE);

    bool full = circular_buf_full(cbuf);
    assert(!full);
    bool empty = circular_buf_empty(cbuf);
    assert(empty);
    printf("Current buffer size: %zu\n", circular_buf_size(cbuf));

    free(buffer);
    circular_buf_free(cbuf);

    return 0;
}