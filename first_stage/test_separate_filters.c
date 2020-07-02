/* 
 * The main logic to run the experiment of seperated operators
 */
#include "gpu.h"
#include "tuple.h"

#include <stdio.h>
#include <stdlib.h>

void run_processing(tuple_t * buffer, int size) {
    gpu_init();
    gpu_free();
}

#define BUFFER_SIZE 32768
int main() {

    // TODO: Create a buffer of tuples
    // 32768 x 32 = 1MB
    tuple_t buffer[BUFFER_SIZE];

    // TODO: Fill the buffer
    int value = 0;
    bool flipper = false;
    int cur = 0;
    while (cur < BUFFER_SIZE) {
        // #0
        buffer[cur].time_stamp = 1;

        // #1
        buffer[cur].i1 = value;
        value = (value + 1) % 100;

        // #2
        if (flipper) {
          buffer[cur].i2 = 0;
        } else {
          buffer[cur].i2 = 1;
        }
        flipper = !flipper;

        // #3
        buffer[cur].i3 = value; 

        // #4 #5 #6
        buffer[cur].i4 = 1;
        buffer[cur].i5 = 1;
        buffer[cur].i6 = 1;
    }   

    // TODO: Start processing
    run_processing(buffer, BUFFER_SIZE);

    return 0;
}