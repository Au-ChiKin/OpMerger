#ifndef REDUCTION_H
#define REDUCTION_H

void reduction_setup(int batch_size, int tuple_size);

void reduction_process(u_int8_t * batch);

#endif