#ifndef REDUCTION_H
#define REDUCTION_H

#include "batch.h"

void reduction_init(int batch_size);

void reduction_setup(int batch_size, int tuple_size);

void reduction_process(batch_p batch, int tuple_size, int qid);

#endif