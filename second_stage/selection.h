#ifndef SELECTION_H
#define SELECTION_H

#include "batch.h"

void selection_init(int batch_size);

void selection_setup(int batch_size, int tuple_size);

void selection_process(batch_p batch, int batch_size, int tuple_size, int qid, batch_p output);

void selection_print_output(batch_p output, int batch_size, int tuple_size);

#endif