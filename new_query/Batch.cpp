#include "Batch.hpp"

void Batch::insertTuple(Schema* tuple) {
    tuples.emplace_back(tuple);
}

