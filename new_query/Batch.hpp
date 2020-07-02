#pragma once

#include <vector>

#include "Schema.hpp"

class Batch {
    std::vector<Schema*> tuples;

  public:
    void insertTuple(Schema* tuple);
};