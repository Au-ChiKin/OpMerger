#pragma once

#include "Schema.hpp"

struct Schema2 : public Schema {
    long timestamp;
    float f1;
    float f2;
    float f3;
    float f4;
    int i1;

    Schema2(long timestamp, float f1, float f2, float f3, float f4, int i1) :
        timestamp(timestamp), f1(f1), f2(f2), f3(f3), f4(f4), i1(i1) {};

};