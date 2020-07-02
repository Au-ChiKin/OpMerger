#pragma once

#include "Schema.hpp"

struct Schema1 : Schema {
    long timestamp;
    float f1;
    int i1;
    int i2;
    int i3;
    int i4;

    Schema1(long timestamp, float f1, int i1, int i2, int i3, int i4) :
        timestamp(timestamp), f1(f1), i1(i1), i2(i2), i3(i3), i4(i4) {};

};