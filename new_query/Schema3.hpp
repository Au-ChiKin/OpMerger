#pragma once

#include "Schema.hpp"

struct Schema3 : Schema {
    long timestamp;
    float f1;
    int f2;
    int f3;
    int i1;
    int i2;

    Schema3(long timestamp, float f1, float f2, float f3, int i1, int i2) :
        timestamp(timestamp), f1(f1), f2(f2), f3(f3), i1(i1), i2(i2) {};

};