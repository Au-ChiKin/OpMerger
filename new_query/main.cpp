// main.cu
//
// Zijian Ou - zo19@ic.ac.uk
// 
// Let's have a couple of schemas first
// <schema1> 
// timestamp:long  f1:float  i1:int  i2:int  i3:int  i4:int
// 
// <schema2>
// timestamp:long  f1:float  f2:float  f3:float  f4:float  i1:int
// 
// <schema3>
// timestamp:long  f1:float  f2:float  i1:int  i2:int  i3:int
// 
// Then we need a query for them
// <query>
// SELECT *
// FROM S1, S3,
//      (SELECT S2.z, count(*))
//       FROM   S2
//       WHERE  S2.y=3
//       GROUP BY S2.z) AS S2
// WHERE S1.x=7 AND S1.a=S3.b AND R2.z=R3.c 
//
// How should we write a stream query?
// So weird since it is not oneshot 
// Based on window?
// 
//

#include <iostream>
#include <vector>

#include "Schema.hpp"
#include "Schema1.hpp"
#include "Schema2.hpp"
#include "Schema3.hpp"
#include "Batch.hpp"

// __global__
void select_cuda() {
    
}

// void select_cpu() {

// }

// It is hard to tell whether it is a line breaker
// Therefore we need something else as a line breaker
// __global__
void join_cuda() {

}

// Line breakers
// __global__
void group_by_count_cuda() {

}

// void join_cpu() {

// }

int main() {
    const int iterNum = 1 << 10;

    Batch batch;

    // Create and initialise tuple set
    std::vector<Schema1 *> streamSet1;
    std::vector<Schema2 *> streamSet2;
    std::vector<Schema3 *> streamSet3;

    const int SCHE_NUM1 = 20;
    for (int i=0; i<10; i++) {
        streamSet1.emplace_back(new Schema1(i, 0.5, 1, 1, 1, 1));
    }
    for (int i=10; i<20; i++) {
        streamSet1.emplace_back(new Schema1(i, 1.5, 1, 1, 1, 1));
    }

    const int SCHE_NUM2 = 20;
    for (int i=0; i<10; i++) {
        streamSet2.emplace_back(new Schema2(i, 1.0, 1.0, 1.0, 1.0, 1));
    }
    for (int i=0; i<10; i++) {
        streamSet2.emplace_back(new Schema2(i, 1.0, 1.0, 1.0, 1.0, 1));
    }

    // Generator
    for (int i = 0; i < iterNum; i++) {
        // TODO: Make query
    }

    return 0;
}