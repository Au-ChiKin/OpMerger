#ifndef __TUPLE_H_
#define __TUPLE_H_

#define ATTRIBUTE_NUM 6

/* must match the input struct in kernel code */
typedef struct tuple {
    /* 32 bytes in total */
    long time_stamp;
    int i1;
    int i2;
    int i3;
    int i4;
    int i5;
    int i6;
} tuple_t;

#endif /* SEEP_TUPLE_H_ */