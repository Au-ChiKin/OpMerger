#ifndef __TUPLE_H_
#define __TUPLE_H_

#define ATTRIBUTE_NUM 11 /* excludes timestamp */
#define TUPLE_SIZE 64

/* must match the input struct in kernel code */
typedef struct tuple {
    /* 64 bytes in total */
    long  time_stamp;
    long  job_id;
    long  task_id;
    long  machine_id;
    int   user_id;
    int   event_type;
    int   category;
    int   priority;
    float cpu;
    float ram;
    float disk;
    int constraints;
} tuple_t;

typedef union {
    tuple_t tuple;
    u_int8_t vectors[TUPLE_SIZE]; // 64 bytes
} input_t;


#endif /* SEEP_TUPLE_H_ */