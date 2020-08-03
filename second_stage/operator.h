#ifndef OPERATOR_H
#define OPERATOR_H

enum operator_types {
    OPERATOR_SELECT,
    OPERATOR_REDUCE,
    OPERATOR_AGGREGATION
};

/* A collection of operator callbacks */
typedef struct opmerger_operator * operator_p;
typedef struct opmerger_operator {
    void * setup;
    void * process;
    void * print;
    enum operator_types type;
} operator_t;

#endif