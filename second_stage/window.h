#ifndef WINDOW_H
#define WINDOW_H 

enum window_types {
    RANGE_BASE,  /* Time based */
    COUNTER_BASE
};

typedef struct window * window_p;
typedef struct window {
    int size;
    int slide;
    enum window_types type;

    int pane_size;
} window_t;

#endif