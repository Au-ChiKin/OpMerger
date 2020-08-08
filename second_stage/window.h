#ifndef WINDOW_H
#define WINDOW_H 

#define WINDOW_SIZE_LIMIT 256 /* Should not exceed maximum threads in a group (reduction) */

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

window_p window(int size, int slide, enum window_types type);

#endif