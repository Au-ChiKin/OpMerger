#include "window.h"

#include "helpers.h"

#include <stdlib.h>
#include <stdio.h>

window_p window(int size, int slide, enum window_types type) {
    window_p p = (window_p) malloc(sizeof(window_t));

    // if (size > 256) {
    //     fprintf(stderr, "error: Window size cannot exceed %d\n", WINDOW_SIZE_LIMIT);
    //     exit(1);
    // }

    p->size = size;
    p->slide = slide;
    p->pane_size = gcd(size, slide);
    p->type = type;

    return p;
}
