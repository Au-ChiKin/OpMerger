#include "window.h"

#include "helpers.h"

window_p window(int size, int slide, enum window_types type) {
    window_p p = (window_p) malloc(sizeof(window_t));

    p->size = size;
    p->slide = slide;
    p->pane_size = gcd(size, slide);
    p->type = type;

    return p;
}
