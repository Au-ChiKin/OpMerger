#include "helpers.h"

#include <stdio.h>

#include "../clib/debug.h"

#define MAX_SOURCE_SIZE (0x100000)
char * read_source(char * filename) {
    FILE *file_p;
    char * source_str;
    size_t source_size;
    int error = 0;

    /* Load kernel source */
    file_p = fopen(filename, "rb");
    if (!file_p) {
        fprintf(stderr, "Failed to load kernel.\n");
        exit(1);
    }
    source_str = (char*)malloc(MAX_SOURCE_SIZE);
    source_size = fread( source_str, 1, MAX_SOURCE_SIZE, file_p);
    fclose(file_p);

    dbg("[MAIN] Loaded kernel source length of %zu bytes\n", source_size);

    return source_str;
}