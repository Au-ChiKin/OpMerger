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
    source_str[source_size] = '\0';
    fclose(file_p);

    dbg("[MAIN] Loaded kernel source length of %zu bytes\n", source_size);

    return source_str;
}

char * read_file(char * filename) {
    FILE *file_p;
    char * source_str;
    size_t source_size;
    int error = 0;

    /* Load kernel source */
    file_p = fopen(filename, "rb");
    if (!file_p) {
        fprintf(stderr, "Failed to load file %s.\n", filename);
        exit(1);
    }
    source_str = (char*)malloc(MAX_SOURCE_SIZE);
    source_size = fread( source_str, 1, MAX_SOURCE_SIZE, file_p);
    source_str[source_size] = '\0';
    fclose(file_p);

    return source_str;
}

/* TODO: a more secure output function is needed */
void print_to_file(const char *filepath, const char *data)
{
    FILE *fp = fopen(filepath, "wb");
    
    if (fp != NULL)
    {
        fputs(data, fp);
        fclose(fp);
    } else {
        fprintf(stderr, "Failed to create the file %s.\n", filepath);
        exit(1);        
    }
}

int gcd(int x, int y) {
    if (x <= 0 || y <= 0) {
        fprintf(stderr, "error: input numbers should be positive (%s)\n", __FUNCTION__);
    }

    int ret;

    for(int i=1; i <= x && i <= y; ++i)
        {
            // Checks if i is factor of both integers
            if(x%i==0 && y%i==0)
                ret = i;
        }

    return ret;
}