#ifndef HELPERS_H
#define HELPERS_H

char * read_file(char * filename);

void print_to_file(const char *filepath, const char *data);

char * read_source(char * filename);

/* Return the greatest commont divisor of two ints */
int gcd(int x, int y);

#endif