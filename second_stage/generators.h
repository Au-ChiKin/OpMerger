#ifndef GENERATORS_H
#define GENERATORS_H

#include "window.h"
#include "schema.h"

/* Generate source output file name */
char * generate_filename(int id, char const * base);

/* Generate window information */
char * generate_window_definition(window_p window);

char * generate_input_tuple (schema_p schema, char const * prefix, int vector);

char * generate_output_tuple (schema_p schema, char const * prefix, int vector);

char * generate_tuple_size(schema_p input_size, schema_p output_size, int vector);

#endif