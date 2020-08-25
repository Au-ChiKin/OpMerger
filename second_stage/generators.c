#include "generators.h"

#include <string.h>
#include <stdio.h>
#include <ctype.h>

#define MAX_LINE_LENGTH 128
#define MAX_PREFIX_LENGTH 64
#define MAX_INPUT_TUPLE_LENGTH 1024
#define MAX_OUTPUT_TUPLE_LENGTH 1024

#define _sprintf(format, ...) \
{\
    sprintf(s, format, __VA_ARGS__);\
    strcat(ret, s);\
}

#define _sprint(format) \
{\
    sprintf(s, format);\
    strcat(ret, s);\
}

/* TODO untested */
void str_to_upper(char * des, char const * str) {
    if (des == NULL) {
        fprintf(stderr, "error: des is NULL (%s)\n", __FUNCTION__);
        exit(1);
    }

    for (size_t i=0; i<strlen(str); i++) {
        des[i] = toupper(str[i]);
    }
    des[strlen(str)] = '\0';
}

char * generate_filename(int id, char const * base) {
    char * ret = (char *) malloc(64 * sizeof(char)); *ret = '\0';
    char s [MAX_LINE_LENGTH] = "";

    _sprintf("%s_%d.cl", base, id);

    return ret;
}

char * generate_window_definition(window_p window) {
    char * ret = (char *) malloc(256 * sizeof(char)); *ret = '\0';
    char s[MAX_LINE_LENGTH] = "";
    
    if (window->type == RANGE_BASE) {
        _sprint("#define RANGE_BASED\n\n");
    } else {
        _sprint("#define COUNT_BASED\n\n");
    }
    
    _sprintf("#define PANES_PER_WINDOW %dL\n", window->size / window->pane_size);
    _sprintf("#define PANES_PER_SLIDE  %dL\n", window->slide / window->pane_size);
    _sprintf("#define PANE_SIZE        %dL\n\n", window->pane_size);
    
    return ret;
}

char * generate_input_tuple (schema_p schema, char const * prefix, int vector) {
    char * ret = (char *) malloc(MAX_INPUT_TUPLE_LENGTH * sizeof(char)); *ret = '\0';
    char s[MAX_LINE_LENGTH] = "";

    if (prefix && strlen(prefix) >= MAX_PREFIX_LENGTH) {
        fprintf(stderr, "error: prefix is to long (limit is %d) (%s)\n", MAX_PREFIX_LENGTH, __FUNCTION__);
        exit(1);
    }
    
    _sprint("typedef struct {\n");
    /* The first attribute is always a timestamp */
    _sprint("    long t;\n");
    for (int i = 1; i < schema->attr_num; i++) {
        switch(schema->attr[i]) {
        case TYPE_INT:   
            _sprintf("    int _%d;\n", i);
            break;
        case TYPE_FLOAT: 
            _sprintf("    float _%d;\n", i);
            break;
        case TYPE_LONG:
            _sprintf("    long _%d;\n", i);
            break;
        
        default:
            fprintf(stderr, "error: failed to generate tuple struct (attribute %d is undefined)", i);
            exit(1);
        }
    }
    
    if (schema_get_pad(schema, vector) > 0) {
        _sprintf("    uchar pad[%d];\n", schema_get_pad(schema, vector));
    }
    
    if (prefix == NULL) {
        _sprint("} input_tuple_t __attribute__((aligned(1)));\n");
    } else {
        _sprintf("} %s_input_tuple_t __attribute__((aligned(1)));\n", prefix);
    }
    _sprint("\n");
    
    _sprint("typedef union {\n");
    if (prefix == NULL) {
        _sprint("    input_tuple_t tuple;\n");
        _sprintf("    uchar%d vectors[INPUT_VECTOR_SIZE];\n", vector);
        _sprint("} input_t;\n");
    } else {
        char upper_prefix [MAX_PREFIX_LENGTH];
        str_to_upper(upper_prefix, prefix);

        _sprintf("    %s_input_tuple_t tuple;\n", prefix);
        _sprintf("    uchar%d vectors[%s_INPUT_VECTOR_SIZE];\n", vector, upper_prefix);
        _sprintf("} %s_input_t;\n", prefix);
    }
    _sprint("\n");
    
    return ret;
}

char * generate_output_tuple (schema_p schema, char const * prefix, int vector) {
    char * ret = (char *) malloc(MAX_OUTPUT_TUPLE_LENGTH * sizeof(char)); *ret = '\0';
    char s[MAX_LINE_LENGTH] = "";

    if (prefix && strlen(prefix) >= MAX_PREFIX_LENGTH) {
        fprintf(stderr, "error: prefix is to long (limit is %d) (%s)\n", MAX_PREFIX_LENGTH, __FUNCTION__);
        exit(1);
    }
		
    _sprint("typedef struct {\n");
    
    if (schema->attr[0] == TYPE_LONG) {
        /* The first long attribute is assumed to be always a timestamp */
        _sprint("    long t;\n");
        for (int i = 1; i < schema->attr_num; i++) {
            
            switch(schema->attr[i]) {
            case TYPE_INT:   _sprintf("    int _%d;\n",   i); break;
            case TYPE_FLOAT: _sprintf("    float _%d;\n", i); break;
            case TYPE_LONG:  _sprintf("    long _%d;\n",  i); break;
            
            default:
                fprintf(stderr, "error: failed to generate tuple struct (attribute %d) is undefined)", i);
                exit(1);
            }
        }
    } else {
        for (int i = 0; i < schema->attr_num; i++) {
            
            switch(schema->attr[i]) {
            case TYPE_INT:   _sprintf("    int _%d;\n",   (i + 1)); break;
            case TYPE_FLOAT: _sprintf("    float _%d;\n", (i + 1)); break;
            case TYPE_LONG:  _sprintf("    long _%d;\n",  (i + 1)); break;
            
            default:
                fprintf(stderr, "error: failed to generate tuple struct (attribute %d) is undefined)", i);
                exit(1);
            }
        }
    }
    
    if (schema_get_pad(schema, vector) > 0) {
        _sprintf("    uchar pad[%d];\n", schema_get_pad(schema, vector));
    }
    
    _sprint("} output_tuple_t __attribute__((aligned(1)));\n\n");
    
    _sprint("typedef union {\n");
    _sprint("    output_tuple_t tuple;\n");
    _sprintf("    uchar%d vectors[OUTPUT_VECTOR_SIZE];\n", vector);
    _sprint("} output_t;\n\n");
    
    return ret;
}

char * generate_tuple_size(schema_p input_schema, schema_p output_schema, int vector) {
    char * ret = (char *) malloc(MAX_OUTPUT_TUPLE_LENGTH * sizeof(char)); *ret = '\0';
    char s[MAX_LINE_LENGTH] = "";

    if ((input_schema->size + schema_get_pad(input_schema, vector)) % vector != 0 || 
        (output_schema->size + schema_get_pad(output_schema, vector)) % vector != 0) {
        fprintf(stderr, "error: input/output size is not a multiple of the vector `uchar%d`\n", vector);
        exit(1);
    }

    _sprintf("#define INPUT_VECTOR_SIZE %d\n", 
        (input_schema->size + schema_get_pad(input_schema, vector)) / vector);
    _sprintf("#define OUTPUT_VECTOR_SIZE %d\n\n", 
        (output_schema->size + schema_get_pad(output_schema, vector)) / vector);

    return ret;
}