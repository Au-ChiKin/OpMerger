#include "generators.h"

#include <string.h>
#include <stdio.h>
#include <ctype.h>

#define MAX_LINE_LENGTH 128
#define MAX_PREFIX_LENGTH 64
#define MAX_INPUT_TUPLE_LENGTH 1024
#define MAX_OUTPUT_TUPLE_LENGTH 1024

#define _sprintf(format, __VA_ARGS__...) \
{\
    sprintf(s, format, __VA_ARGS__);\
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
        _sprintf("#define RANGE_BASED\n\n", NULL);
    } else {
        _sprintf("#define COUNT_BASED\n\n", NULL);
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
    
    _sprintf("typedef struct {\n", NULL);
    /* The first attribute is always a timestamp */
    _sprintf("    long t;\n", NULL);
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
    
    /* TODO: add padding feature */
    // if (schema.getPad().length > 0)
    //     _sprintf("    uchar pad[%d];\n", schema.getPad().length));
    
    if (prefix == NULL) {
        _sprintf("} input_tuple_t __attribute__((aligned(1)));\n", NULL);
    } else {
        _sprintf("} %s_input_tuple_t __attribute__((aligned(1)));\n", prefix);
    }
    _sprintf("\n", NULL);
    
    _sprintf("typedef union {\n", NULL);
    if (prefix == NULL) {
        _sprintf("    input_tuple_t tuple;\n", NULL);
        _sprintf("    uchar%d vectors[INPUT_VECTOR_SIZE];\n", vector);
        _sprintf("} input_t;\n", NULL);
    } else {
        char upper_prefix [MAX_PREFIX_LENGTH];
        str_to_upper(upper_prefix, prefix);

        _sprintf("    %s_input_tuple_t tuple;\n", prefix);
        _sprintf("    uchar%d vectors[%s_INPUT_VECTOR_SIZE];\n", vector, upper_prefix);
        _sprintf("} %s_input_t;\n", prefix);
    }
    _sprintf("\n", NULL);
    
    return ret;
}

char * generate_output_tuple (schema_p schema, char const * prefix, int vector) {
    char * ret = (char *) malloc(MAX_OUTPUT_TUPLE_LENGTH * sizeof(char)); *ret = '\0';
    char s[MAX_LINE_LENGTH] = "";

    if (prefix && strlen(prefix) >= MAX_PREFIX_LENGTH) {
        fprintf(stderr, "error: prefix is to long (limit is %d) (%s)\n", MAX_PREFIX_LENGTH, __FUNCTION__);
        exit(1);
    }
		
    _sprintf("typedef struct {\n", NULL);
    
    if (schema->attr[0] == TYPE_LONG) {
        /* The first long attribute is assumed to be always a timestamp */
        _sprintf("    long t;\n", NULL);
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
    
    /* TODO */
    // if (schema.getPad().length > 0)
    //     _sprintf("    uchar pad[%d];\n", schema.getPad().length);
    
    _sprintf("} output_tuple_t __attribute__((aligned(1)));\n\n", NULL);
    
    _sprintf("typedef union {\n", NULL);
    _sprintf("    output_tuple_t tuple;\n", NULL);
    _sprintf("    uchar%d vectors[OUTPUT_VECTOR_SIZE];\n", vector, NULL);
    _sprintf("} output_t;\n\n", NULL);
    
    return ret;
}

char * generate_tuple_size(int input_size, int output_size, int vector) {
    char * ret = (char *) malloc(MAX_OUTPUT_TUPLE_LENGTH * sizeof(char)); *ret = '\0';
    char s[MAX_LINE_LENGTH] = "";

    if (input_size % vector != 0 || output_size % vector != 0) {
        fprintf(stderr, "error: input/output sizz is not a multiple of the vector `uchar%d`\n", vector);
        exit(1);
    }

    _sprintf("#define INPUT_VECTOR_SIZE %d\n", input_size / vector);
    _sprintf("#define OUTPUT_VECTOR_SIZE %d\n\n", output_size / vector);

    return ret;
}