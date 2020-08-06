#include "schema.h"

schema_p schema() {
    schema_p schema = (schema_p) malloc(sizeof(schema_t));
    schema->attr_num = 0;
    schema->size = 0;

    return schema;
}

void schema_add_attr(schema_p schema, enum attr_type attr) {
    schema->attr[schema->attr_num] = attr;
    schema->attr_num += 1;
    switch (attr)
    {
    case TYPE_INT:
        schema->size += 4;
        break;
    case TYPE_FLOAT:
        schema->size += 4;
        break;
    case TYPE_LONG:
        schema->size += 8;
        break;
    default:
        break;
    }
}

// private static String getInputHeader (ITupleSchema schema, String prefix, int vector) {
    
//     StringBuilder b = new StringBuilder ();
    
//     b.append("typedef struct {\n");
//     /* The first attribute is always a timestamp */
//     b.append("\tlong t;\n");
//     for (int i = 1; i < schema.numberOfAttributes(); i++) {
        
//         PrimitiveType type = schema.getAttributeType(i);
        
//         switch(type) {
//         case INT:   b.append(String.format("\tint _%d;\n",   i)); break;
//         case FLOAT: b.append(String.format("\tfloat _%d;\n", i)); break;
//         case LONG:  b.append(String.format("\tlong _%d;\n",  i)); break;
        
//         case UNDEFINED:
//             System.err.println("error: failed to generate tuple struct (attribute " + i + " is undefined)");
//             System.exit(1);
//         }
//     }
    
//     if (schema.getPad().length > 0)
//         b.append(String.format("\tuchar pad[%d];\n", schema.getPad().length));
    
//     if (prefix == null)
//         b.append("} input_tuple_t __attribute__((aligned(1)));\n");
//     else
//         b.append(String.format("} %s_input_tuple_t __attribute__((aligned(1)));\n", prefix));
//     b.append("\n");
    
//     b.append("typedef union {\n");
//     if (prefix == null) {
//         b.append("\tinput_tuple_t tuple;\n");
//         b.append(String.format("\tuchar%d vectors[INPUT_VECTOR_SIZE];\n", vector));
//         b.append("} input_t;\n");
//     } else {
//         b.append(String.format("\t%s_input_tuple_t tuple;\n", prefix));
//         b.append(String.format("\tuchar%d vectors[%s_INPUT_VECTOR_SIZE];\n", vector, prefix.toUpperCase()));
//         b.append(String.format("} %s_input_t;\n", prefix));
//     }
    
//     return b.toString();
// }

// private static String getOutputHeader (ITupleSchema schema, int vector) {
		
// 		StringBuilder b = new StringBuilder ();
		
// 		b.append("typedef struct {\n");
		
// 		if (schema.getAttributeType(0) == PrimitiveType.LONG) {
// 			/* The first long attribute is assumed to be always a timestamp */
// 			b.append("\tlong t;\n");
// 			for (int i = 1; i < schema.numberOfAttributes(); i++) {
				
// 				PrimitiveType type = schema.getAttributeType(i);
				
// 				switch(type) {
// 				case INT:   b.append(String.format("\tint _%d;\n",   i)); break;
// 				case FLOAT: b.append(String.format("\tfloat _%d;\n", i)); break;
// 				case LONG:  b.append(String.format("\tlong _%d;\n",  i)); break;
				
// 				case UNDEFINED:
// 					System.err.println("error: failed to generate tuple struct (attribute " + i + " is undefined)");
// 					System.exit(1);
// 				}
// 			}
// 		} else {
// 			for (int i = 0; i < schema.numberOfAttributes(); i++) {
				
// 				PrimitiveType type = schema.getAttributeType(i);
				
// 				switch(type) {
// 				case INT:   b.append(String.format("\tint _%d;\n",   (i + 1))); break;
// 				case FLOAT: b.append(String.format("\tfloat _%d;\n", (i + 1))); break;
// 				case LONG:  b.append(String.format("\tlong _%d;\n",  (i + 1))); break;
				
// 				case UNDEFINED:
// 					System.err.println("error: failed to generate tuple struct (attribute " + i + " is undefined)");
// 					System.exit(1);
// 				}
// 			}
// 		}
		
// 		if (schema.getPad().length > 0)
// 			b.append(String.format("\tuchar pad[%d];\n", schema.getPad().length));
		
// 		b.append("} output_tuple_t __attribute__((aligned(1)));\n");
// 		b.append("\n");
		
// 		b.append("typedef union {\n");
// 		b.append("\toutput_tuple_t tuple;\n");
// 		b.append(String.format("\tuchar%d vectors[OUTPUT_VECTOR_SIZE];\n", vector));
// 		b.append("} output_t;\n");
		
// 		return b.toString();
// 	}