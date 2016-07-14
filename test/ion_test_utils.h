#include <ion_types.h>

static inline char* ion_test_get_type_name(ION_TYPE t) {
    switch(ION_TYPE_INT(t)) {
        case tid_none_INT:      return "tid_none";
        case tid_EOF_INT:       return "tid_EOF";
        case tid_NULL_INT:      return "tid_NULL";
        case tid_BOOL_INT:      return "tid_BOOL";
        case tid_INT_INT:       return "tid_INT";
        case tid_FLOAT_INT:     return "tid_FLOAT";
        case tid_DECIMAL_INT:   return "tid_DECIMAL";
        case tid_TIMESTAMP_INT: return "tid_TIMESTAMP";
        case tid_STRING_INT:    return "tid_STRING";
        case tid_SYMBOL_INT:    return "tid_SYMBOL";
        case tid_CLOB_INT:      return "tid_CLOB";
        case tid_BLOB_INT:      return "tid_BLOB";
        case tid_STRUCT_INT:    return "tid_STRUCT";
        case tid_LIST_INT:      return "tid_LIST";
        case tid_SEXP_INT:      return "tid_SEXP";
        case tid_DATAGRAM_INT:  return "tid_DATAGRAM";
        default:
            break;
    }

    char* buffer = NULL;
    size_t size = snprintf(buffer, 0, "unrecognized type: %d", ION_TYPE_INT(t));
    buffer = calloc(size, sizeof(char));
    snprintf(buffer, size, "unrecognized type: %d", ION_TYPE_INT(t));
    return buffer;
}
