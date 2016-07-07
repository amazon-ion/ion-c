#include <ion_types.h>
#include <stdbool.h>
#include <string.h>

#define TYPE_NAME_TEMP_SIZE 100

static char type_name_temp[TYPE_NAME_TEMP_SIZE];

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

    snprintf(type_name_temp, TYPE_NAME_TEMP_SIZE, "unrecognized type: %d", ION_TYPE_INT(t));
    return type_name_temp;
}

static inline BYTE hex_char_to_byte(char hex) {
    switch(hex) {
        case '0':
            return 0;
        case '1':
            return 1;
        case '2':
            return 2;
        case '3':
            return 3;
        case '4':
            return 4;
        case '5':
            return 5;
        case '6':
            return 6;
        case '7':
            return 7;
        case '8':
            return 8;
        case '9':
            return 9;
        case 'a':
        case 'A':
            return 10;
        case 'b':
        case 'B':
            return 11;
        case 'c':
        case 'C':
            return 12;
        case 'd':
        case 'D':
            return 13;
        case 'e':
        case 'E':
            return 14;
        case 'f':
        case 'F':
            return 15;
    }

    return 0;
}

static inline void hex_string_to_bytes(char* hex, BYTE** result, size_t* result_size) {
    size_t length = strlen(hex);
    bool is_odd = (length % 2) == 1;
    if (is_odd) {
        return;
    }

    BYTE* bytes = calloc(length / 2, sizeof(BYTE));

    for (int i = 0; i < length; i += 2) {
        BYTE upper = hex_char_to_byte(hex[i]);
        BYTE lower = hex_char_to_byte(hex[i + 1]);
        bytes[i/2] = (upper << 4) | lower;
    }

    *result = bytes;
    *result_size = length / 2;
}

static inline char byte_to_hex_char(BYTE b) {
    switch(b) {
        case 0:
            return '0';
        case 1:
            return '1';
        case 2:
            return '2';
        case 3:
            return '3';
        case 4:
            return '4';
        case 5:
            return '5';
        case 6:
            return '6';
        case 7:
            return '7';
        case 8:
            return '8';
        case 9:
            return '9';
        case 10:
            return 'a';
        case 11:
            return 'b';
        case 12:
            return 'c';
        case 13:
            return 'd';
        case 14:
            return 'e';
        case 15:
            return 'f';
    }

    return '?';
}

static inline char* bytes_to_hex_string(BYTE* bytes, size_t length) {
    char* result = calloc(length * 2 + 1, sizeof(char));
    for (int i = 0; i < length * 2; i += 2) {
        BYTE b = bytes[i/2];
        result[i] = byte_to_hex_char(b >> 4);
        result[i+1] = byte_to_hex_char(b & 0xF);
    }
    return result;
}
