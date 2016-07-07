#include "ion_assert.h"
#include <ion_string.h>
#include <string.h>

iERR assert_equals_int(int expected, int actual) {
    return (expected == actual) ? IERR_OK : IERR_INVALID_ARG;
}

iERR assert_equals_ion_type(ION_TYPE expected, ION_TYPE actual) {
    return (expected == actual) ? IERR_OK : IERR_INVALID_ARG;
}

iERR assert_equals_c_string_ion_string(char* c_string, ION_STRING* ion_string) {
    if (strlen(c_string) != ion_string->length) {
        return IERR_INVALID_ARG;
    }

    if (strncmp(c_string, ion_string->value, ion_string->length) != 0) {
        return IERR_INVALID_ARG;
    }

    return IERR_OK;
}

iERR assert_equals_c_string(char* expected, char* actual) {
    return (strcmp(expected, actual) == 0) ? IERR_OK : IERR_INVALID_ARG;
}

iERR assert_equals_bytes(BYTE* expected, BYTE* actual, size_t length) {
    return (strncmp((char*) expected, (char*) actual, length) == 0) ? IERR_OK : IERR_INVALID_ARG;
}
