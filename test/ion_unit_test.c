#include "ion_unit_test.h"

#include <stdbool.h>
#include <stdlib.h>
#include "tester.h"

iERR run_unit_test(unit_test f) {
    iENTER;

    g_value_count++;

    err = f();
    if (err != IERR_OK) {
        g_failure_count++;
    }

    iRETURN;
}

BYTE* hex_string_to_bytes(char* hex) {
    size_t length = strlen(hex);
    bool is_odd = (length % 2) == 1;
    if (is_odd) {
        return NULL;
    }

    BYTE* bytes = calloc(length / 2, sizeof(BYTE));

    for (int i = 0; i < length; i += 2) {
        BYTE upper = hex_char_to_byte(hex[i]);
        BYTE lower = hex_char_to_byte(hex[i + 1]);
        bytes[i/2] = (upper << 4) | lower;
    }

    return bytes;
}

BYTE hex_char_to_byte(char hex) {
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
