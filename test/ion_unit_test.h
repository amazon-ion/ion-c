#include <ion_errors.h>
#include <ion_types.h>

typedef iERR (*unit_test)();

iERR run_unit_test(unit_test f);
BYTE* hex_string_to_bytes(char* hex);
BYTE hex_char_to_byte(char hex);
