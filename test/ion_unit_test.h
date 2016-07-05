#include <ion_errors.h>

typedef iERR (*unit_test)();

iERR run_unit_test(unit_test f);
