#include "ion_unit_test.h"

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
