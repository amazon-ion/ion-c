#include "ion_unit_test.h"

#include "tester.h"

iERR run_unit_test(unit_test f) {
    iENTER;

    err = f();
    if (err != IERR_OK) {
        g_failure_count++;
    }

    iRETURN;
}
