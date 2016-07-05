#include "ion_assert.h"

iERR assert_equals_int(int expected, int actual) {
    return (expected == actual) ? IERR_OK : IERR_INVALID_ARG;
}
