#include <ion_debug.h>

static inline iERR assert_equals_int(int expected, int actual) {
    return (expected == actual) ? IERR_OK : IERR_INVALID_ARG;
}
