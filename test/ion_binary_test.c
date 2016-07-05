#include "ion_binary_test.h"

#include <ion_binary.h>
#include "ion_assert.h"
#include "ion_unit_test.h"
#include "tester.h"

iERR ion_binary_test() {
    iENTER;

    run_unit_test(test_ion_binary_len_uint_64);
    run_unit_test(test_ion_binary_len_int64);

    iRETURN;
}

iERR test_ion_binary_len_uint_64() {
    iENTER;

    IONCHECK(assert_equals_int(0, ion_binary_len_uint_64(0LL)));
    IONCHECK(assert_equals_int(1, ion_binary_len_uint_64(1LL)));
    IONCHECK(assert_equals_int(1, ion_binary_len_uint_64(255LL)));
    IONCHECK(assert_equals_int(2, ion_binary_len_uint_64(256LL)));

    iRETURN;
}

iERR test_ion_binary_len_int64() {
    iENTER;

    IONCHECK(assert_equals_int(0, ion_binary_len_int64(0LL)));
    IONCHECK(assert_equals_int(1, ion_binary_len_int64(1LL)));
    IONCHECK(assert_equals_int(1, ion_binary_len_int64(-1LL)));
    IONCHECK(assert_equals_int(2, ion_binary_len_int64(255LL)));
    IONCHECK(assert_equals_int(2, ion_binary_len_int64(-255LL)));
    IONCHECK(assert_equals_int(2, ion_binary_len_int64(256LL)));
    IONCHECK(assert_equals_int(2, ion_binary_len_int64(-256LL)));

    iRETURN;
}