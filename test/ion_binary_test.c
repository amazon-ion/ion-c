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

    ASSERT_EQUALS_INT(0, ion_binary_len_uint_64(0LL), "Wrong uint64 length");
    ASSERT_EQUALS_INT(1, ion_binary_len_uint_64(1LL), "Wrong uint64 length");
    ASSERT_EQUALS_INT(1, ion_binary_len_uint_64(255LL), "Wrong uint64 length");
    ASSERT_EQUALS_INT(2, ion_binary_len_uint_64(256LL), "Wrong uint64 length");

    iRETURN;
}

iERR test_ion_binary_len_int64() {
    iENTER;

    ASSERT_EQUALS_INT(0, ion_binary_len_int64(0LL), "Wrong int64 length");
    ASSERT_EQUALS_INT(1, ion_binary_len_int64(1LL), "Wrong int64 length");
    ASSERT_EQUALS_INT(1, ion_binary_len_int64(-1LL), "Wrong int64 length");
    ASSERT_EQUALS_INT(2, ion_binary_len_int64(255LL), "Wrong int64 length");
    ASSERT_EQUALS_INT(2, ion_binary_len_int64(-255LL), "Wrong int64 length");
    ASSERT_EQUALS_INT(2, ion_binary_len_int64(256LL), "Wrong int64 length");
    ASSERT_EQUALS_INT(2, ion_binary_len_int64(-256LL), "Wrong int64 length");

    iRETURN;
}
