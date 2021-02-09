/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

#include "ion_assert.h"
#include "ion_helpers.h"
#include "ion_test_util.h"

iERR test_ion_int_roundtrip_int64_t(int64_t value_in, int64_t * value_out) {
    iENTER;
    // Create an uninitialized Ion integer.
    ION_INT * iint;
    // Initialize the Ion integer, setting its owner to NULL.
    IONCHECK(ion_int_alloc(NULL, &iint));
    // Populate the Ion integer with the provided int64_t value.
    IONCHECK(ion_int_from_long(iint, value_in));
    // Read the Ion integer's value back out into the output int64_t.
    IONCHECK(ion_int_to_int64(iint, value_out));
    // Free the memory used to store the Ion integer's digits.
    ion_int_free(iint);
    iRETURN;
}

TEST(IonInteger, IIntToInt64RoundTrip) {
    // This test verifies that the `ion_int_from_long` and `ion_int_to_int64` functions work as
    // intended. For each of the following values in the range from MIN_INT64 to MAX_INT64 inclusive,
    // the test will convert the int64_t to an IINT and then back again, confirming that the output
    // int64_t is equal to the input int64_t.
    const uint32_t number_of_values = 19;
    int64_t values[number_of_values] = {
            MIN_INT64, MAX_INT64,
            MIN_INT64 + 16, MAX_INT64 - 16,
            -9670031482938124, 9670031482938124,
            -10031482954246, 10031482954246,
            -58116329947, 58116329947,
            -66182226, 66182226,
            -75221, 75221,
            -825, 825
            -1, 1,
            0
    };

    int64_t value_in;
    int64_t value_out;
    for(int m = 0; m < number_of_values; m++) {
        value_in = values[m];
        ION_ASSERT_OK(test_ion_int_roundtrip_int64_t(value_in, &value_out));
        ASSERT_EQ(value_in, value_out);
    }
}

iERR test_ion_int_to_int64_t_overflow_detection(const char * p_chars) {
    iENTER;
    const uint32_t max_string_length = 32;
    uint32_t string_length = strnlen(p_chars, max_string_length);
    // Create an uninitialized Ion integer.
    ION_INT iint;
    // Create an int64_t that we will later populate with the value of iint.
    int64_t value_out;
    // Initialize the Ion integer, setting its owner to NULL.
    IONCHECK(ion_int_init(&iint, NULL));
    // Populate the Ion integer with the value of the provided base-10 string
    IONCHECK(ion_int_from_chars(&iint, p_chars, string_length));
    // Attempt to read the Ion integer's value back out into the int64_t.
    // If the number is outside the range of values that can be represented by
    // an int64_t, this should return IERR_NUMERIC_OVERFLOW.
    IONCHECK(ion_int_to_int64(&iint, &value_out));
    iRETURN;
}

TEST(IonInteger, IIntToInt64Overflow) {
    // This test verifies that the `ion_int_to_int64` method will return IERR_NUMERIC_OVERFLOW
    // if the provided Ion integer's value will not fit in an int64_t. Because any Ion integer
    // constructed using `ion_int_from_long` will inherently fit in an int64_t, we instead
    // construct each Ion integer with `ion_int_from_chars`, passing in Ion text encodings
    // of the integers to create.
    const uint32_t number_of_ok_values = 9;
    const char *small_integers[number_of_ok_values] = {
        "-10004991088",
        "-9862",
        "-138",
        "-1",
        "0",
        "1",
        "138",
        "9862",
        "10004991088"
    };

    // Each of the above values will fit in an int64_t, so the test function should succeed.
    for (int m = 0; m < number_of_ok_values; m++) {
        const char *small_integer = small_integers[m];
        ION_ASSERT_OK(test_ion_int_to_int64_t_overflow_detection(small_integer));
    }

    const uint32_t number_of_oversized_values = 4;
    const char *oversized_integers[number_of_oversized_values] = {
        "9223372036854775808",  // MAX_INT64 + 1
        "-9223372036854775809", // MIN_INT64 - 1
        "10004991088252643637337337422",
        "-10004991088252643637337337422",
    };

    // Each of the above values has a magnitude that is too large to fit in an int64_t,
    // so the test function should fail, returning IERR_NUMERIC_OVERFLOW.
    for (int m = 0; m < number_of_oversized_values; m++) {
        const char *oversized_integer = oversized_integers[m];
        iERR error_value = test_ion_int_to_int64_t_overflow_detection(oversized_integer);
        ASSERT_EQ(error_value, IERR_NUMERIC_OVERFLOW);
    }
}