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
    ION_INT iint;
    // Initialize the Ion integer, setting its owner to NULL.
    IONCHECK(ion_int_init(&iint, NULL));
    // Populate the Ion integer with the provided int64_t value.
    IONCHECK(ion_int_from_long(&iint, value_in));
    // Read the Ion integer's value back out into the output int64_t.
    IONCHECK(ion_int_to_int64(&iint, value_out));
    iRETURN;
}

TEST(IonInteger, IIntToInt64RoundTrip) {
    // This test verifies that the `ion_int_from_long` and `ion_int_to_int64` functions work as
    // intended. For each of the following values in the range from MIN_INT64 to MAX_INT64 inclusive,
    // the test will convert the int64_t to an IINT and then back again, confirming that the output
    // int64_t is equal to the input int64_t.
    int64_t values[] = {
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

    int64_t value_out;
    for(int64_t value_in : values) {
        ION_ASSERT_OK(test_ion_int_roundtrip_int64_t(value_in, &value_out));
        ASSERT_EQ(value_in, value_out);
    }
}
