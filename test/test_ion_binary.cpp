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
#include <ion_binary.h>

TEST(IonBinaryLen, UInt64) {

    ASSERT_EQ(0, ion_binary_len_uint_64(0LL));
    ASSERT_EQ(1, ion_binary_len_uint_64(1LL));
    ASSERT_EQ(1, ion_binary_len_uint_64(255LL));
    ASSERT_EQ(2, ion_binary_len_uint_64(256LL));
}

TEST(IonBinaryLen, Int64) {
    ASSERT_EQ(0, ion_binary_len_int_64(0LL));
    ASSERT_EQ(1, ion_binary_len_int_64(1LL));
    ASSERT_EQ(1, ion_binary_len_int_64(-1LL));
    ASSERT_EQ(2, ion_binary_len_int_64(255LL));
    ASSERT_EQ(2, ion_binary_len_int_64(-255LL));
    ASSERT_EQ(2, ion_binary_len_int_64(256LL));
    ASSERT_EQ(2, ion_binary_len_int_64(-256LL));
}
