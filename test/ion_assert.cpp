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

std::string _bytesToHexString(const BYTE *bytes, SIZE len) {
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < len; ++i) {
        ss << std::setfill('0') << std::setw(2) << (int)bytes[i] << " ";
    }
    return ss.str();
}

void assertBytesEqual(const char *expected, SIZE expected_len, const BYTE *actual, SIZE actual_len) {
    EXPECT_EQ(expected_len, actual_len);
    BOOL bytes_not_equal = memcmp((BYTE *)expected, actual, (size_t)actual_len);
    if (bytes_not_equal) {
        ASSERT_FALSE(bytes_not_equal) << "Expected: " << _bytesToHexString((BYTE *)expected, expected_len) << " vs. " << std::endl
                                      << "  Actual: "<< _bytesToHexString(actual, actual_len);
    }
}

void assertStringsEqual(const char *expected, const char *actual, SIZE actual_len) {
    BOOL strings_not_equal = strlen(expected) != actual_len || strncmp(expected, actual, (size_t)actual_len);
    if (strings_not_equal) {
        ASSERT_FALSE(strings_not_equal) << std::string(expected) << " vs. " << std::endl
                                        << std::string(actual, (unsigned long)actual_len);
    }
}
