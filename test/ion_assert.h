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

#ifndef IONC_ION_ASSERT_H
#define IONC_ION_ASSERT_H

#include <ionc/ion.h>
#include <gtest/gtest.h>

/**
 * Tests that the given bytes are equal.
 */
void assertBytesEqual(const char *expected, SIZE expected_len, const BYTE *actual, SIZE actual_len);

/**
 * Tests that the given strings are equal.
 */
void assertStringsEqual(const char *expected, const char *actual, SIZE actual_len);

#endif