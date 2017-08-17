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

#include <cmath>
#include <gtest/gtest.h>
#include "ion.h"
#include "ion_event_stream.h"

#define ION_ENTER_ASSERTIONS /* nothing */

#define ION_EXIT_ASSERTIONS return TRUE

#define ION_ACCUMULATE_ASSERTION(x) if (!(x)) return FALSE;

#define ION_ASSERT(x, m) { \
    if (!(x)) { \
        if (assertion_type == ASSERTION_TYPE_NORMAL) { \
            EXPECT_TRUE(x) << "Test: " << g_CurrentTest << ": " << m; \
        } \
        return FALSE; \
    }\
} \

#define ION_EXPECT_EQ_MSG(x, y, m) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_EQ(x, y) << "Test: " << g_CurrentTest << ": " << m; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            if ((x) != (y)) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_DOUBLE_EQ(x, y) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_TRUE(!(isnan(x) ^ isnan(y))); \
            if (isnan(x) || isnan(y)) break; \
            EXPECT_TRUE(!(ion_float_is_negative_zero(x) ^ ion_float_is_negative_zero(y))); \
            if (ion_float_is_negative_zero(x) || ion_float_is_negative_zero(y)) break; \
            EXPECT_DOUBLE_EQ(x, y) << "Test: " << g_CurrentTest; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            if (isnan(x) ^ isnan(y)) { \
                return FALSE; \
            } \
            if (isnan(x)) break; \
            if (ion_float_is_negative_zero(x) ^ ion_float_is_negative_zero(y)) { \
                return FALSE; \
            } \
            if (ion_float_is_negative_zero(x)) break; \
            if (!((::testing::DoubleLE("", "", x, y) == ::testing::AssertionSuccess()) \
                    && (::testing::DoubleLE("", "", y, x) == ::testing::AssertionSuccess()))) { \
                return FALSE; \
            } \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_TRUE_MSG(x, m) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_TRUE(x) << "Test: " << g_CurrentTest << ": " << m; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            if (!(x)) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_FALSE_MSG(x, m) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_FALSE(x) << "Test: " << g_CurrentTest << ": " << m; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            if (x) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_STRING_EQ(x, y) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_TRUE(assertIonStringEq(x, y)) << "Test: " << g_CurrentTest; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            if ((x) == NULL ^ (y) == NULL) return FALSE; \
            if ((x) != NULL && !ion_string_is_equal(x, y)) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_INT_EQ(x, y) { \
    int _ion_int_assertion_result = 0; \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_TRUE(assertIonIntEq(x, y)) << "Test: " << g_CurrentTest; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            EXPECT_EQ(IERR_OK, ion_int_compare(x, y, &_ion_int_assertion_result)); \
            if (_ion_int_assertion_result) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_DECIMAL_EQ(x, y) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_TRUE(assertIonDecimalEq(x, y)) << "Test: " << g_CurrentTest; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            BOOL _decimal_equals; \
            EXPECT_EQ(IERR_OK, ion_decimal_equals(x, y, &g_Context, &_decimal_equals)); \
            if (!_decimal_equals) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_TIMESTAMP_EQ(x, y) { \
    switch (assertion_type) { \
        case ASSERTION_TYPE_NORMAL: \
            EXPECT_TRUE(assertIonTimestampEq(x, y)) << "Test: " << g_CurrentTest; \
            break; \
        case ASSERTION_TYPE_SET_FLAG: \
            BOOL _timestamps_equal; \
            EXPECT_EQ(IERR_OK, g_TimestampEquals(x, y, &_timestamps_equal, &g_Context)); \
            if (!_timestamps_equal) return FALSE; \
            break; \
        default: \
            EXPECT_FALSE("Illegal state: unknown assertion type."); \
            break; \
    } \
} \

#define ION_EXPECT_EQ(x, y) ION_EXPECT_EQ_MSG(x, y, "")
#define ION_EXPECT_TRUE(x) ION_EXPECT_TRUE_MSG(x, "")
#define ION_EXPECT_FALSE(x) ION_EXPECT_FALSE_MSG(x, "")

typedef enum _assertion_type {
    /**
     * Any assertion failure causes a test failure.
     */
    ASSERTION_TYPE_NORMAL = 0,
    /*
     * An assertion failure returns FALSE. Useful when using the assertions to test recursive equivalence.
     */
    ASSERTION_TYPE_SET_FLAG
} ASSERTION_TYPE;

typedef enum _comparison_type {
    COMPARISON_TYPE_EQUIVS = 0,
    COMPARISON_TYPE_NONEQUIVS
} COMPARISON_TYPE;

typedef iERR (*TIMESTAMP_COMPARISON_FN)(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2, BOOL *is_equal, decContext *pcontext);

/**
 * Global variable that determines which timestamp comparison semantics should be used
 * (data model equivalence vs instant equivalence).
 */
extern TIMESTAMP_COMPARISON_FN g_TimestampEquals;

/**
 * Global variable that holds the name of the current test.
 */
extern std::string g_CurrentTest;

/**
 * Allocates and returns a new char * representing the given ION_INT.
 * Note: the caller is responsible for freeing the returned char *.
 */
char *ionIntToString(ION_INT *value);

/**
 * Allocates and returns a new char * representing the given ION_STRING.
 * Note: the caller is responsible for freeing the returned char *.
 */
char *ionStringToString(ION_STRING *value);

::testing::AssertionResult assertIonStringEq(ION_STRING *expected, ION_STRING *actual);
::testing::AssertionResult assertIonIntEq(ION_INT *expected, ION_INT *actual);
::testing::AssertionResult assertIonDecimalEq(decQuad *expected, decQuad *actual);

/**
 * Asserts that the given timestamps are equal. Uses g_TimestampEquals as the comparison method.
 */
::testing::AssertionResult assertIonTimestampEq(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual);

/**
 * Returns TRUE if and only if assertIonStringEq succeeds for the inputs.
 */
BOOL ionStringEq(ION_STRING *expected, ION_STRING *actual);

/**
 * Tests the values starting at the given indices in the given streams (they may be the same) for equivalence
 * under the Ion data model. If the indices start on CONTAINER_START events, this will recursively compare
 * the containers' children.
 */
BOOL assertIonEventsEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                       size_t index_actual, ASSERTION_TYPE assertion_type);

/**
 * Tests the given IonEventStreams for equivalence, meaning that the corresponding values in each stream
 * must all be equivalent.
 */
BOOL assertIonEventStreamEq(IonEventStream *expected, IonEventStream *actual, ASSERTION_TYPE assertion_type);

#endif