/*
 * Copyright 2009-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#ifndef IONC_ION_EVENT_EQUIVALENCE_H
#define IONC_ION_EVENT_EQUIVALENCE_H

#include <cmath>
#include "ion.h"
#include "ion_event_stream.h"
#include "floating_point_util.h"

#define ION_ENTER_ASSERTIONS

#define ION_EXIT_ASSERTIONS return TRUE

#define ION_ACCUMULATE_ASSERTION(x) if (!(x)) return FALSE;

#define ION_STREAM_EXPECTED_ARG stream_expected
#define ION_INDEX_EXPECTED_ARG index_expected
#define ION_STREAM_ACTUAL_ARG stream_actual
#define ION_INDEX_ACTUAL_ARG index_actual
#define ION_COMPARISON_TYPE_ARG comparison_type
#define ION_RESULT_ARG result

#define ION_FAIL_COMPARISON(message) \
    _ion_event_set_comparison_result(ION_RESULT_ARG, ION_COMPARISON_TYPE_ARG, expected, actual, \
        ION_INDEX_EXPECTED_ARG, ION_INDEX_ACTUAL_ARG, ION_STREAM_EXPECTED_ARG->location, \
        ION_STREAM_ACTUAL_ARG->location, message); \
    return FALSE;

#define ION_FAIL_ASSERTION(message) \
    _ion_event_set_error(ION_RESULT_ARG, ERROR_TYPE_STATE, IERR_INVALID_STATE, message, NULL, NULL, __FILE__, __LINE__); \
    return FALSE;

#define ION_EXPECT_OK(x) \
    if ((x) != IERR_OK) { \
        std::string m = std::string("IERR_OK vs. ") + std::string(ion_error_to_str(x)); \
        ION_FAIL_ASSERTION(m); \
    } \

#define ION_ASSERT(x, m) if (!(x)) { ION_FAIL_ASSERTION(m); }

#define _ION_IS_VALUE_EQ(x, y, assertion) { \
    std::string m; \
    if (!assertion(x, y, &m, ION_RESULT_ARG)) { \
        ION_FAIL_COMPARISON(m); \
    } \
}

#define ION_EXPECT_TRUE(x, m) if (!(x)) { ION_FAIL_COMPARISON(m); }
#define ION_EXPECT_FALSE(x, m) if (x) { ION_FAIL_COMPARISON(m); }
#define ION_EXPECT_EQ(x, y, m) if((x) != (y)) { ION_FAIL_COMPARISON(m); }
#define ION_EXPECT_EVENT_TYPE_EQ(x, y) ION_EXPECT_EQ(x, y, "Event types did not match.")
#define ION_EXPECT_BOOL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonBoolEq)
#define ION_EXPECT_DOUBLE_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonFloatEq)
#define ION_EXPECT_STRING_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonStringEq)
#define ION_EXPECT_SYMBOL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonSymbolEq)
#define ION_EXPECT_INT_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonIntEq)
#define ION_EXPECT_DECIMAL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonDecimalEq)
#define ION_EXPECT_TIMESTAMP_EQ(x, y) _ION_IS_VALUE_EQ(x, y, assertIonTimestampEq)


typedef enum _ion_event_comparison_type {
    COMPARISON_TYPE_EQUIVS = 0,
    COMPARISON_TYPE_NONEQUIVS,
    COMPARISON_TYPE_BASIC,
    COMPARISON_TYPE_UNKNOWN
} ION_EVENT_COMPARISON_TYPE;

typedef iERR (*TIMESTAMP_COMPARISON_FN)(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2, BOOL *is_equal, decContext *pcontext);

/**
 * Global variable that determines which timestamp comparison semantics should be used
 * (data model equivalence vs instant equivalence).
 */
extern TIMESTAMP_COMPARISON_FN g_TimestampEquals;

/**
 * Tests the given IonEventStreams for equivalence, meaning that the corresponding values in each stream
 * must all be equivalent.
 */
BOOL assertIonEventStreamEq(IonEventStream *ION_STREAM_EXPECTED_ARG, IonEventStream *ION_STREAM_ACTUAL_ARG, IonEventResult *ION_RESULT_ARG=NULL);

BOOL testComparisonSets(IonEventStream *ION_STREAM_EXPECTED_ARG, IonEventStream *ION_STREAM_ACTUAL_ARG, ION_EVENT_COMPARISON_TYPE ION_COMPARISON_TYPE_ARG, IonEventResult *ION_RESULT_ARG=NULL);

BOOL assertIonBoolEq(BOOL *expected, BOOL *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);
BOOL assertIonStringEq(ION_STRING *expected, ION_STRING *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);
BOOL assertIonSymbolEq(ION_SYMBOL *expected, ION_SYMBOL *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);
BOOL assertIonIntEq(ION_INT *expected, ION_INT *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);
BOOL assertIonDecimalEq(ION_DECIMAL *expected, ION_DECIMAL *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);
BOOL assertIonFloatEq(double *expected, double *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);

/**
 * Asserts that the given timestamps are equal. Uses g_TimestampEquals as the comparison method.
 */
BOOL assertIonTimestampEq(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message=NULL, IonEventResult *ION_RESULT_ARG=NULL);

#endif //IONC_ION_EVENT_EQUIVALENCE_H
