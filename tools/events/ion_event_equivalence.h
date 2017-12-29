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

#define ION_FAIL_COMPARISON(message) \
    _ion_cli_set_comparison_result(result, comparison_type, expected, actual, index_expected, index_actual, stream_expected->location, stream_actual->location, message); \
    return FALSE;

#define ION_FAIL_ASSERTION(message) \
    _ion_cli_set_error(result, ERROR_TYPE_STATE, IERR_INVALID_STATE, message, NULL, NULL, __FILE__, __LINE__); \
    return FALSE;

#define ION_EXPECT_OK(x) \
    if ((x) != IERR_OK) { \
        std::string m = std::string("IERR_OK vs. ") + std::string(ion_error_to_str(x)); \
        ION_FAIL_ASSERTION(m); \
    } \

#define ION_ASSERT(x, m) if (!(x)) { ION_FAIL_ASSERTION(m); }

#define _ION_IS_VALUE_EQ(x, y, assertion) { \
    std::string m; \
    if (!assertion(x, y, &m, result)) { \
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


typedef enum _comparison_type {
    COMPARISON_TYPE_EQUIVS = 0,
    COMPARISON_TYPE_NONEQUIVS,
    COMPARISON_TYPE_BASIC,
    COMPARISON_TYPE_UNKNOWN
} COMPARISON_TYPE;

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
BOOL assertIonEventStreamEq(IonEventStream *stream_expected, IonEventStream *stream_actual, IonEventResult *result=NULL);

BOOL testComparisonSets(IonEventStream *lhs, IonEventStream *rhs, COMPARISON_TYPE comparison_type, IonEventResult *result=NULL);

BOOL assertIonBoolEq(BOOL *expected, BOOL *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);
BOOL assertIonStringEq(ION_STRING *expected, ION_STRING *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);
BOOL assertIonSymbolEq(ION_SYMBOL *expected, ION_SYMBOL *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);
BOOL assertIonIntEq(ION_INT *expected, ION_INT *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);
BOOL assertIonDecimalEq(ION_DECIMAL *expected, ION_DECIMAL *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);
BOOL assertIonFloatEq(double *expected, double *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);

/**
 * Asserts that the given timestamps are equal. Uses g_TimestampEquals as the comparison method.
 */
BOOL assertIonTimestampEq(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message=NULL, IonEventResult *result=NULL);

#endif //IONC_ION_EVENT_EQUIVALENCE_H
