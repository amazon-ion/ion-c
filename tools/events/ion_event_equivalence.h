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

// TODO for all events/ headers, split into *impl.h

#define ION_ENTER_ASSERTIONS

#define ION_EXIT_ASSERTIONS return TRUE

#define ION_ACCUMULATE_ASSERTION(x) if (!(x)) return FALSE;

#define ION_STREAM_EXPECTED_ARG stream_expected
#define ION_INDEX_EXPECTED_ARG index_expected
#define ION_STREAM_ACTUAL_ARG stream_actual
#define ION_INDEX_ACTUAL_ARG index_actual
#define ION_COMPARISON_TYPE_ARG comparison_type
#define ION_EXPECTED_ARG expected
#define ION_ACTUAL_ARG actual

#define ION_FAIL_COMPARISON(message) \
    _ion_event_set_comparison_result(ION_RESULT_ARG, ION_COMPARISON_TYPE_ARG, ION_EXPECTED_ARG, ION_ACTUAL_ARG, \
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
#define ION_EXPECT_BOOL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_bool)
#define ION_EXPECT_DOUBLE_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_float)
#define ION_EXPECT_STRING_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_string)
#define ION_EXPECT_SYMBOL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_symbol)
#define ION_EXPECT_INT_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_int)
#define ION_EXPECT_DECIMAL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_decimal)
#define ION_EXPECT_TIMESTAMP_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_timestamp)


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
BOOL ion_compare_streams(IonEventStream *stream_expected, IonEventStream *stream_actual, IonEventResult *result = NULL);

/**
 * Compares the comparison sets contained in the given stream based on the given comparison type. A comparison set
 * is a top-level sequence type (list or s-expression) that contains values or embedded streams that will be compared
 * for equivalence or non-equivalence against all other values or embedded streams in that sequence.
 */
BOOL ion_compare_sets(IonEventStream *stream_expected, IonEventStream *stream_actual,
                      ION_EVENT_COMPARISON_TYPE comparison_type, IonEventResult *result = NULL);

// Equivalence checks. If the optional failure_message output parameter is supplied and the given values are not
// equivalent, failure_message will be populated with a message explaining why. If the optional result output parameter
// is supplied, it will be populated with an IonEventErrorDescription if an error occurs during the comparison. If an
// error occurs, the function will always return FALSE.

BOOL ion_equals_bool(BOOL *expected, BOOL *actual, std::string *failure_message = NULL, IonEventResult *result = NULL);


BOOL ion_equals_string(ION_STRING *expected, ION_STRING *actual, std::string *failure_message = NULL,
                       IonEventResult *result = NULL);


BOOL ion_equals_symbol(ION_SYMBOL *expected, ION_SYMBOL *actual, std::string *failure_message = NULL,
                       IonEventResult *result = NULL);


BOOL ion_equals_int(ION_INT *expected, ION_INT *actual, std::string *failure_message = NULL,
                    IonEventResult *result = NULL);


BOOL ion_equals_decimal(ION_DECIMAL *expected, ION_DECIMAL *actual, std::string *failure_message = NULL,
                        IonEventResult *result = NULL);


BOOL ion_equals_float(double *expected, double *actual, std::string *failure_message = NULL,
                      IonEventResult *result = NULL);

/**
 * Uses g_TimestampEquals as the comparison method.
 */
BOOL ion_equals_timestamp(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message = NULL,
                          IonEventResult *result = NULL);

#endif //IONC_ION_EVENT_EQUIVALENCE_H
