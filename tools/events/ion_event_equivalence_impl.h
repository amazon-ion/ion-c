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

#ifndef IONC_ION_EVENT_EQUIVALENCE_IMPL_H
#define IONC_ION_EVENT_EQUIVALENCE_IMPL_H

#define ION_PASS_ASSERTIONS return TRUE

#define ION_CHECK_ASSERTION(x) if (!(x)) return FALSE;

#define ION_STREAM_EXPECTED_ARG stream_expected
#define ION_INDEX_EXPECTED_ARG index_expected
#define ION_STREAM_ACTUAL_ARG stream_actual
#define ION_INDEX_ACTUAL_ARG index_actual
#define ION_COMPARISON_TYPE_ARG comparison_type
#define ION_EXPECTED_ARG expected
#define ION_ACTUAL_ARG actual

/**
 * Sets the current IonEventResult's comparison_result with context about the comparison failure and returns.
 *
 * Note: We don't write comparison report for non-equivs mode here.
 */
#define ION_FAIL_COMPARISON(message) \
    if (ION_COMPARISON_TYPE_ARG != COMPARISON_TYPE_NONEQUIVS) { \
        _ion_event_set_comparison_result(ION_RESULT_ARG, ION_COMPARISON_TYPE_ARG, ION_EXPECTED_ARG, ION_ACTUAL_ARG, \
            ION_INDEX_EXPECTED_ARG, ION_INDEX_ACTUAL_ARG, ION_STREAM_EXPECTED_ARG->location, \
            ION_STREAM_ACTUAL_ARG->location, message); \
    } \
    return FALSE;

/**
 * Sets the current IonEventResult's comparison_result with context about the comparison failure and returns.
 */
#define ION_FAIL_COMPARISON_FOR_NON_EQUIVS(message) \
    _ion_event_set_comparison_result(ION_RESULT_ARG, ION_COMPARISON_TYPE_ARG, ION_EXPECTED_ARG, ION_ACTUAL_ARG, \
        ION_INDEX_EXPECTED_ARG, ION_INDEX_ACTUAL_ARG, ION_STREAM_EXPECTED_ARG->location, \
        ION_STREAM_ACTUAL_ARG->location, message); \
    return FALSE;

/**
 * Sets the desired index of event for IonEventResult's comparison_result context.
 */
#define ION_FAIL_COMPARISON_WITH_INDEX(message, index_expected, index_actual) \
    _ion_event_set_comparison_result(ION_RESULT_ARG, ION_COMPARISON_TYPE_ARG, stream_expected->at(index_expected), \
        stream_actual->at(index_actual), index_expected, index_actual, ION_STREAM_EXPECTED_ARG->location, \
        ION_STREAM_ACTUAL_ARG->location, message); \
    return FALSE;

/**
 * Sets the current IonEventResult's error_description with context about the error and returns.
 */
#define ION_FAIL_ASSERTION(message, loc) \
    _ion_event_set_error(ION_RESULT_ARG, ERROR_TYPE_STATE, IERR_INVALID_STATE, message, loc, NULL, __FILE__, __LINE__); \
    return FALSE;

/**
 * Fails in error (setting the error_description) if the given error code is not IERR_OK.
 */
#define ION_EXPECT_OK(x) \
    if ((x) != IERR_OK) { \
        std::string m = std::string("IERR_OK vs. ") + std::string(ion_error_to_str(x)); \
        ION_FAIL_ASSERTION(m, NULL); \
    } \

/**
 * Fails in error if the first argument is false. Conveys the given message in the error_description.
 */
#define ION_ASSERT(x, m) \
    if (!(x)) { \
        std::string ION_ERROR_LOCATION_VAR = ION_STREAM_EXPECTED_ARG->location + ";" + ION_STREAM_ACTUAL_ARG->location; \
        ION_FAIL_ASSERTION(m, &ION_ERROR_LOCATION_VAR); \
    }

#define _ION_IS_VALUE_EQ(x, y, assertion) { \
    std::string m; \
    if (!assertion(x, y, &m, ION_RESULT_ARG)) { \
        ION_FAIL_COMPARISON(m); \
    } \
}

// Equivalence assertions. Each sets the comparison_result and returns in the event of inequality.
#define ION_EXPECT_TRUE(x, m) if (!(x)) { ION_FAIL_COMPARISON(m); }
#define ION_EXPECT_TRUE_WITH_INDEX(x, m, e, a) if (!(x)) { ION_FAIL_COMPARISON_WITH_INDEX(m, e, a); }
#define ION_EXPECT_FALSE(x, m) if (x) { ION_FAIL_COMPARISON(m); }
#define ION_EXPECT_FALSE_FOR_NON_EQUIVS(x, m) if (x) { ION_FAIL_COMPARISON_FOR_NON_EQUIVS(m); } //for non-equivs mode only
#define ION_EXPECT_EQ(x, y, m) if((x) != (y)) { ION_FAIL_COMPARISON(m); }
#define ION_EXPECT_EVENT_TYPE_EQ(x, y) ION_EXPECT_EQ(x, y, "Event types did not match.")
#define ION_EXPECT_BOOL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_bool)
#define ION_EXPECT_DOUBLE_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_float)
#define ION_EXPECT_STRING_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_string)
#define ION_EXPECT_SYMBOL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_symbol)
#define ION_EXPECT_INT_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_int)
#define ION_EXPECT_DECIMAL_EQ(x, y) _ION_IS_VALUE_EQ(x, y, ion_equals_decimal)
#define ION_EXPECT_TIMESTAMP_EQ(x, y) \
    if (ION_COMPARISON_TYPE_ARG == COMPARISON_TYPE_EQUIVTIMELINE) { \
        _ION_IS_VALUE_EQ(x, y, ion_equals_timestamp_instant); \
    } \
    else { \
        _ION_IS_VALUE_EQ(x, y, ion_equals_timestamp); \
    }

#define ION_EVENT_EQUIVALENCE_PARAMS \
IonEventStream *ION_STREAM_EXPECTED_ARG, size_t ION_INDEX_EXPECTED_ARG, IonEventStream *ION_STREAM_ACTUAL_ARG, \
size_t ION_INDEX_ACTUAL_ARG, ION_EVENT_COMPARISON_TYPE ION_COMPARISON_TYPE_ARG, IonEventResult *ION_RESULT_ARG

#define ION_EVENT_EQUIVALENCE_ARGS \
ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG, \
ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG, ION_COMPARISON_TYPE_ARG, ION_RESULT_ARG

#define ION_GET_EXPECTED ION_STREAM_EXPECTED_ARG->at(ION_INDEX_EXPECTED_ARG)
#define ION_GET_ACTUAL ION_STREAM_ACTUAL_ARG->at(ION_INDEX_ACTUAL_ARG)
#define ION_SET_EXPECTED IonEvent *ION_EXPECTED_ARG = ION_GET_EXPECTED
#define ION_SET_ACTUAL IonEvent *ION_ACTUAL_ARG = ION_GET_ACTUAL
#define ION_PREPARE_COMPARISON ION_SET_EXPECTED; ION_SET_ACTUAL
#define ION_NEXT_EXPECTED_INDEX ION_INDEX_EXPECTED_ARG++
#define ION_NEXT_ACTUAL_INDEX ION_INDEX_ACTUAL_ARG++
#define ION_NEXT_INDICES ION_NEXT_EXPECTED_INDEX; ION_NEXT_ACTUAL_INDEX
#define ION_ACTUAL_VALUE_LENGTH ion_event_value_length(ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG)
#define ION_EXPECTED_VALUE_LENGTH ion_event_value_length(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG)
#define ION_NEXT_ACTUAL_VALUE_INDEX ION_INDEX_ACTUAL_ARG += ION_ACTUAL_VALUE_LENGTH
#define ION_NEXT_EXPECTED_VALUE_INDEX ION_INDEX_EXPECTED_ARG += ION_EXPECTED_VALUE_LENGTH
#define ION_NEXT_VALUE_INDICES ION_NEXT_EXPECTED_VALUE_INDEX; ION_NEXT_ACTUAL_VALUE_INDEX

void _ion_event_set_comparison_result(IonEventResult *result, ION_EVENT_COMPARISON_TYPE comparison_type, IonEvent *lhs,
                                      IonEvent *rhs, size_t lhs_index, size_t rhs_index, std::string lhs_location,
                                      std::string rhs_location, std::string message);

#endif //IONC_ION_EVENT_EQUIVALENCE_IMPL_H
