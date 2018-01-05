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

#include <set>
#include "ion_event_equivalence.h"
#include "ion_event_util.h"
#include <ion_const.h>
#include <cstdlib>
#include <sstream>

TIMESTAMP_COMPARISON_FN g_TimestampEquals = ion_timestamp_equals;

/**
 * Tests the values starting at the given indices in the given streams (they may be the same) for equivalence
 * under the Ion data model. If the indices start on CONTAINER_START events, this will recursively compare
 * the containers' children.
 */
BOOL assertIonEventsEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                       size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result);

void _ion_cli_set_comparison_result(IonEventResult *result, COMPARISON_TYPE comparison_type, IonEvent *lhs, IonEvent *rhs, size_t lhs_index, size_t rhs_index, std::string lhs_location, std::string rhs_location, std::string message) {
    if (result != NULL) {
        if (ion_event_copy(&result->comparison_result.lhs.event, lhs, lhs_location, result)
            || ion_event_copy(&result->comparison_result.rhs.event, rhs, lhs_location, result)) {
            return;
        }
        result->comparison_result.lhs.event_index = lhs_index;
        result->comparison_result.lhs.location = lhs_location;
        result->comparison_result.rhs.event_index = rhs_index;
        result->comparison_result.rhs.location = rhs_location;
        result->comparison_result.message = message;
        if (comparison_type == COMPARISON_TYPE_BASIC || comparison_type == COMPARISON_TYPE_EQUIVS) {
            result->comparison_result.result = COMPARISON_RESULT_NOT_EQUAL;
        }
        else if (comparison_type == COMPARISON_TYPE_NONEQUIVS) {
            result->comparison_result.result = COMPARISON_RESULT_EQUAL;
        }
        else {
            result->comparison_result.result = COMPARISON_RESULT_ERROR;
        }
        result->has_comparison_result = true;
    }
}

BOOL assertIonScalarEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual, size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    IonEvent *expected = stream_expected->at(index_expected);
    IonEvent *actual = stream_actual->at(index_actual);
    void *expected_value = expected->value;
    void *actual_value = actual->value;
    int tid = ION_TID_INT(expected->ion_type);
    ION_EXPECT_FALSE((expected_value == NULL) ^ (actual_value == NULL), "Only one value was null.");
    if (expected_value == NULL) {
        // Equivalence of ion types has already been tested.
        return TRUE;
    }
    switch (tid) {
        case TID_BOOL:
            ION_EXPECT_BOOL_EQ((BOOL *)expected_value, (BOOL *)actual_value);
            break;
        case TID_POS_INT:
        case TID_NEG_INT:
            ION_EXPECT_INT_EQ((ION_INT *) expected_value, (ION_INT *) actual_value);
            break;
        case TID_FLOAT:
            ION_EXPECT_DOUBLE_EQ((double *) expected_value, (double *) actual_value);
            break;
        case TID_DECIMAL:
            ION_EXPECT_DECIMAL_EQ((ION_DECIMAL *) expected_value, (ION_DECIMAL *) actual_value);
            break;
        case TID_TIMESTAMP:
            ION_EXPECT_TIMESTAMP_EQ((ION_TIMESTAMP *) expected_value, (ION_TIMESTAMP *) actual_value);
            break;
        case TID_SYMBOL:
            ION_EXPECT_SYMBOL_EQ((ION_SYMBOL *) expected_value, (ION_SYMBOL *) actual_value);
            break;
        case TID_STRING:
        case TID_CLOB:
        case TID_BLOB: // Clobs and blobs are stored in ION_STRINGs too...
            ION_EXPECT_STRING_EQ((ION_STRING *) expected_value, (ION_STRING *) actual_value);
            break;
        default:
            ION_ASSERT(FALSE, "Illegal state: unknown ion type.");
    }
    ION_EXIT_ASSERTIONS;
}

/**
 * Asserts that the struct starting at index_expected is a subset of the struct starting at index_actual.
 */
BOOL assertIonStructIsSubset(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                             size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    int target_depth = stream_expected->at(index_expected)->depth;
    index_expected++; // Move past the CONTAINER_START events
    index_actual++;
    size_t index_actual_start = index_actual;
    std::set<size_t> skips;
    BOOL field_names_equal;
    while (index_expected < stream_expected->size()) {
        index_actual = index_actual_start;
        IonEvent *expected = stream_expected->at(index_expected);
        if (expected->event_type == CONTAINER_END && expected->depth == target_depth) {
            break;
        }
        ION_SYMBOL *expected_field_name = expected->field_name;
        ION_ASSERT(expected_field_name != NULL, "Field name in struct cannot be null.");
        while (index_actual < stream_actual->size()) {
            if (skips.count(index_actual) == 0) {
                IonEvent *actual = stream_actual->at(index_actual);
                ION_EXPECT_TRUE(!(actual->event_type == CONTAINER_END && actual->depth == target_depth),
                                    "Did not find matching field for "
                                    + ion_event_symbol_to_string(expected_field_name));
                ION_EXPECT_OK(ion_symbol_is_equal(expected_field_name, actual->field_name, &field_names_equal));
                if (field_names_equal
                    && assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual,
                                         comparison_type, NULL)) { // No need to convey the result.
                    // Skip indices that have already matched. Ensures that structs with different numbers of the same
                    // key:value mapping are not equal.
                    skips.insert(index_actual);
                    break;
                }
            }
            index_actual += valueEventLength(stream_actual, index_actual);
        }
        index_expected += valueEventLength(stream_expected, index_expected);
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonStructEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                       size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    // By asserting that 'expected' and 'actual' are bidirectional subsets, we are asserting they are equivalent.
    ION_ACCUMULATE_ASSERTION(
            assertIonStructIsSubset(stream_expected, index_expected, stream_actual, index_actual, comparison_type, result));
    ION_ACCUMULATE_ASSERTION(
            assertIonStructIsSubset(stream_actual, index_actual, stream_expected, index_expected, comparison_type, result));
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonSequenceEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                         size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    int target_depth = stream_expected->at(index_expected)->depth;
    index_expected++; // Move past the CONTAINER_START events
    index_actual++;
    while (TRUE) {
        IonEvent *expected = stream_expected->at(index_expected);
        IonEvent *actual = stream_actual->at(index_actual);
        // NOTE: symbol tables are only allowed within embedded stream sequences. Logic could be added to verify this.
        if (expected->event_type == SYMBOL_TABLE) {
            index_expected++;
            continue;
        }
        if (actual->event_type == SYMBOL_TABLE) {
            index_actual++;
            continue;
        }
        ION_ACCUMULATE_ASSERTION(
                assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual, comparison_type, result));
        BOOL sequence_end = expected->event_type == CONTAINER_END && expected->depth == target_depth;
        if (sequence_end ^ (actual->event_type == CONTAINER_END && actual->depth == target_depth)) {
            ION_EXPECT_TRUE(FALSE, "Sequences have different lengths.");
        }
        if (sequence_end) {
            break;
        }
        index_expected += valueEventLength(stream_expected, index_expected);
        index_actual += valueEventLength(stream_actual, index_actual);
    }
    ION_EXIT_ASSERTIONS;
}

// TODO just pass through an object containing the comparison type and result?

BOOL assertIonEventsEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                       size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    IonEvent *expected = stream_expected->at(index_expected);
    IonEvent *actual = stream_actual->at(index_actual);

    int tid = ION_TID_INT(expected->ion_type);
    ION_EXPECT_EVENT_TYPE_EQ(expected->event_type, actual->event_type);
    ION_EXPECT_EQ(expected->ion_type, actual->ion_type, "Ion types did not match.");
    ION_EXPECT_EQ(expected->depth, actual->depth, "Depths did not match.");
    ION_EXPECT_SYMBOL_EQ(expected->field_name, actual->field_name);
    ION_EXPECT_EQ(expected->num_annotations, actual->num_annotations, "Number of annotations did not match.");
    for (size_t i = 0; i < expected->num_annotations; i++) {
        ION_EXPECT_SYMBOL_EQ(&expected->annotations[i], &actual->annotations[i]);
    }
    switch (expected->event_type) {
        case STREAM_END:
        case CONTAINER_END:
            break;
        case CONTAINER_START:
            switch (tid) {
                case TID_STRUCT:
                    ION_ACCUMULATE_ASSERTION(
                            assertIonStructEq(stream_expected, index_expected, stream_actual, index_actual,
                                              comparison_type, result));
                    break;
                case TID_SEXP: // intentional fall-through
                case TID_LIST:
                    ION_ACCUMULATE_ASSERTION(
                            assertIonSequenceEq(stream_expected, index_expected, stream_actual, index_actual,
                                                comparison_type, result));
                    break;
                default:
                    ION_ASSERT(FALSE, "Illegal state: container start event with non-container type.");
            }
            break;
        case SCALAR:
            ION_ACCUMULATE_ASSERTION(assertIonScalarEq(stream_expected, index_expected, stream_actual, index_actual,
                                                       comparison_type, result));
            break;
        default:
            ION_ASSERT(FALSE, "Illegal state: unknown event type.");
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonEventSubstreamEq(IonEventStream *stream_expected, size_t expected_start, size_t expected_end, IonEventStream *stream_actual, size_t actual_start, size_t actual_end, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    size_t index_expected = expected_start;
    size_t index_actual = actual_start;
    IonEvent *actual = NULL;
    IonEvent *expected = NULL;
    const COMPARISON_TYPE comparison_type = COMPARISON_TYPE_BASIC;
    while (index_expected < expected_end && index_actual < actual_end) {
        expected = stream_expected->at(index_expected);
        actual = stream_actual->at(index_actual);
        if (expected->event_type == SYMBOL_TABLE) {
            index_expected++;
            continue;
        }
        if (actual->event_type == SYMBOL_TABLE) {
            index_actual++;
            continue;
        }
        ION_ACCUMULATE_ASSERTION(assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual, comparison_type, result));
        index_expected += valueEventLength(stream_expected, index_expected);
        index_actual += valueEventLength(stream_actual, index_actual);
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonEventStreamEq(IonEventStream *stream_expected, IonEventStream *stream_actual, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    ION_ACCUMULATE_ASSERTION(assertIonEventSubstreamEq(stream_expected, 0, stream_expected->size(), stream_actual, 0, stream_actual->size(), result));
    ION_EXIT_ASSERTIONS;
}

typedef BOOL (*COMPARISON_FN)(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual, size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result);

BOOL comparisonEquivs(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual, size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    ION_ACCUMULATE_ASSERTION(assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual, comparison_type, result));
    ION_EXIT_ASSERTIONS;
}

BOOL comparisonNonequivs(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual, size_t index_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    // The corresponding indices are assumed to be equivalent.
    if (index_expected != index_actual) {
        IonEvent *expected = stream_expected->at(index_expected);
        IonEvent *actual = stream_actual->at(index_actual);
        // Since inequality is expected here, passing a NULL result here prevents the comparison report from being
        // polluted. If the events are equal, a comparison result stating such will be added to the report.
        ION_EXPECT_FALSE(assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual, comparison_type, NULL), "Equivalent values in a non-equivs set.");
    }
    ION_EXIT_ASSERTIONS;
}

/**
 * Compares each element in the current container to every other element in the container. The given index refers
 * to the starting index of the first element in the container.
 */
BOOL testEquivsSet(IonEventStream *lhs, size_t lhs_index, IonEventStream *rhs, size_t rhs_index, int target_depth, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    COMPARISON_FN comparison_fn = (comparison_type == COMPARISON_TYPE_EQUIVS) ? comparisonEquivs
                                                                              : comparisonNonequivs;
    size_t i = lhs_index;
    size_t j = rhs_index;
    while (TRUE) {
        if (rhs->at(j)->event_type == CONTAINER_END && rhs->at(j)->depth == target_depth) {
            i += valueEventLength(lhs, i);
            if (lhs->at(i)->event_type == CONTAINER_END && lhs->at(i)->depth == target_depth) {
                break;
            }
            j = rhs_index;
        } else {
            ION_ACCUMULATE_ASSERTION((*comparison_fn)(lhs, i, rhs, j, comparison_type, result));
            j += valueEventLength(rhs, j);
        }
    }
    ION_EXIT_ASSERTIONS;
}

/**
 * The 'embedded_documents' annotation denotes that the current container contains streams of Ion data embedded
 * in string values. These embedded streams are parsed and their resulting IonEventStreams compared.
 */
BOOL testEmbeddedDocumentSet(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual, size_t index_actual, int target_depth, COMPARISON_TYPE comparison_type, size_t *expected_len, size_t *actual_len, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    size_t expected_stream_count = 0;
    size_t actual_stream_count = 0;
    const size_t index_expected_initial = index_expected;
    const size_t index_actual_initial = index_actual;
    while (TRUE) {
        IonEvent *expected = stream_expected->at(index_expected);
        IonEvent *actual = stream_actual->at(index_actual);
        size_t step_expected = ion_event_stream_length(stream_expected, index_expected);
        if (actual->event_type == CONTAINER_END && actual->depth == target_depth) {
            expected_stream_count += 1;
            index_expected += step_expected;
            if (stream_expected->at(index_expected)->event_type == CONTAINER_END && stream_expected->at(index_expected)->depth == target_depth) {
                // Step past the CONTAINER_ENDs
                index_expected++;
                index_actual++;
                break;
            }
            index_actual = index_actual_initial;
            actual_stream_count = 0;
        }
        else {
            size_t step_actual = ion_event_stream_length(stream_actual, index_actual);
            // For non-equivs, embedded streams must not be compared reflexively.
            if (comparison_type != COMPARISON_TYPE_NONEQUIVS || expected_stream_count != actual_stream_count) {
                if (comparison_type == COMPARISON_TYPE_EQUIVS) {
                    if (step_expected == 1 ^ step_actual == 1) {
                        ION_EXPECT_TRUE(step_expected == 1 && step_actual == 1, "Only one embedded stream represents an empty stream.");
                    }
                    else if (step_expected > 1 && step_actual > 1) {
                        ION_ACCUMULATE_ASSERTION(
                                assertIonEventSubstreamEq(stream_expected, index_expected, index_expected + step_expected,
                                                          stream_actual, index_actual, index_actual + step_actual, result)
                        );
                    }
                }
                else {
                    ION_ASSERT(comparison_type == COMPARISON_TYPE_NONEQUIVS, "Invalid embedded documents comparison type.");
                    if (step_expected == 1 && step_actual == 1) {
                        ION_EXPECT_FALSE(step_expected == 1 && step_actual == 1, "Both embedded streams are empty stream in a non-equivs set.")
                    }
                    else if (step_expected > 1 && step_actual > 1) {
                        ION_EXPECT_FALSE(
                                assertIonEventSubstreamEq(stream_expected, index_expected, index_expected + step_expected,
                                                          stream_actual, index_actual, index_actual + step_actual, NULL),
                                "Equivalent streams in a non-equivs set."); // Result not needed.
                    }
                }
            }
            actual_stream_count += 1;
            index_actual += step_actual;
        }
    }
    *expected_len = index_expected - index_expected_initial;
    *actual_len = index_actual - index_actual_initial;
    ION_EXIT_ASSERTIONS;
}

/**
 * Comparison sets are conveyed as sequences. Each element in the sequence must be equivalent to all other elements
 * in the same sequence.
 */
BOOL testComparisonSets(IonEventStream *stream_expected, IonEventStream *stream_actual, COMPARISON_TYPE comparison_type, IonEventResult *result) {
    ION_ENTER_ASSERTIONS;
    size_t index_expected = 0, index_actual = 0;
    IonEvent *expected = stream_expected->at(index_expected);
    IonEvent *actual = stream_actual->at(index_expected);
    ION_EXPECT_TRUE(!(stream_expected->size() == 0 ^ stream_actual->size()== 0), "Only one of the streams was empty.");
    if (stream_expected->size() == 0) return TRUE;
    while (TRUE) {
        if (index_expected == stream_expected->size() - 1) {
            // Even if the streams' corresponding sets have different number of elements, the loop will reach the
            // end of each stream at the same time as long as the streams have the same number of sets. And if they
            // don't have the same number of sets, an error is raised.
            ION_EXPECT_EQ(stream_actual->size() - 1, index_actual, "Only one of the streams reached its end.");
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, expected->event_type);
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, actual->event_type);
            break;
        }
        else if (index_actual == stream_actual->size() - 1) {
            ION_EXPECT_EQ(stream_expected->size() - 1, index_expected, "Only one of the streams reached its end.");
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, actual->event_type);
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, expected->event_type);
            break;
        }
        else {
            ION_ASSERT(CONTAINER_START == expected->event_type, "Comparison sets must be lists or s-expressions.");
            ION_ASSERT((tid_SEXP == expected->ion_type) || (tid_LIST == expected->ion_type), "Comparison sets must be lists or s-expressions.");
            ION_ASSERT(CONTAINER_START == actual->event_type, "Comparison sets must be lists or s-expressions.");
            ION_ASSERT((tid_SEXP == actual->ion_type) || (tid_LIST == actual->ion_type), "Comparison sets must be lists or s-expressions.");
            ION_STRING *lhs_annotation = (expected->num_annotations > 0) ? &expected->annotations[0].value : NULL;
            ION_STRING *rhs_annotation = (actual->num_annotations > 0) ? &actual->annotations[0].value : NULL;
            size_t step_lhs;
            size_t step_rhs;
            if (lhs_annotation && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, lhs_annotation)) {
                ION_ASSERT(rhs_annotation && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, rhs_annotation), "Embedded streams set expected.");
                // Skip past the CONTAINER_START events.
                index_expected++;
                index_actual++;
                ION_ACCUMULATE_ASSERTION(testEmbeddedDocumentSet(stream_expected, index_expected, stream_actual, index_actual, 0, comparison_type, &step_lhs, &step_rhs, result));
            } else {
                ION_ASSERT(!(rhs_annotation && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, rhs_annotation)), "Embedded streams set not expected.");
                step_lhs = valueEventLength(stream_expected, index_expected);
                step_rhs = valueEventLength(stream_actual, index_actual);
                ION_ACCUMULATE_ASSERTION(testEquivsSet(stream_expected, index_expected + 1, stream_actual, index_actual + 1, 0, comparison_type, result));
            }
            index_expected += step_lhs;
            index_actual += step_rhs;
            expected = stream_expected->at(index_expected);
            actual = stream_actual->at(index_actual);
        }
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonBoolEq(BOOL *expected, BOOL *actual, std::string *failure_message, IonEventResult *result) {
    if (!(expected == NULL ^ actual == NULL)) {
        if (expected == NULL) {
            return TRUE;
        }
        if (*expected == *actual) {
            return TRUE;
        }
    }
    if (failure_message) {
        *failure_message = std::string("")
                           + (expected ? ((*expected) ? "true" : "false") : "null") + " vs. "
                           + (actual ? ((*actual) ? "true" : "false") : "null");
    }
    return FALSE;
}

BOOL assertIonStringEq(ION_STRING *expected, ION_STRING *actual, std::string *failure_message, IonEventResult *result) {
    if (!(expected == NULL ^ actual == NULL)) {
        if (expected == NULL || ion_string_is_equal(expected, actual)) {
            return TRUE;
        }
    }
    if (failure_message) {
        *failure_message = ION_EVENT_STRING_OR_NULL(expected) + "  vs. " + ION_EVENT_STRING_OR_NULL(actual);
    }
    return FALSE;
}

BOOL assertIonSymbolEq(ION_SYMBOL *expected, ION_SYMBOL *actual, std::string *failure_message, IonEventResult *result) {
    if (!(expected == NULL ^ actual == NULL)) {
        if (expected == NULL) {
            return TRUE;
        }
        BOOL is_equal;
        ION_EXPECT_OK(ion_symbol_is_equal(expected, actual, &is_equal));
        if (is_equal) {
            return TRUE;
        }
    }
    if (failure_message) {
        *failure_message = ion_event_symbol_to_string(expected) + " vs. " + ion_event_symbol_to_string(actual);
    }
    return FALSE;
}

char *ionIntToString(ION_INT *value) {
    SIZE len, written;
    ion_int_char_length(value, &len);
    char *int_str = (char *)malloc(len * sizeof(char));
    ion_int_to_char(value, (BYTE *)int_str, len, &written);
    return int_str;
}

BOOL assertIonIntEq(ION_INT *expected, ION_INT *actual, std::string *failure_message, IonEventResult *result) {
    int int_comparison = 0;
    char *expected_str = NULL;
    char *actual_str = NULL;
    ION_EXPECT_OK(ion_int_compare(expected, actual, &int_comparison));
    if (int_comparison == 0) {
        return TRUE;
    }
    if (failure_message) {
        expected_str = ionIntToString(expected);
        actual_str = ionIntToString(actual);
        *failure_message = std::string("") + expected_str + " vs. " + actual_str;
        free(expected_str);
        free(actual_str);
    }
    return FALSE;
}

BOOL assertIonDecimalEq(ION_DECIMAL *expected, ION_DECIMAL *actual, std::string *failure_message, IonEventResult *result) {
    BOOL decimal_equals;
    ION_EXPECT_OK(ion_decimal_equals(expected, actual, &g_IonEventDecimalContext, &decimal_equals));
    if (decimal_equals) {
        return TRUE;
    }
    if (failure_message) {
        char expected_str[ION_EVENT_DECIMAL_MAX_STRLEN];
        char actual_str[ION_EVENT_DECIMAL_MAX_STRLEN];
        ION_EXPECT_OK(ion_decimal_to_string(expected, expected_str));
        ION_EXPECT_OK(ion_decimal_to_string(actual, actual_str));
        *failure_message = std::string("") + expected_str + " vs. " + actual_str;
    }
    return FALSE;
}

BOOL assertIonTimestampEq(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message, IonEventResult *result) {
    BOOL timestamps_equal;
    ION_EXPECT_OK(g_TimestampEquals(expected, actual, &timestamps_equal, &g_IonEventDecimalContext));
    if (timestamps_equal) {
        return TRUE;
    }
    if (failure_message) {
        char expected_str[ION_MAX_TIMESTAMP_STRING];
        char actual_str[ION_MAX_TIMESTAMP_STRING];
        SIZE expected_str_len, actual_str_len;
        ION_EXPECT_OK(ion_timestamp_to_string(expected, expected_str, ION_MAX_TIMESTAMP_STRING, &expected_str_len,
                                              &g_IonEventDecimalContext));
        ION_EXPECT_OK(ion_timestamp_to_string(actual, actual_str, ION_MAX_TIMESTAMP_STRING, &actual_str_len,
                                              &g_IonEventDecimalContext));
        *failure_message =
                std::string(expected_str, (size_t) expected_str_len)
                + " vs. "
                + std::string(actual_str, (size_t) actual_str_len);
    }
    return FALSE;
}

BOOL assertIonFloatEq(double *expected, double *actual, std::string *failure_message, IonEventResult *result) {
    do {
        if (!(expected == NULL ^ actual == NULL)) {
            if (expected == NULL) {
                return TRUE;
            }
            double lhs = *expected;
            double rhs = *actual;
            if (std::isnan(lhs) ^ std::isnan(rhs)) {
                break;
            }
            if (std::isnan(lhs)) return TRUE;
            if (ion_float_is_negative_zero(lhs) ^ ion_float_is_negative_zero(rhs)) {
                break;
            }
            if (ion_float_is_negative_zero(lhs)) return TRUE;
            const FloatingPoint<double> lhs_fp(lhs), rhs_fp(rhs);
            if (!(lhs_fp.AlmostEquals(rhs_fp) || rhs_fp.AlmostEquals(lhs_fp))) {
                break;
            }
            return TRUE;
        }
    } while (FALSE);
    if (failure_message) {
        std::string expected_str, actual_str;
        if (expected == NULL) {
            expected_str = "NULL";
        } else {
            std::ostringstream ss;
            ss << *expected;
            expected_str = ss.str();
        }
        if (actual == NULL) {
            actual_str = "NULL";
        } else {
            std::ostringstream ss;
            ss << *actual;
            actual_str = ss.str();
        }
        *failure_message = expected_str + " vs. " + actual_str;
    }
    return FALSE;
}
