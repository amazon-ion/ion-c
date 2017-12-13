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

TIMESTAMP_COMPARISON_FN g_TimestampEquals = ion_timestamp_equals;

BOOL assertIonScalarEq(IonEvent *expected, IonEvent *actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    void *expected_value = expected->value;
    void *actual_value = actual->value;
    int tid = ION_TID_INT(expected->ion_type);
    ION_EXPECT_FALSE((expected_value == NULL) ^ (actual_value == NULL));
    if (expected_value == NULL) {
        // Equivalence of ion types has already been tested.
        return TRUE;
    }
    switch (tid) {
        case TID_BOOL:
            ION_EXPECT_EQ(*(BOOL *) expected_value, *(BOOL *) actual_value);
            break;
        case TID_POS_INT:
        case TID_NEG_INT:
            ION_EXPECT_INT_EQ((ION_INT *) expected_value, (ION_INT *) actual_value);
            break;
        case TID_FLOAT:
            ION_EXPECT_DOUBLE_EQ(*(double *) expected_value, *(double *) actual_value);
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
            ION_EXPECT_FALSE("Illegal state: unknown ion type.");
    }
    ION_EXIT_ASSERTIONS;
}

/**
 * Asserts that the struct starting at index_expected is a subset of the struct starting at index_actual.
 */
BOOL assertIonStructIsSubset(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                             size_t index_actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    int target_depth = stream_expected->at(index_expected)->depth;
    index_expected++; // Move past the CONTAINER_START events
    index_actual++;
    size_t index_actual_start = index_actual;
    std::set<size_t> skips;
    BOOL field_names_equal;
    while (TRUE) {
        index_actual = index_actual_start;
        IonEvent *expected = stream_expected->at(index_expected);
        if (expected->event_type == CONTAINER_END && expected->depth == target_depth) {
            break;
        }
        ION_SYMBOL *expected_field_name = expected->field_name;
        ION_EXPECT_TRUE(expected_field_name != NULL);
        while (TRUE) {
            if (skips.count(index_actual) == 0) {
                IonEvent *actual = stream_actual->at(index_actual);
                ION_ASSERT(!(actual->event_type == CONTAINER_END && actual->depth == target_depth),
                           "Reached end of struct before finding matching field.");
                ION_EXPECT_OK(ion_symbol_is_equal(expected_field_name, actual->field_name, &field_names_equal));
                if (field_names_equal
                    && assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual,
                                         ASSERTION_TYPE_SET_FLAG)) {
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
                       size_t index_actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    // By asserting that 'expected' and 'actual' are bidirectional subsets, we are asserting they are equivalent.
    ION_ACCUMULATE_ASSERTION(
            assertIonStructIsSubset(stream_expected, index_expected, stream_actual, index_actual, assertion_type));
    ION_ACCUMULATE_ASSERTION(
            assertIonStructIsSubset(stream_actual, index_actual, stream_expected, index_expected, assertion_type));
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonSequenceEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                         size_t index_actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    int target_depth = stream_expected->at(index_expected)->depth;
    index_expected++; // Move past the CONTAINER_START events
    index_actual++;
    while (TRUE) {
        ION_ACCUMULATE_ASSERTION(
                assertIonEventsEq(stream_expected, index_expected, stream_actual, index_actual, assertion_type));
        if (stream_expected->at(index_expected)->event_type == CONTAINER_END &&
            stream_expected->at(index_expected)->depth == target_depth) {
            ION_EXPECT_TRUE(stream_actual->at(index_actual)->event_type == CONTAINER_END &&
                            stream_actual->at(index_actual)->depth == target_depth);
            break;
        }
        index_expected += valueEventLength(stream_expected, index_expected);
        index_actual += valueEventLength(stream_actual, index_actual);
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonEventsEq(IonEventStream *stream_expected, size_t index_expected, IonEventStream *stream_actual,
                       size_t index_actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    IonEvent *expected = stream_expected->at(index_expected);
    IonEvent *actual = stream_actual->at(index_actual);
    int tid = ION_TID_INT(expected->ion_type);
    ION_EXPECT_EQ(expected->event_type, actual->event_type);
    ION_EXPECT_EQ(expected->ion_type, actual->ion_type);
    ION_EXPECT_EQ(expected->depth, actual->depth);
    ION_EXPECT_SYMBOL_EQ(expected->field_name, actual->field_name);
    ION_EXPECT_EQ(expected->num_annotations, actual->num_annotations);
    if (expected->num_annotations == actual->num_annotations) {
        for (size_t i = 0; i < expected->num_annotations; i++) {
            ION_EXPECT_SYMBOL_EQ(expected->annotations[i], actual->annotations[i]);
        }
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
                                              assertion_type));
                    break;
                case TID_SEXP: // intentional fall-through
                case TID_LIST:
                    ION_ACCUMULATE_ASSERTION(
                            assertIonSequenceEq(stream_expected, index_expected, stream_actual, index_actual,
                                                assertion_type));
                    break;
                default:
                ION_EXPECT_FALSE("Illegal state: container start event with non-container type.");
            }
            break;
        case SCALAR:
            ION_ACCUMULATE_ASSERTION(assertIonScalarEq(expected, actual, assertion_type));
            break;
        default:
            ION_EXPECT_FALSE("Illegal state: unknown event type.");
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonEventStreamEq(IonEventStream *expected, IonEventStream *actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    size_t index_expected = 0;
    size_t index_actual = 0;
    while (index_expected < expected->size() && index_actual < actual->size()) {
        if (expected->at(index_expected)->event_type == SYMBOL_TABLE) {
            index_expected++;
            continue;
        }
        if (actual->at(index_actual)->event_type == SYMBOL_TABLE) {
            index_actual++;
            continue;
        }
        ION_ACCUMULATE_ASSERTION(assertIonEventsEq(expected, index_expected, actual, index_actual, assertion_type));
        index_expected += valueEventLength(expected, index_expected);
        index_actual += valueEventLength(actual, index_actual);
    }
    ION_ASSERT(expected->size() == index_expected, "Expected stream did not reach its end.");
    ION_ASSERT(actual->size() == index_actual, "Actual stream did not reach its end.");
    ION_EXIT_ASSERTIONS;
}

BOOL doublesEq(double expected, double actual) {
    const FloatingPoint<double> lhs(expected), rhs(actual);
    return lhs.AlmostEquals(rhs) || rhs.AlmostEquals(lhs);
}

char *ionIntToString(ION_INT *value) {
    SIZE len, written;
    ion_int_char_length(value, &len);
    char *int_str = (char *)malloc(len * sizeof(char));
    ion_int_to_char(value, (BYTE *)int_str, len, &written);
    return int_str;
}

char *ionStringToString(ION_STRING *value) {
    BYTE *src, *dst;
    SIZE len;
    if (!ION_STRING_IS_NULL(value)) {
        src = value->value;
        len = value->length;
    }
    else {
        src = (BYTE *)"NULL";
        len = 4;
    }
    dst = (BYTE *)malloc(((size_t)len + 1) * sizeof(char));
    if (!dst) return NULL;

    memcpy(dst, src, (size_t)len);
    dst[len] = 0;

    return (char *)dst;
}

typedef BOOL (*COMPARISON_FN)(IonEventStream *stream, size_t index_expected, size_t index_actual, ASSERTION_TYPE assertion_type);

BOOL comparisonEquivs(IonEventStream *stream, size_t index_expected, size_t index_actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
#ifdef ION_ASSERT_ENABLE_GTEST
    ASSERTION_TYPE individual_assertion = ASSERTION_TYPE_NORMAL;
#else
    // TODO check
    ASSERTION_TYPE individual_assertion = ASSERTION_TYPE_SET_FLAG;
#endif
    ION_EXPECT_TRUE_MSG(assertIonEventsEq(stream, index_expected, stream, index_actual, individual_assertion),
                        std::string("Test: ") << g_CurrentTest
                                              << " comparing events at index " << index_expected << " and " << index_actual);
    ION_EXIT_ASSERTIONS;
}

BOOL comparisonNonequivs(IonEventStream *stream, size_t index_expected, size_t index_actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    ION_EXPECT_FALSE_MSG(assertIonEventsEq(stream, index_expected, stream, index_actual, ASSERTION_TYPE_SET_FLAG),
                         std::string("Test: ") << g_CurrentTest
                                               << " comparing events at index " << index_expected << " and " << index_actual);
    ION_EXIT_ASSERTIONS;
}

/**
 * Compares each element in the current container to every other element in the container. The given index refers
 * to the starting index of the first element in the container.
 */
BOOL testEquivsSet(IonEventStream *stream, size_t index, int target_depth, COMPARISON_TYPE comparison_type, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    // TODO might as well compare each element to itself too (for equivs only). This isn't done currently.
    COMPARISON_FN comparison_fn = (comparison_type == COMPARISON_TYPE_EQUIVS) ? comparisonEquivs
                                                                              : comparisonNonequivs;
    size_t i = index;
    size_t j = index;
    size_t step = 1;
    BOOL are_containers = stream->at(i)->event_type == CONTAINER_START;
    while (TRUE) {
        if (are_containers) {
            // Find the start of the next container to compare its events for equivalence with this one.
            step = valueEventLength(stream, j);
        }
        j += step;
        if (stream->at(j)->event_type == CONTAINER_END && stream->at(j)->depth == target_depth) {
            i += valueEventLength(stream, i);
            j = i;
        } else {
            ION_ACCUMULATE_ASSERTION((*comparison_fn)(stream, i, j, assertion_type));
        }
        if (stream->at(i)->event_type == CONTAINER_END && stream->at(i)->depth == target_depth) {
            break;
        }
    }
    ION_EXIT_ASSERTIONS;
}

/**
 * The 'embedded_documents' annotation denotes that the current container contains streams of Ion data embedded
 * in string values. These embedded streams are parsed and their resulting IonEventStreams compared.
 */
BOOL testEmbeddedDocumentSet(IonEventStream *stream, size_t index, int target_depth, COMPARISON_TYPE comparison_type) {
    // TODO could roundtrip the embedded event streams instead of the strings representing them
    ION_ENTER_ASSERTIONS;
#ifdef ION_ASSERT_ENABLE_GTEST
    ASSERTION_TYPE assertion_type = (comparison_type == COMPARISON_TYPE_EQUIVS) ? ASSERTION_TYPE_NORMAL
                                                                                : ASSERTION_TYPE_SET_FLAG;
#else
    // TODO check this
    ASSERTION_TYPE assertion_type = ASSERTION_TYPE_SET_FLAG;
#endif
    size_t i = index;
    size_t j = index;
    while (TRUE) {
        j += 1;
        if (stream->at(j)->event_type == CONTAINER_END && stream->at(j)->depth == target_depth) {
            i += 1;
            j = i;
        } else {
            IonEvent *expected_event = stream->at(i);
            IonEvent *actual_event = stream->at(j);
            ION_ASSERT(tid_STRING == expected_event->ion_type, "Embedded documents must be strings.");
            ION_ASSERT(tid_STRING == actual_event->ion_type, "Embedded documents must be strings.");
            char *expected_ion_string = ionStringToString((ION_STRING *)expected_event->value);
            char *actual_ion_string = ionStringToString((ION_STRING *)actual_event->value);
            IonEventStream expected_stream, actual_stream;
            ION_ASSERT(IERR_OK == read_value_stream_from_string(expected_ion_string, &expected_stream, NULL),
                       "Embedded document failed to parse");
            ION_ASSERT(IERR_OK == read_value_stream_from_string(actual_ion_string, &actual_stream, NULL),
                       "Embedded document failed to parse");
            ION_EXPECT_EQ_MSG(comparison_type == COMPARISON_TYPE_EQUIVS, assertIonEventStreamEq(&expected_stream, &actual_stream, assertion_type),
                              std::string("Error comparing streams \"") << expected_ion_string << "\" and \""
                                                                        << actual_ion_string << "\".");
            free(expected_ion_string);
            free(actual_ion_string);
        }
        if (stream->at(i)->event_type == CONTAINER_END && stream->at(i)->depth == target_depth) {
            break;
        }
    }
    ION_EXIT_ASSERTIONS;
}

const char *embeddedDocumentsAnnotation = "embedded_documents";

// TODO figure out how to pass failure context upward. Also, generalize to compare across different streams.

/**
 * Comparison sets are conveyed as sequences. Each element in the sequence must be equivalent to all other elements
 * in the same sequence.
 */
BOOL testComparisonSets(IonEventStream *stream, COMPARISON_TYPE comparison_type, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    size_t i = 0;
    while (i < stream->size()) {
        IonEvent *event = stream->at(i);
        if (i == stream->size() - 1) {
            ION_EXPECT_EQ(STREAM_END, event->event_type);
            i++;
        } else {
            ION_EXPECT_EQ(CONTAINER_START, event->event_type);
            ION_EXPECT_TRUE((tid_SEXP == event->ion_type) || (tid_LIST == event->ion_type));
            size_t step = valueEventLength(stream, i);
            char *first_annotation = (event->num_annotations == 1) ? ionStringToString(&event->annotations[0]->value) : NULL;
            if (first_annotation && !strcmp(first_annotation, embeddedDocumentsAnnotation)) {
                ION_ACCUMULATE_ASSERTION(testEmbeddedDocumentSet(stream, i + 1, 0, comparison_type));
            } else {
                ION_ACCUMULATE_ASSERTION(testEquivsSet(stream, i + 1, 0, comparison_type, assertion_type));
            }
            if (first_annotation) {
                free(first_annotation);
            }
            i += step;
        }
    }
    ION_EXIT_ASSERTIONS;
}

#ifdef ION_ASSERT_ENABLE_GTEST
std::string g_CurrentTest = "NONE";

::testing::AssertionResult assertIonStringEq(ION_STRING *expected, ION_STRING *actual) {
    char *expected_str = NULL;
    char *actual_str = NULL;
    if (!(expected == NULL ^ actual == NULL)) {
        if (expected == NULL || ion_string_is_equal(expected, actual)) {
            return ::testing::AssertionSuccess();
        }
    }
    expected_str = ionStringToString(expected);
    actual_str = ionStringToString(actual);
    ::testing::AssertionResult result = ::testing::AssertionFailure()
            << std::string("") << expected_str << "  vs. " << actual_str;
    free(expected_str);
    free(actual_str);
    return result;
}

::testing::AssertionResult assertIonSymbolEq(ION_SYMBOL *expected, ION_SYMBOL *actual) {
    char *expected_str = NULL;
    char *actual_str = NULL;
    char *expected_import_str = NULL;
    char *actual_import_str = NULL;
    if (!(expected == NULL ^ actual == NULL)) {
        if (expected == NULL) {
            return ::testing::AssertionSuccess();
        }
        BOOL is_equal;
        ION_EXPECT_OK(ion_symbol_is_equal(expected, actual, &is_equal));
        if (is_equal) {
            return ::testing::AssertionSuccess();
        }
    }
    expected_str = ionStringToString(&expected->value);
    actual_str = ionStringToString(&actual->value);
    expected_import_str = ionStringToString(&expected->import_location.name);
    actual_import_str = ionStringToString(&actual->import_location.name);
    ::testing::AssertionResult result = ::testing::AssertionFailure()
            << std::string("(text=") << expected_str << ", local_sid=" << expected->sid
                << ", location=(" << expected_import_str << ", " << expected->import_location.location << "))"
            << " vs. "
            << "(text=" << actual_str << ", local_sid=" << actual->sid
                << ", location=(" << actual_import_str << ", " << actual->import_location.location << "))";
    free(expected_str);
    free(actual_str);
    return result;
}

::testing::AssertionResult assertIonIntEq(ION_INT *expected, ION_INT *actual) {
    int int_comparison = 0;
    char *expected_str = NULL;
    char *actual_str = NULL;
    EXPECT_EQ(IERR_OK, ion_int_compare(expected, actual, &int_comparison));
    if (int_comparison == 0) {
        return ::testing::AssertionSuccess();
    }
    expected_str = ionIntToString(expected);
    actual_str = ionIntToString(actual);
    ::testing::AssertionResult result = ::testing::AssertionFailure()
            << std::string("") << expected_str << " vs. " << actual_str;
    free(expected_str);
    free(actual_str);
    return result;
}

::testing::AssertionResult assertIonDecimalEq(ION_DECIMAL *expected, ION_DECIMAL *actual) {
    BOOL decimal_equals;
    ION_EXPECT_OK(ion_decimal_equals(expected, actual, &g_IonEventDecimalContext, &decimal_equals));
    if (decimal_equals) {
        return ::testing::AssertionSuccess();
    }
    char expected_str[ION_EVENT_DECIMAL_MAX_STRLEN];
    char actual_str[ION_EVENT_DECIMAL_MAX_STRLEN];
    ION_EXPECT_OK(ion_decimal_to_string(expected, expected_str));
    ION_EXPECT_OK(ion_decimal_to_string(actual, actual_str));
    return ::testing::AssertionFailure()
            << std::string("") << expected_str << " vs. " << actual_str;
}

::testing::AssertionResult assertIonTimestampEq(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual) {
    BOOL timestamps_equal;
    EXPECT_EQ(IERR_OK, g_TimestampEquals(expected, actual, &timestamps_equal, &g_IonEventDecimalContext));
    if (timestamps_equal) {
        return ::testing::AssertionSuccess();
    }
    char expected_str[ION_MAX_TIMESTAMP_STRING];
    char actual_str[ION_MAX_TIMESTAMP_STRING];
    SIZE expected_str_len, actual_str_len;
    EXPECT_EQ(IERR_OK, ion_timestamp_to_string(expected, expected_str, ION_MAX_TIMESTAMP_STRING, &expected_str_len, &g_IonEventDecimalContext));
    EXPECT_EQ(IERR_OK, ion_timestamp_to_string(actual, actual_str, ION_MAX_TIMESTAMP_STRING, &actual_str_len, &g_IonEventDecimalContext));
    return ::testing::AssertionFailure()
            << std::string(expected_str, (size_t)expected_str_len) << " vs. " << std::string(actual_str, (size_t)actual_str_len);
}
#endif
