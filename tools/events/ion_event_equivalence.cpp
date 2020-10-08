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
#include "ion_event_equivalence_impl.h"
#include "ion_event_util.h"
#include "ion_event_stream_impl.h"
#include <ion_const.h>
#include <cstdlib>
#include <sstream>
#include <ion_internal.h>
#include "floating_point_util.h"

BOOL ion_compare_events(ION_EVENT_EQUIVALENCE_PARAMS);

void _ion_event_set_comparison_result(IonEventResult *result, ION_EVENT_COMPARISON_TYPE comparison_type, IonEvent *lhs,
                                      IonEvent *rhs, size_t lhs_index, size_t rhs_index, std::string lhs_location,
                                      std::string rhs_location, std::string message) {
    if (result != NULL) {
        if (result->comparison_result.lhs.event != NULL) {
            // A pair of offending events has already been specified in the result. Only set the first pair.
            return;
        }
        if (ion_event_copy(&result->comparison_result.lhs.event, lhs, &lhs_location, result)
            || ion_event_copy(&result->comparison_result.rhs.event, rhs, &rhs_location, result)) {
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

BOOL ion_compare_scalars(ION_EVENT_EQUIVALENCE_PARAMS) {
    ION_PREPARE_COMPARISON;
    void *expected_value = ION_EXPECTED_ARG->value;
    void *actual_value = ION_ACTUAL_ARG->value;
    ION_EXPECT_FALSE((expected_value == NULL) ^ (actual_value == NULL), "Only one value was null.");
    if (expected_value == NULL) {
        // Equivalence of ion types has already been tested.
        return TRUE;
    }
    switch (ION_TYPE_INT(ION_EXPECTED_ARG->ion_type)) {
        case tid_BOOL_INT:
            ION_EXPECT_BOOL_EQ((BOOL *)expected_value, (BOOL *)actual_value);
            break;
        case tid_INT_INT:
            ION_EXPECT_INT_EQ((ION_INT *)expected_value, (ION_INT *)actual_value);
            break;
        case tid_FLOAT_INT:
            ION_EXPECT_DOUBLE_EQ((double *)expected_value, (double *)actual_value);
            break;
        case tid_DECIMAL_INT:
            ION_EXPECT_DECIMAL_EQ((ION_DECIMAL *)expected_value, (ION_DECIMAL *)actual_value);
            break;
        case tid_TIMESTAMP_INT:
            ION_EXPECT_TIMESTAMP_EQ((ION_TIMESTAMP *)expected_value, (ION_TIMESTAMP *)actual_value);
            break;
        case tid_SYMBOL_INT:
            ION_EXPECT_SYMBOL_EQ((ION_SYMBOL *)expected_value, (ION_SYMBOL *)actual_value);
            break;
        case tid_STRING_INT:
        case tid_BLOB_INT:
        case tid_CLOB_INT: // Clobs and blobs are stored in ION_STRINGs too...
            ION_EXPECT_STRING_EQ((ION_STRING *)expected_value, (ION_STRING *)actual_value);
            break;
        default:
            ION_ASSERT(FALSE, "Illegal state: unknown ion type.");
    }
    ION_PASS_ASSERTIONS;
}

/**
 * Asserts that the struct starting at index_expected is a subset of the struct starting at index_actual.
 */
BOOL ion_compare_struct_subset(ION_EVENT_EQUIVALENCE_PARAMS) {
    const int target_depth = ION_GET_EXPECTED->depth;
    const int index_expected_container_start = ION_INDEX_ACTUAL_ARG;
    const int index_actual_container_start = ION_INDEX_EXPECTED_ARG;
    ION_NEXT_INDICES; // Move past the CONTAINER_START events
    const size_t index_actual_start = ION_INDEX_ACTUAL_ARG;
    std::set<size_t> skips;
    BOOL field_names_equal;
    while (ION_INDEX_EXPECTED_ARG < ION_STREAM_EXPECTED_ARG->size()) {
        ION_INDEX_ACTUAL_ARG = index_actual_start;
        ION_SET_EXPECTED;
        if (ION_EXPECTED_ARG->event_type == CONTAINER_END && ION_EXPECTED_ARG->depth == target_depth) {
            break;
        }
        ION_SYMBOL *expected_field_name = ION_EXPECTED_ARG->field_name;
        ION_ASSERT(expected_field_name != NULL, "Field name in struct cannot be null.");
        while (ION_INDEX_ACTUAL_ARG < ION_STREAM_ACTUAL_ARG->size()) {
            if (skips.count(ION_INDEX_ACTUAL_ARG) == 0) {
                ION_SET_ACTUAL;
                ION_EXPECT_TRUE_WITH_INDEX(!(ION_ACTUAL_ARG->event_type == CONTAINER_END && ION_ACTUAL_ARG->depth == target_depth),
                                           "Did not find matching field for " + ion_event_symbol_to_string(expected_field_name),
                                           index_expected_container_start, index_actual_container_start);
                ION_ASSERT(IERR_OK == ion_symbol_is_equal(expected_field_name,
                                                          ION_ACTUAL_ARG->field_name, &field_names_equal),
                           "Failed to compare field names.");
                if (field_names_equal
                    && ion_compare_events(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG, ION_STREAM_ACTUAL_ARG,
                                          ION_INDEX_ACTUAL_ARG, ION_COMPARISON_TYPE_ARG,
                                          NULL)) { // No need to convey the result.
                    // Skip indices that have already matched. Ensures that structs with different numbers of the same
                    // key:value mapping are not equal.
                    skips.insert(ION_INDEX_ACTUAL_ARG);
                    break;
                }
            }
            ION_NEXT_ACTUAL_VALUE_INDEX;
        }
        ION_NEXT_EXPECTED_VALUE_INDEX;
    }
    ION_PASS_ASSERTIONS;
}

BOOL ion_compare_structs(ION_EVENT_EQUIVALENCE_PARAMS) {
    // By asserting that 'expected' and 'actual' are bidirectional subsets, we are asserting they are equivalent.
    ION_CHECK_ASSERTION(
            ion_compare_struct_subset(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG, ION_STREAM_ACTUAL_ARG,
                                      ION_INDEX_ACTUAL_ARG, ION_COMPARISON_TYPE_ARG, ION_RESULT_ARG));
    ION_CHECK_ASSERTION(
            ion_compare_struct_subset(ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG, ION_STREAM_EXPECTED_ARG,
                                      ION_INDEX_EXPECTED_ARG, ION_COMPARISON_TYPE_ARG, ION_RESULT_ARG));
    ION_PASS_ASSERTIONS;
}

BOOL ion_compare_sequences(ION_EVENT_EQUIVALENCE_PARAMS) {
    const int target_depth = ION_GET_EXPECTED->depth;
    ION_PREPARE_COMPARISON;
    ION_NEXT_INDICES; // Move past the CONTAINER_START events
    while (TRUE) {
        if (ION_STREAM_EXPECTED_ARG->size() == ION_INDEX_EXPECTED_ARG) {
            if (ION_STREAM_ACTUAL_ARG->size() != ION_INDEX_ACTUAL_ARG) {
                ION_EXPECT_TRUE(FALSE, "Streams have different lengths");
            }
            // The streams are incomplete, but they are the same length and all their values are equivalent.
            break;
        }
        if (ION_STREAM_ACTUAL_ARG->size() == ION_INDEX_ACTUAL_ARG) {
            if (ION_STREAM_EXPECTED_ARG->size() != ION_INDEX_EXPECTED_ARG) {
                ION_EXPECT_TRUE(FALSE, "Streams have different lengths");
            }
            break;
        }
        ION_EXPECTED_ARG = ION_GET_EXPECTED;
        ION_ACTUAL_ARG = ION_GET_ACTUAL;
        // NOTE: symbol tables are only allowed within embedded stream sequences. Logic could be added to verify this.
        if (ION_EXPECTED_ARG->event_type == SYMBOL_TABLE) {
            ION_NEXT_EXPECTED_INDEX;
            continue;
        }
        if (ION_ACTUAL_ARG->event_type == SYMBOL_TABLE) {
            ION_NEXT_ACTUAL_INDEX;
            continue;
        }
        ION_CHECK_ASSERTION(ion_compare_events(ION_EVENT_EQUIVALENCE_ARGS));
        BOOL sequence_end = ION_EXPECTED_ARG->event_type == CONTAINER_END && ION_EXPECTED_ARG->depth == target_depth;
        if (sequence_end ^ (ION_ACTUAL_ARG->event_type == CONTAINER_END && ION_ACTUAL_ARG->depth == target_depth)) {
            ION_EXPECT_TRUE(FALSE, "Sequences have different lengths.");
        }
        if (sequence_end) {
            break;
        }
        ION_NEXT_VALUE_INDICES;
    }
    ION_PASS_ASSERTIONS;
}

/**
 * Tests the values starting at the given indices in the given streams (they may be the same) for equivalence
 * under the Ion data model. If the indices start on CONTAINER_START events, this will recursively compare
 * the containers' children.
 */
BOOL ion_compare_events(ION_EVENT_EQUIVALENCE_PARAMS) {
    ION_PREPARE_COMPARISON;
    ION_EXPECT_EVENT_TYPE_EQ(ION_EXPECTED_ARG->event_type, ION_ACTUAL_ARG->event_type);
    ION_EXPECT_EQ(ION_EXPECTED_ARG->ion_type, ION_ACTUAL_ARG->ion_type, "Ion types did not match.");
    ION_EXPECT_EQ(ION_EXPECTED_ARG->depth, ION_ACTUAL_ARG->depth, "Depths did not match.");
    ION_EXPECT_SYMBOL_EQ(ION_EXPECTED_ARG->field_name, ION_ACTUAL_ARG->field_name);
    ION_EXPECT_EQ(ION_EXPECTED_ARG->num_annotations, ION_ACTUAL_ARG->num_annotations,
                  "Number of annotations did not match.");
    for (size_t i = 0; i < ION_EXPECTED_ARG->num_annotations; i++) {
        ION_EXPECT_SYMBOL_EQ(&ION_EXPECTED_ARG->annotations[i], &ION_ACTUAL_ARG->annotations[i]);
    }
    switch (ION_EXPECTED_ARG->event_type) {
        case STREAM_END:
        case CONTAINER_END:
            break;
        case CONTAINER_START:
            switch (ION_TYPE_INT(ION_EXPECTED_ARG->ion_type)) {
                case tid_STRUCT_INT:
                    ION_CHECK_ASSERTION(ion_compare_structs(ION_EVENT_EQUIVALENCE_ARGS));
                    break;
                case tid_SEXP_INT: // intentional fall-through
                case tid_LIST_INT:
                    ION_CHECK_ASSERTION(ion_compare_sequences(ION_EVENT_EQUIVALENCE_ARGS));
                    break;
                default:
                    ION_ASSERT(FALSE, "Illegal state: container start event with non-container type.");
            }
            break;
        case SCALAR:
            ION_CHECK_ASSERTION(ion_compare_scalars(ION_EVENT_EQUIVALENCE_ARGS));
            break;
        default:
            ION_ASSERT(FALSE, "Illegal state: unknown event type.");
    }
    ION_PASS_ASSERTIONS;
}

BOOL ion_compare_substreams(ION_EVENT_EQUIVALENCE_PARAMS, size_t expected_end, size_t actual_end) {
    ASSERT(ION_COMPARISON_TYPE_ARG == COMPARISON_TYPE_BASIC);
    while (ION_INDEX_EXPECTED_ARG < expected_end && ION_INDEX_ACTUAL_ARG < actual_end) {
        ION_PREPARE_COMPARISON;
        if (ION_EXPECTED_ARG->event_type == SYMBOL_TABLE) {
            ION_NEXT_EXPECTED_INDEX;
            continue;
        }
        if (ION_ACTUAL_ARG->event_type == SYMBOL_TABLE) {
            ION_NEXT_ACTUAL_INDEX;
            continue;
        }
        ION_CHECK_ASSERTION(ion_compare_events(ION_EVENT_EQUIVALENCE_ARGS));
        ION_NEXT_VALUE_INDICES;
    }
    ION_PASS_ASSERTIONS;
}

BOOL ion_compare_streams(IonEventStream *stream_expected, IonEventStream *stream_actual,
                         IonEventResult *ION_RESULT_ARG) {
    size_t ION_INDEX_EXPECTED_ARG = 0;
    size_t ION_INDEX_ACTUAL_ARG = 0;
    ION_EVENT_COMPARISON_TYPE ION_COMPARISON_TYPE_ARG = COMPARISON_TYPE_BASIC;
    ION_CHECK_ASSERTION(
            ion_compare_substreams(ION_EVENT_EQUIVALENCE_ARGS, stream_expected->size(), stream_actual->size()));
    ION_PASS_ASSERTIONS;
}

typedef BOOL (*COMPARISON_FN)(ION_EVENT_EQUIVALENCE_PARAMS);

BOOL ion_compare_sets_equivs(ION_EVENT_EQUIVALENCE_PARAMS) {
    ION_CHECK_ASSERTION(ion_compare_events(ION_EVENT_EQUIVALENCE_ARGS));
    ION_PASS_ASSERTIONS;
}

BOOL ion_compare_sets_nonequivs(ION_EVENT_EQUIVALENCE_PARAMS) {
    // The corresponding indices are assumed to be equivalent.
    if (ION_INDEX_EXPECTED_ARG != ION_INDEX_ACTUAL_ARG) {
        ION_PREPARE_COMPARISON;
        ION_EXPECT_FALSE_FOR_NON_EQUIVS(ion_compare_events(ION_EVENT_EQUIVALENCE_ARGS), "Equivalent values in a non-equivs set.");
    }
    ION_PASS_ASSERTIONS;
}

/**
 * Compares each element in the current container to every other element in the container. The given index refers
 * to the starting index of the first element in the container.
 */
BOOL ion_compare_sets_standard(ION_EVENT_EQUIVALENCE_PARAMS) {
    COMPARISON_FN comparison_fn = (ION_COMPARISON_TYPE_ARG == COMPARISON_TYPE_EQUIVS
                                   || ION_COMPARISON_TYPE_ARG == COMPARISON_TYPE_EQUIVTIMELINE)
                                  ? ion_compare_sets_equivs
                                  : ion_compare_sets_nonequivs;
    const size_t index_actual_initial = ION_INDEX_ACTUAL_ARG;
    while (TRUE) {
        if (ION_GET_ACTUAL->event_type == CONTAINER_END && ION_GET_ACTUAL->depth == 0) {
            ION_NEXT_EXPECTED_VALUE_INDEX;
            if (ION_GET_EXPECTED->event_type == CONTAINER_END && ION_GET_EXPECTED->depth == 0) {
                break;
            }
            ION_INDEX_ACTUAL_ARG = index_actual_initial;
        } else {
            ION_CHECK_ASSERTION((*comparison_fn)(ION_EVENT_EQUIVALENCE_ARGS));
            ION_NEXT_ACTUAL_VALUE_INDEX;
        }
    }
    ION_PASS_ASSERTIONS;
}

/**
 * The 'embedded_documents' annotation denotes that the current container contains streams of Ion data embedded
 * in string values. These embedded streams are parsed and their resulting IonEventStreams compared.
 */
BOOL ion_compare_sets_embedded(ION_EVENT_EQUIVALENCE_PARAMS, size_t *expected_len, size_t *actual_len) {
    size_t expected_stream_count = 0;
    size_t actual_stream_count = 0;
    const size_t index_expected_initial = ION_INDEX_EXPECTED_ARG;
    const size_t index_actual_initial = ION_INDEX_ACTUAL_ARG;
    while (TRUE) {
        ION_PREPARE_COMPARISON;
        size_t step_expected = ion_event_stream_length(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG);
        if (ION_ACTUAL_ARG->event_type == CONTAINER_END && ION_ACTUAL_ARG->depth == 0) {
            expected_stream_count += 1;
            ION_INDEX_EXPECTED_ARG += step_expected;
            if (ION_GET_EXPECTED->event_type == CONTAINER_END && ION_GET_EXPECTED->depth == 0) {
                // Step past the CONTAINER_ENDs
                ION_NEXT_INDICES;
                break;
            }
            ION_INDEX_ACTUAL_ARG = index_actual_initial;
            actual_stream_count = 0;
        }
        else {
            size_t step_actual = ion_event_stream_length(ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG);
            // For non-equivs, embedded streams must not be compared reflexively.
            if (ION_COMPARISON_TYPE_ARG != COMPARISON_TYPE_NONEQUIVS || expected_stream_count != actual_stream_count) {
                if (ION_COMPARISON_TYPE_ARG == COMPARISON_TYPE_EQUIVS) {
                    if (step_expected == 1 ^ step_actual == 1) {
                        ION_EXPECT_TRUE(step_expected == 1 && step_actual == 1,
                                        "Only one embedded stream represents an empty stream.");
                    }
                    else if (step_expected > 1 && step_actual > 1) {
                        ION_CHECK_ASSERTION(
                                ion_compare_substreams(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG,
                                                       ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG,
                                                       COMPARISON_TYPE_BASIC, ION_RESULT_ARG,
                                                       ION_INDEX_EXPECTED_ARG + step_expected,
                                                       ION_INDEX_ACTUAL_ARG + step_actual)
                        );
                    }
                }
                else {
                    ION_ASSERT(ION_COMPARISON_TYPE_ARG == COMPARISON_TYPE_NONEQUIVS,
                               "Invalid embedded documents comparison type.");
                    if (step_expected == 1 && step_actual == 1) {
                        ION_EXPECT_FALSE_FOR_NON_EQUIVS(
                                step_expected == 1 && step_actual == 1,
                                "Both embedded streams are empty stream in a non-equivs set.")
                    }
                    else if (step_expected > 1 && step_actual > 1) {
                        ION_EXPECT_FALSE_FOR_NON_EQUIVS(
                                ion_compare_substreams(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG,
                                                       ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG,
                                                       COMPARISON_TYPE_BASIC, /*result=*/NULL, // Result not needed.
                                                       ION_INDEX_EXPECTED_ARG + step_expected,
                                                       ION_INDEX_ACTUAL_ARG + step_actual),
                                "Equivalent streams in a non-equivs set.");
                    }
                }
            }
            actual_stream_count += 1;
            ION_INDEX_ACTUAL_ARG += step_actual;
        }
    }
    *expected_len = ION_INDEX_EXPECTED_ARG - index_expected_initial;
    *actual_len = ION_INDEX_ACTUAL_ARG - index_actual_initial;
    ION_PASS_ASSERTIONS;
}

/**
 * Comparison sets are conveyed as sequences. Each element in the sequence must be equivalent to all other elements
 * in the same sequence.
 */
BOOL ion_compare_sets(IonEventStream *ION_STREAM_EXPECTED_ARG, IonEventStream *ION_STREAM_ACTUAL_ARG,
                      ION_EVENT_COMPARISON_TYPE ION_COMPARISON_TYPE_ARG, IonEventResult *ION_RESULT_ARG) {
    size_t ION_INDEX_EXPECTED_ARG = 0, ION_INDEX_ACTUAL_ARG = 0;
    ION_PREPARE_COMPARISON;
    ION_EXPECT_TRUE(!(ION_STREAM_EXPECTED_ARG->size() == 0 ^ ION_STREAM_ACTUAL_ARG->size()== 0),
                    "The input streams had a different number of comparison sets.");
    if (ION_STREAM_EXPECTED_ARG->size() == 0) return TRUE;
    while (TRUE) {
        if (ION_INDEX_EXPECTED_ARG == ION_STREAM_EXPECTED_ARG->size() - 1) {
            // Even if the streams' corresponding sets have different number of elements, the loop will reach the
            // end of each stream at the same time as long as the streams have the same number of sets. And if they
            // don't have the same number of sets, an error is raised.
            ION_EXPECT_EQ(ION_STREAM_ACTUAL_ARG->size() - 1, ION_INDEX_ACTUAL_ARG,
                          "The input streams had a different number of comparison sets.");
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, ION_EXPECTED_ARG->event_type);
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, ION_ACTUAL_ARG->event_type);
            break;
        }
        else if (ION_INDEX_ACTUAL_ARG == ION_STREAM_ACTUAL_ARG->size() - 1) {
            ION_EXPECT_EQ(ION_STREAM_EXPECTED_ARG->size() - 1, ION_INDEX_EXPECTED_ARG,
                          "The input streams had a different number of comparison sets.");
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, ION_ACTUAL_ARG->event_type);
            ION_EXPECT_EVENT_TYPE_EQ(STREAM_END, ION_EXPECTED_ARG->event_type);
            break;
        }
        else {
            ION_ASSERT(CONTAINER_START == ION_EXPECTED_ARG->event_type,
                       "Comparison sets must be lists or s-expressions.");
            ION_ASSERT((tid_SEXP == ION_EXPECTED_ARG->ion_type) || (tid_LIST == ION_EXPECTED_ARG->ion_type),
                       "Comparison sets must be lists or s-expressions.");
            ION_ASSERT(CONTAINER_START == ION_ACTUAL_ARG->event_type,
                       "Comparison sets must be lists or s-expressions.");
            ION_ASSERT((tid_SEXP == ION_ACTUAL_ARG->ion_type) || (tid_LIST == ION_ACTUAL_ARG->ion_type),
                       "Comparison sets must be lists or s-expressions.");
            ION_STRING *lhs_annotation = (ION_EXPECTED_ARG->num_annotations > 0)
                                         ? &ION_EXPECTED_ARG->annotations[0].value : NULL;
            ION_STRING *rhs_annotation = (ION_ACTUAL_ARG->num_annotations > 0)
                                         ? &ION_ACTUAL_ARG->annotations[0].value : NULL;
            size_t step_lhs;
            size_t step_rhs;
            if (lhs_annotation && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, lhs_annotation)) {
                ION_ASSERT(rhs_annotation && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, rhs_annotation),
                           "Embedded streams set expected.");
                // Skip past the CONTAINER_START events.
                ION_NEXT_INDICES;
                ION_CHECK_ASSERTION(ion_compare_sets_embedded(ION_EVENT_EQUIVALENCE_ARGS, &step_lhs, &step_rhs));
            } else {
                ION_ASSERT(!(rhs_annotation
                             && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, rhs_annotation)),
                           "Embedded streams set not expected.");
                step_lhs = ION_EXPECTED_VALUE_LENGTH;
                step_rhs = ION_ACTUAL_VALUE_LENGTH;
                ION_CHECK_ASSERTION(ion_compare_sets_standard(ION_STREAM_EXPECTED_ARG, ION_INDEX_EXPECTED_ARG + 1,
                                                                   ION_STREAM_ACTUAL_ARG, ION_INDEX_ACTUAL_ARG + 1,
                                                                   ION_COMPARISON_TYPE_ARG, ION_RESULT_ARG));
            }
            ION_INDEX_EXPECTED_ARG += step_lhs;
            ION_INDEX_ACTUAL_ARG += step_rhs;
            ION_EXPECTED_ARG = ION_GET_EXPECTED;
            ION_ACTUAL_ARG = ION_GET_ACTUAL;
        }
    }
    ION_PASS_ASSERTIONS;
}

BOOL ion_equals_bool(BOOL *expected, BOOL *actual, std::string *failure_message, IonEventResult *ION_RESULT_ARG) {
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

BOOL ion_equals_string(ION_STRING *expected, ION_STRING *actual, std::string *failure_message,
                       IonEventResult *ION_RESULT_ARG) {
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

BOOL ion_equals_symbol(ION_SYMBOL *expected, ION_SYMBOL *actual, std::string *failure_message,
                       IonEventResult *ION_RESULT_ARG) {
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

char *ion_compare_int_to_string(ION_INT *value) {
    SIZE len, written;
    ion_int_char_length(value, &len);
    char *int_str = (char *)malloc(len * sizeof(char));
    ion_int_to_char(value, (BYTE *)int_str, len, &written);
    return int_str;
}

BOOL ion_equals_int(ION_INT *expected, ION_INT *actual, std::string *failure_message, IonEventResult *ION_RESULT_ARG) {
    int int_comparison = 0;
    char *expected_str = NULL;
    char *actual_str = NULL;
    ION_EXPECT_OK(ion_int_compare(expected, actual, &int_comparison));
    if (int_comparison == 0) {
        return TRUE;
    }
    if (failure_message) {
        expected_str = ion_compare_int_to_string(expected);
        actual_str = ion_compare_int_to_string(actual);
        *failure_message = std::string("") + expected_str + " vs. " + actual_str;
        free(expected_str);
        free(actual_str);
    }
    return FALSE;
}

BOOL ion_equals_decimal(ION_DECIMAL *expected, ION_DECIMAL *actual, std::string *failure_message,
                        IonEventResult *ION_RESULT_ARG) {
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

BOOL _ion_equals_timestamp_common(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message,
                                  IonEventResult *ION_RESULT_ARG, BOOL is_instant_equality) {
    BOOL timestamps_equal;
    if (is_instant_equality) {
        ION_EXPECT_OK(ion_timestamp_instant_equals(expected, actual, &timestamps_equal, &g_IonEventDecimalContext));
    }
    else {
        ION_EXPECT_OK(ion_timestamp_equals(expected, actual, &timestamps_equal, &g_IonEventDecimalContext));
    }
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

BOOL ion_equals_timestamp(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message,
                          IonEventResult *ION_RESULT_ARG) {
    return _ion_equals_timestamp_common(expected, actual, failure_message, ION_RESULT_ARG, FALSE);
}

BOOL ion_equals_timestamp_instant(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message,
                                  IonEventResult *ION_RESULT_ARG) {
    return _ion_equals_timestamp_common(expected, actual, failure_message, ION_RESULT_ARG, TRUE);
}

BOOL ion_equals_float(double *expected, double *actual, std::string *failure_message, IonEventResult *ION_RESULT_ARG) {
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
