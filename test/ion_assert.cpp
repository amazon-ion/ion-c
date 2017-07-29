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

TIMESTAMP_COMPARISON_FN g_TimestampEquals = ion_timestamp_equals;
std::string g_CurrentTest = "NONE";
decContext g_TestDecimalContext = {
    ION_TEST_DECIMAL_MAX_DIGITS,    // max digits (arbitrarily high -- raise if test data requires more)
    DEC_MAX_MATH,                   // max exponent
    -DEC_MAX_MATH,                  // min exponent
    DEC_ROUND_HALF_EVEN,            // rounding mode
    DEC_Errors,                     // trap conditions
    0,                              // status flags
    0                               // apply exponent clamp?
};

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
    if (value) {
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
    ION_EXPECT_OK(ion_decimal_equals(expected, actual, &g_TestDecimalContext, &decimal_equals));
    if (decimal_equals) {
        return ::testing::AssertionSuccess();
    }
    char expected_str[ION_TEST_DECIMAL_MAX_STRLEN];
    char actual_str[ION_TEST_DECIMAL_MAX_STRLEN];
    ION_EXPECT_OK(ion_decimal_to_string(expected, expected_str));
    ION_EXPECT_OK(ion_decimal_to_string(actual, actual_str));
    return ::testing::AssertionFailure()
            << std::string("") << expected_str << " vs. " << actual_str;
}

::testing::AssertionResult assertIonTimestampEq(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual) {
    BOOL timestamps_equal;
    EXPECT_EQ(IERR_OK, g_TimestampEquals(expected, actual, &timestamps_equal, &g_TestDecimalContext));
    if (timestamps_equal) {
        return ::testing::AssertionSuccess();
    }
    char expected_str[ION_MAX_TIMESTAMP_STRING];
    char actual_str[ION_MAX_TIMESTAMP_STRING];
    SIZE expected_str_len, actual_str_len;
    EXPECT_EQ(IERR_OK, ion_timestamp_to_string(expected, expected_str, ION_MAX_TIMESTAMP_STRING, &expected_str_len, &g_TestDecimalContext));
    EXPECT_EQ(IERR_OK, ion_timestamp_to_string(actual, actual_str, ION_MAX_TIMESTAMP_STRING, &actual_str_len, &g_TestDecimalContext));
    return ::testing::AssertionFailure()
            << std::string(expected_str, (size_t)expected_str_len) << " vs. " << std::string(actual_str, (size_t)actual_str_len);
}

BOOL ionStringEq(ION_STRING *expected, ION_STRING *actual) {
    return assertIonStringEq(expected, actual) == ::testing::AssertionSuccess();
}

BOOL assertIonScalarEq(IonEvent *expected, IonEvent *actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    void *expected_value = expected->value;
    void *actual_value = actual->value;
    ION_EXPECT_FALSE((expected_value == NULL) ^ (actual_value == NULL));
    if (expected_value == NULL) {
        // Equivalence of ion types has already been tested.
        return TRUE;
    }
    switch (ION_TYPE_INT(expected->ion_type)) {
        case tid_BOOL_INT:
            ION_EXPECT_EQ(*(BOOL *) expected_value, *(BOOL *) actual_value);
            break;
        case tid_INT_INT:
            ION_EXPECT_INT_EQ((ION_INT *) expected_value, (ION_INT *) actual_value);
            break;
        case tid_FLOAT_INT:
            ION_EXPECT_DOUBLE_EQ(*(double *) expected_value, *(double *) actual_value);
            break;
        case tid_DECIMAL_INT:
            ION_EXPECT_DECIMAL_EQ((ION_DECIMAL *) expected_value, (ION_DECIMAL *) actual_value);
            break;
        case tid_TIMESTAMP_INT:
            ION_EXPECT_TIMESTAMP_EQ((ION_TIMESTAMP *) expected_value, (ION_TIMESTAMP *) actual_value);
            break;
        case tid_SYMBOL_INT:
        case tid_STRING_INT:
        case tid_CLOB_INT:
        case tid_BLOB_INT: // Clobs and blobs are stored in ION_STRINGs too...
            ION_EXPECT_STRING_EQ((ION_STRING *) expected_value, (ION_STRING *) actual_value);
            break;
        default:
            EXPECT_FALSE("Illegal state: unknown ion type.");
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
    while (TRUE) {
        index_actual = index_actual_start;
        IonEvent *expected = stream_expected->at(index_expected);
        if (expected->event_type == CONTAINER_END && expected->depth == target_depth) {
            break;
        }
        ION_STRING *expected_field_name = expected->field_name;
        EXPECT_TRUE(expected_field_name != NULL);
        while (TRUE) {
            if (skips.count(index_actual) == 0) {
                IonEvent *actual = stream_actual->at(index_actual);
                ION_ASSERT(!(actual->event_type == CONTAINER_END && actual->depth == target_depth),
                           "Reached end of struct before finding matching field.");
                if (ionStringEq(expected_field_name, actual->field_name)
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
    ION_EXPECT_EQ(expected->event_type, actual->event_type);
    ION_EXPECT_EQ(expected->ion_type, actual->ion_type);
    ION_EXPECT_EQ(expected->depth, actual->depth);
    ION_EXPECT_STRING_EQ(expected->field_name, actual->field_name);
    ION_EXPECT_EQ(expected->num_annotations, actual->num_annotations);
    for (size_t i = 0; i < expected->num_annotations; i++) {
        ION_EXPECT_STRING_EQ(expected->annotations[i], actual->annotations[i]);
    }
    switch (expected->event_type) {
        case STREAM_END:
        case CONTAINER_END:
            break;
        case CONTAINER_START:
            switch (ION_TYPE_INT(expected->ion_type)) {
                case tid_STRUCT_INT:
                    ION_ACCUMULATE_ASSERTION(
                            assertIonStructEq(stream_expected, index_expected, stream_actual, index_actual,
                                              assertion_type));
                    break;
                case tid_SEXP_INT: // intentional fall-through
                case tid_LIST_INT:
                    ION_ACCUMULATE_ASSERTION(
                            assertIonSequenceEq(stream_expected, index_expected, stream_actual, index_actual,
                                                assertion_type));
                    break;
                default:
                    EXPECT_FALSE("Illegal state: container start event with non-container type.");
            }
            break;
        case SCALAR:
            ION_ACCUMULATE_ASSERTION(assertIonScalarEq(expected, actual, assertion_type));
            break;
        default:
            EXPECT_FALSE("Illegal state: unknown event type.");
    }
    ION_EXIT_ASSERTIONS;
}

BOOL assertIonEventStreamEq(IonEventStream *expected, IonEventStream *actual, ASSERTION_TYPE assertion_type) {
    ION_ENTER_ASSERTIONS;
    size_t index_expected = 0;
    size_t index_actual = 0;
    while (index_expected < expected->size() && index_actual < actual->size()) {
        ION_ACCUMULATE_ASSERTION(assertIonEventsEq(expected, index_expected, actual, index_actual, assertion_type));
        index_expected += valueEventLength(expected, index_expected);
        index_actual += valueEventLength(actual, index_actual);
    }
    ION_ASSERT(expected->size() == index_expected, "Expected stream did not reach its end.");
    ION_ASSERT(actual->size() == index_actual, "Actual stream did not reach its end.");
    ION_EXIT_ASSERTIONS;
}

std::string _bytesToHexString(const BYTE *bytes, SIZE len) {
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < len; ++i) {
        ss << std::setfill('0') << std::setw(2) << (int)bytes[i] << " ";
    }
    return ss.str();
}

void assertBytesEqual(const char *expected, SIZE expected_len, const BYTE *actual, SIZE actual_len) {
    ASSERT_EQ(expected_len, actual_len);
    BOOL bytes_not_equal = memcmp((BYTE *)expected, actual, (size_t)actual_len);
    if (bytes_not_equal) {
        ASSERT_FALSE(bytes_not_equal) << _bytesToHexString((BYTE *)expected, expected_len) << " vs. " << std::endl
                                      << _bytesToHexString(actual, actual_len);
    }
}

void assertStringsEqual(const char *expected, const char *actual, SIZE actual_len) {
    BOOL strings_not_equal = strncmp(expected, actual, (size_t)actual_len);
    if (strings_not_equal) {
        ASSERT_FALSE(strings_not_equal) << std::string(expected) << " vs. " << std::endl
                                        << std::string(actual, (unsigned long)actual_len);
    }
}

