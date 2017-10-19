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

#include "ion_assert.h"
#include "ion_helpers.h"
#include "ion_test_util.h"
#include "ion_decimal_impl.h"

/* General test utilities */

#define ION_DECIMAL_FREE_1(decimal) \
    ION_ASSERT_OK(ion_decimal_free(decimal));

#define ION_DECIMAL_FREE_2(decimal_1, decimal_2) \
    ION_DECIMAL_FREE_1(decimal_1); \
    ION_DECIMAL_FREE_1(decimal_2);

#define ION_DECIMAL_FREE_3(decimal_1, decimal_2, decimal_3) \
    ION_DECIMAL_FREE_1(decimal_1); \
    ION_DECIMAL_FREE_2(decimal_2, decimal_3);

#define ION_DECIMAL_FREE_4(decimal_1, decimal_2, decimal_3, decimal_4) \
    ION_DECIMAL_FREE_1(decimal_1); \
    ION_DECIMAL_FREE_3(decimal_2, decimal_3, decimal_4);

#define ION_DECIMAL_FREE_5(decimal_1, decimal_2, decimal_3, decimal_4, decimal_5) \
    ION_DECIMAL_FREE_2(decimal_1, decimal_2); \
    ION_DECIMAL_FREE_3(decimal_3, decimal_4, decimal_5);

/* Reading/writing test utilities */

#define ION_DECIMAL_READER_NEXT \
    ION_ASSERT_OK(ion_reader_next(reader, &type)); \
    ASSERT_EQ(tid_DECIMAL, type);

#define ION_DECIMAL_WRITER_INIT(is_binary) \
    hWRITER writer = NULL; \
    ION_STREAM *ion_stream = NULL; \
    BYTE *result; \
    SIZE result_len; \
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));

#define ION_DECIMAL_READER_DECLARE \
    hREADER reader; \
    ION_TYPE type;

/**
 * Declares and initializes a reader that supports reading arbitrarily high decimal precision.
 */
#define ION_DECIMAL_READER_INIT \
    ION_DECIMAL_READER_DECLARE; \
    ION_ASSERT_OK(ion_test_new_text_reader(text_decimal, &reader));

#define ION_DECIMAL_CLOSE_READER_WRITER \
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len)); \
    ION_ASSERT_OK(ion_reader_close(reader));

/**
 * Asserts that the output of the writer matches the input to the reader.
 */
#define ION_DECIMAL_ASSERT_ROUNDTRIP_RESULT \
    ION_DECIMAL_CLOSE_READER_WRITER; \
    ASSERT_EQ(strlen(text_decimal), result_len) << text_decimal << " vs. " << std::endl \
        << std::string((char *)result, result_len); \
    assertStringsEqual(text_decimal, (char *)result, result_len); \
    free(result);

/**
 * (Re)Initializes the declared reader that supports reading `max_digits` of decimal precision.
 */
#define ION_DECIMAL_READER_INIT_CUSTOM_OPTIONS(max_digits) \
    ION_READER_OPTIONS options; \
    decContext context = { \
        max_digits,           /* max digits */ \
        DEC_MAX_MATH,         /* max exponent */ \
        -DEC_MAX_MATH,        /* min exponent */ \
        DEC_ROUND_HALF_EVEN,  /* rounding mode */ \
        DEC_Errors,           /* trap conditions */ \
        0,                    /* status flags */ \
        0                     /* apply exponent clamp? */ \
    }; \
    ion_test_initialize_reader_options(&options); \
    options.decimal_context = &context; \
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)text_decimal, strlen(text_decimal), &options));

/**
 * (Re)Initializes the reader to support `max_digits` of decimal precision and asserts that the reader fails to read
 * the input because overflow occurred.
 */
#define ION_DECIMAL_EXPECT_OVERFLOW(func, max_digits) \
    ION_DECIMAL_READER_INIT_CUSTOM_OPTIONS(max_digits); \
    ION_DECIMAL_READER_NEXT; \
    ASSERT_EQ(IERR_NUMERIC_OVERFLOW, func); \
    ION_ASSERT_OK(ion_reader_close(reader));

/**
 * Converts a decimal with `decimal_digits` digits of precision from text to binary Ion.
 */
#define ION_DECIMAL_TEXT_TO_BINARY(decimal_digits) \
    ION_DECIMAL ion_decimal; \
    /* This reader supports arbitrarily-high decimal precision. */ \
    ION_DECIMAL_READER_INIT; \
    ION_DECIMAL_WRITER_INIT(TRUE); \
    \
    ION_DECIMAL_READER_NEXT; \
    ION_ASSERT_OK(ion_reader_read_ion_decimal(reader, &ion_decimal)); \
    ASSERT_TRUE(ION_DECIMAL_IS_NUMBER((&ion_decimal))); \
    ASSERT_EQ(decimal_digits, ion_decimal.value.num_value->digits); \
    \
    ION_ASSERT_OK(ion_writer_write_ion_decimal(writer, &ion_decimal)); \
    ION_DECIMAL_CLOSE_READER_WRITER;

#define ION_DECIMAL_BINARY_READER_EXPECT_OVERFLOW(func, decimal_digits) \
    ION_DECIMAL_TEXT_TO_BINARY(decimal_digits); \
    /* This new reader only supports decQuad precision, which the input exceeds. */ \
    ION_DECIMAL_EXPECT_OVERFLOW(func, DECQUAD_Pmax); \
    free(result); \
    ION_DECIMAL_FREE_1(&ion_decimal); \


/* Reading/writing tests */

TEST(IonTextDecimal, RoundtripPreservesFullFidelityDecNumber) {
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875";
    ION_DECIMAL ion_decimal;

    ION_DECIMAL_READER_INIT;
    ION_DECIMAL_WRITER_INIT(FALSE);

    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_ion_decimal(reader, &ion_decimal));
    ION_ASSERT_OK(ion_writer_write_ion_decimal(writer, &ion_decimal));

    ION_DECIMAL_ASSERT_ROUNDTRIP_RESULT;
    ION_DECIMAL_FREE_1(&ion_decimal);
}

TEST(IonBinaryDecimal, RoundtripPreservesFullFidelityDecNumber) {
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875";
    ION_DECIMAL ion_decimal_after;

    ION_DECIMAL_TEXT_TO_BINARY(53);

    ION_ASSERT_OK(ion_test_new_reader(result, (SIZE)result_len, &reader));
    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_ion_decimal(reader, &ion_decimal_after));
    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_TRUE(assertIonDecimalEq(&ion_decimal, &ion_decimal_after));

    free(result);
    ION_DECIMAL_FREE_2(&ion_decimal, &ion_decimal_after);
}

TEST(IonTextDecimal, ReaderFailsUponLossOfPrecisionDecNumber) {
    // This test asserts that an error is raised when decimal precision would be lost. From the
    // `ion_reader_read_ion_decimal` API, this only occurs when the input has more digits of precision than would fit in
    // a decQuad, and the precision exceeds the context's max digits.
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875";
    ION_DECIMAL ion_decimal;

    ION_DECIMAL_READER_DECLARE;
    ION_DECIMAL_EXPECT_OVERFLOW(ion_reader_read_ion_decimal(reader, &ion_decimal), DECQUAD_Pmax);
    ION_DECIMAL_FREE_1(&ion_decimal);
}

TEST(IonTextDecimal, ReaderFailsUponLossOfPrecisionDecQuad) {
    // This test asserts that an error is raised when decimal precision would be lost. From the
    // `ion_reader_read_decimal` API, This always occurs when the input has more digits of precision than would fit in a
    // decQuad.
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875";
    decQuad quad;

    ION_DECIMAL_READER_DECLARE;
    ION_DECIMAL_EXPECT_OVERFLOW(ion_reader_read_decimal(reader, &quad), DECQUAD_Pmax);
}

TEST(IonBinaryDecimal, ReaderFailsUponLossOfPrecisionDecNumber) {
    // This test asserts that an error is raised when decimal precision would be lost. From the
    // `ion_reader_read_ion_decimal` API, this only occurs when the input has more digits of precision than would fit in
    // a decQuad , and the precision exceeds the context's max digits.
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875";
    ION_DECIMAL_BINARY_READER_EXPECT_OVERFLOW(ion_reader_read_ion_decimal(reader, &ion_decimal), 53);
}

TEST(IonBinaryDecimal, ReaderFailsUponLossOfPrecisionDecQuad) {
    // This test asserts that an error is raised when decimal precision would be lost. From the
    // `ion_reader_read_decimal` API, This always occurs when the input has more digits of precision than would fit in a
    // decQuad.
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875";
    decQuad quad;
    ION_DECIMAL_BINARY_READER_EXPECT_OVERFLOW(ion_reader_read_decimal(reader, &quad), 53);
}

TEST(IonTextDecimal, ReaderAlwaysPreservesUpTo34Digits) {
    // Because decQuads are statically sized, decimals with <= DECQUAD_Pmax digits of precision never need to overflow;
    // they can always be accommodated in a decQuad. This test asserts that precision is preserved even when the context
    // is configured with fewer digits than DECQUAD_Pmax.
    const char *text_decimal = "1.234 5.678";
    ION_DECIMAL ion_decimal;
    decQuad quad;

    ION_DECIMAL_READER_DECLARE;
    ION_DECIMAL_READER_INIT_CUSTOM_OPTIONS(3);
    ION_DECIMAL_WRITER_INIT(FALSE);

    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_ion_decimal(reader, &ion_decimal));
    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_decimal(reader, &quad));

    ION_ASSERT_OK(ion_writer_write_ion_decimal(writer, &ion_decimal));
    ION_ASSERT_OK(ion_writer_write_decimal(writer, &quad));

    ION_DECIMAL_ASSERT_ROUNDTRIP_RESULT;
    ION_DECIMAL_FREE_1(&ion_decimal);
}

TEST(IonBinaryDecimal, ReaderAlwaysPreservesUpTo34Digits) {
    // Because decQuads are statically sized, decimals with <= DECQUAD_Pmax digits of precision never need to overflow;
    // they ca always be accommodated in a decQuad. This test asserts that precision is preserved even when the context
    // is configured with fewer digits than DECQUAD_Pmax.
    const char *text_decimal = "1.234 5.678";
    ION_DECIMAL ion_decimal_before, ion_decimal_after;
    decQuad quad_before, quad_after;
    BOOL decimal_equals, quad_equals;

    // This reader supports arbitrarily-high decimal precision.
    ION_DECIMAL_READER_INIT;
    ION_DECIMAL_WRITER_INIT(TRUE);

    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_ion_decimal(reader, &ion_decimal_before));
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, ion_decimal_before.type);
    ASSERT_EQ(4, decQuadDigits(&ion_decimal_before.value.quad_value));
    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_decimal(reader, &quad_before));

    ION_ASSERT_OK(ion_writer_write_ion_decimal(writer, &ion_decimal_before));
    ION_ASSERT_OK(ion_writer_write_decimal(writer, &quad_before));
    ION_DECIMAL_CLOSE_READER_WRITER;

    ION_DECIMAL_READER_INIT_CUSTOM_OPTIONS(3);
    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_ion_decimal(reader, &ion_decimal_after));
    ION_DECIMAL_READER_NEXT;
    ION_ASSERT_OK(ion_reader_read_decimal(reader, &quad_after));

    ION_ASSERT_OK(ion_decimal_equals(&ion_decimal_before, &ion_decimal_after, &((ION_READER *)reader)->_deccontext, &decimal_equals));
    ION_ASSERT_OK(ion_decimal_equals_quad(&quad_before, &quad_after, &((ION_READER *)reader)->_deccontext, &quad_equals));

    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_TRUE(decimal_equals);
    ASSERT_TRUE(quad_equals);
    free(result);
    ION_DECIMAL_FREE_2(&ion_decimal_before, &ion_decimal_after);
}

TEST(IonDecimal, WriteAllValues) {
    const char *text_decimal = "1.1999999999999999555910790149937383830547332763671875 -1d+123";
    ION_DECIMAL ion_decimal;

    ION_DECIMAL_READER_INIT;
    ION_DECIMAL_WRITER_INIT(FALSE);

    ION_ASSERT_OK(ion_writer_write_all_values(writer, reader));

    ION_DECIMAL_ASSERT_ROUNDTRIP_RESULT;
    ION_DECIMAL_FREE_1(&ion_decimal);
}

/* `ION_DECIMAL_COMPUTE_API_BUILDER_THREE_OPERAND` tests */

TEST(IonDecimal, FMADecQuad) {
    ION_DECIMAL result, lhs, rhs, fhs, expected;
    // The operands are all backed by decQuads.
    ION_ASSERT_OK(ion_decimal_from_int32(&lhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&fhs, 1));
    ION_ASSERT_OK(ion_decimal_fma(&result, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&expected, 101));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    ION_DECIMAL_FREE_5(&result, &lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMADecQuadInPlaceAllOperandsSame) {
    ION_DECIMAL lhs, expected;
    // The operands are all backed by decQuads.
    ION_ASSERT_OK(ion_decimal_from_int32(&lhs, 10));
    ION_ASSERT_OK(ion_decimal_fma(&lhs, &lhs, &lhs, &lhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&expected, 110));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &lhs));

    ION_DECIMAL_FREE_2(&lhs, &expected);
}

TEST(IonDecimal, FMADecNumber) {
    ION_DECIMAL result, lhs, rhs, fhs, expected;
    // Because these decimals have more than DECQUAD_Pmax digits, they will be backed by decNumbers.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&rhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&fhs, "-100000000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_fma(&result, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "10000000000000000000000000000000000000100000000000000000000000000000000000000.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    ION_DECIMAL_FREE_5(&result, &lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMAMixed) {
    ION_DECIMAL result, lhs, rhs, fhs, expected;
    // Because this decimal has more than DECQUAD_Pmax digits, it will be backed by a decNumber.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    // These operands are backed by decQuads. They will be temporarily converted to decNumbers to perform the calculation.
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&fhs, 1));
    ION_ASSERT_OK(ion_decimal_fma(&result, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "1000000000000000000000000000000000000011.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, fhs.type);

    ION_DECIMAL_FREE_5(&result, &lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMAMixedInPlaceNumber) {
    ION_DECIMAL lhs, rhs, fhs, expected;
    // Because this decimal has more than DECQUAD_Pmax digits, it will be backed by a decNumber.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    // These operands are backed by decQuads. They will be temporarily converted to decNumbers to perform the calculation.
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&fhs, 1));
    ION_ASSERT_OK(ion_decimal_fma(&lhs, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "1000000000000000000000000000000000000011.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &lhs));

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, fhs.type);

    ION_DECIMAL_FREE_4(&lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMAMixedInPlaceQuad) {
    ION_DECIMAL lhs, rhs, fhs, expected;
    // Because this decimal has more than DECQUAD_Pmax digits, it will be backed by a decNumber.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    // These operands are backed by decQuads. They will be temporarily converted to decNumbers to perform the calculation.
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&fhs, 1));
    ION_ASSERT_OK(ion_decimal_fma(&fhs, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "1000000000000000000000000000000000000011.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &fhs));

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, lhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);

    ION_DECIMAL_FREE_4(&lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMADecQuadOverflows) {
    ION_DECIMAL result, lhs, rhs, fhs, expected;
    // This decimal has exactly DECQUAD_Pmax digits, so it fits into a decQuad.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "1000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&fhs, 1));
    // The operation will try to keep this in decQuads, but detects overflow and upgrades them to decNumbers.
    ION_ASSERT_OK(ion_decimal_fma(&result, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "10000000000000000000000000000000011.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    // Asserts that the operation results in a decNumber.
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, result.type);

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, lhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, fhs.type);

    ION_DECIMAL_FREE_5(&result, &lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMADecQuadOverflowsInPlace) {
    ION_DECIMAL lhs, rhs, fhs, expected;
    // This decimal has exactly DECQUAD_Pmax digits, so it fits into a decQuad.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "1000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 10));
    ION_ASSERT_OK(ion_decimal_from_int32(&fhs, 1));
    // The operation will try to keep this in decQuads, but detects overflow and upgrades them to decNumbers.
    ION_ASSERT_OK(ion_decimal_fma(&lhs, &lhs, &rhs, &fhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "10000000000000000000000000000000011.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &lhs));

    // Asserts that the operation results in a decNumber.
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, lhs.type);

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, fhs.type);

    ION_DECIMAL_FREE_4(&lhs, &rhs, &fhs, &expected);
}

TEST(IonDecimal, FMADecQuadOverflowsTwoOperandsSameAsOutput) {
    ION_DECIMAL lhs, rhs, expected;
    // This decimal has exactly DECQUAD_Pmax digits, so it fits into a decQuad.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "1000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 11));
    // The operation will try to keep this in decQuads, but detects overflow and upgrades them to decNumbers.
    ION_ASSERT_OK(ion_decimal_fma(&rhs, &lhs, &rhs, &rhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "11000000000000000000000000000000022.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &rhs));

    // Asserts that the operation results in a decNumber.
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, rhs.type);

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, lhs.type);

    ION_DECIMAL_FREE_3(&lhs, &rhs, &expected);
}

/* ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND tests */

TEST(IonDecimal, AddDecQuad) {
    ION_DECIMAL result, lhs, rhs, expected;
    // The operands are all backed by decQuads.
    ION_ASSERT_OK(ion_decimal_from_int32(&lhs, 9));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 1));
    ION_ASSERT_OK(ion_decimal_add(&result, &lhs, &rhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&expected, 10));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    ION_DECIMAL_FREE_4(&result, &lhs, &rhs, &expected);
}

TEST(IonDecimal, AddDecNumber) {
    ION_DECIMAL result, lhs, rhs, expected;
    // Because these decimals have more than DECQUAD_Pmax digits, they will be backed by decNumbers.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&rhs, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_add(&result, &lhs, &rhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "200000000000000000000000000000000000002.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    ION_DECIMAL_FREE_4(&result, &lhs, &rhs, &expected);
}

TEST(IonDecimal, AddMixed) {
    ION_DECIMAL result, lhs, rhs, expected;
    // Because this decimal has more than DECQUAD_Pmax digits, it will be backed by a decNumber.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "100000000000000000000000000000000000002.", &g_TestDecimalContext));

    // These operands are backed by decQuads. They will be temporarily converted to decNumbers to perform the calculation.
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, -1));
    ION_ASSERT_OK(ion_decimal_add(&result, &lhs, &rhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "100000000000000000000000000000000000001.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);

    ION_DECIMAL_FREE_4(&result, &lhs, &rhs, &expected);
}

TEST(IonDecimal, AddDecQuadOverflows) {
    ION_DECIMAL result, lhs, rhs, expected;
    // This decimal has exactly DECQUAD_Pmax digits, so it fits into a decQuad.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "9999999999999999999999999999999999.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 2));
    // The operation will try to keep this in decQuads, but detects overflow and upgrades them to decNumbers.
    ION_ASSERT_OK(ion_decimal_add(&result, &lhs, &rhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "10000000000000000000000000000000001.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &result));

    // Asserts that the operation results in a decNumber.
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, result.type);

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, lhs.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);

    ION_DECIMAL_FREE_4(&result, &lhs, &rhs, &expected);
}

TEST(IonDecimal, AddDecQuadOverflowsInPlace) {
    ION_DECIMAL lhs, rhs, expected;
    // This decimal has exactly DECQUAD_Pmax digits, so it fits into a decQuad.
    ION_ASSERT_OK(ion_decimal_from_string(&lhs, "9999999999999999999999999999999999.", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&rhs, 2));
    // The operation will try to keep this in decQuads, but detects overflow and upgrades them to decNumbers.
    ION_ASSERT_OK(ion_decimal_add(&lhs, &lhs, &rhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&expected, "10000000000000000000000000000000001.", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &lhs));

    // Asserts that the operation results in a decNumber.
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, lhs.type);

    // Asserts that the operation did not change the operands.
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, rhs.type);

    ION_DECIMAL_FREE_3(&lhs, &rhs, &expected);
}

TEST(IonDecimal, AddDecQuadInPlaceAllOperandsSame) {
    ION_DECIMAL lhs, expected;
    ION_ASSERT_OK(ion_decimal_from_int32(&lhs, 1));
    ION_ASSERT_OK(ion_decimal_add(&lhs, &lhs, &lhs, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&expected, 2));
    ASSERT_TRUE(assertIonDecimalEq(&expected, &lhs));
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, lhs.type);

    ION_DECIMAL_FREE_2(&lhs, &expected);
}

TEST(IonDecimal, CopySign) {
    ION_DECIMAL ion_number_positive, ion_quad_negative, ion_number_result;
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_positive, "999999999999999999999999999999999999999999", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_int32(&ion_quad_negative, -1));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_number_positive));
    ION_ASSERT_OK(ion_decimal_copy_sign(&ion_number_result, &ion_number_positive, &ion_quad_negative, &g_TestDecimalContext));
    ASSERT_TRUE(ion_decimal_is_negative(&ion_number_result));
    ION_ASSERT_OK(ion_decimal_minus(&ion_number_result, &ion_number_result, &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_positive, &ion_number_result));

    ION_DECIMAL_FREE_3(&ion_number_positive, &ion_quad_negative, &ion_number_result);
}

/* Comparison tests */

TEST(IonDecimal, EqualsWithMixedOperands) {
    decNumber number;
    decQuad quad;
    ION_DECIMAL lhs, rhs;

    // Note: no need to allocate extra space for this decNumber because it always has at least one decimal unit
    // available (and 7 fits in one decimal unit).
    decNumberFromInt32(&number, 7);
    decQuadFromInt32(&quad, 7);
    ION_ASSERT_OK(ion_decimal_from_number(&lhs, &number));
    ION_ASSERT_OK(ion_decimal_from_quad(&rhs, &quad));

    ASSERT_TRUE(assertIonDecimalEq(&lhs, &rhs));
    ASSERT_TRUE(assertIonDecimalEq(&rhs, &lhs));
    ASSERT_TRUE(assertIonDecimalEq(&rhs, &rhs));
    ASSERT_TRUE(assertIonDecimalEq(&lhs, &lhs));

    ION_DECIMAL_FREE_2(&lhs, &rhs);
}

/* ION_DECIMAL_UTILITY_API_BUILDER tests */

TEST(IonDecimal, IsNegative) {
    ION_DECIMAL ion_number_positive, ion_number_negative, ion_quad_positive, ion_quad_negative;
    decNumber number_positive, number_negative;
    decQuad quad_positive, quad_negative;

    decNumberFromInt32(&number_positive, 1);
    decNumberFromInt32(&number_negative, -1);
    decQuadFromInt32(&quad_positive, 1);
    decQuadFromInt32(&quad_negative, -1);

    ION_ASSERT_OK(ion_decimal_from_number(&ion_number_positive, &number_positive));
    ION_ASSERT_OK(ion_decimal_from_number(&ion_number_negative, &number_negative));
    ION_ASSERT_OK(ion_decimal_from_quad(&ion_quad_positive, &quad_positive));
    ION_ASSERT_OK(ion_decimal_from_quad(&ion_quad_negative, &quad_negative));

    ASSERT_TRUE(ion_decimal_is_negative(&ion_number_negative));
    ASSERT_TRUE(ion_decimal_is_negative(&ion_quad_negative));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_number_positive));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_quad_positive));

    ION_DECIMAL_FREE_4(&ion_number_positive, &ion_number_negative, &ion_quad_positive, &ion_quad_negative);
}

/* ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND tests */

TEST(IonDecimal, AbsQuad) {
    ION_DECIMAL ion_quad_negative, ion_quad_positive, ion_quad_positive_result;
    ION_ASSERT_OK(ion_decimal_from_int32(&ion_quad_negative, -999999));
    ION_ASSERT_OK(ion_decimal_from_int32(&ion_quad_positive, 999999));
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, ion_quad_negative.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_QUAD, ion_quad_positive.type);
    ASSERT_TRUE(ion_decimal_is_negative(&ion_quad_negative));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_quad_positive));
    ION_ASSERT_OK(ion_decimal_abs(&ion_quad_negative, &ion_quad_negative, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_abs(&ion_quad_positive_result, &ion_quad_positive, &g_TestDecimalContext));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_quad_negative));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_quad_positive));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_quad_positive_result));
    ASSERT_TRUE(assertIonDecimalEq(&ion_quad_positive, &ion_quad_negative));
    ASSERT_TRUE(assertIonDecimalEq(&ion_quad_positive, &ion_quad_positive_result));

    ION_DECIMAL_FREE_3(&ion_quad_positive, &ion_quad_negative, &ion_quad_positive_result);
}

TEST(IonDecimal, AbsNumber) {
    ION_DECIMAL ion_number_negative, ion_number_positive, ion_number_positive_result;
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_negative, "-999999999999999999999999999999999999999999", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_positive, "999999999999999999999999999999999999999999", &g_TestDecimalContext));
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, ion_number_negative.type);
    ASSERT_EQ(ION_DECIMAL_TYPE_NUMBER, ion_number_positive.type);
    ASSERT_TRUE(ion_decimal_is_negative(&ion_number_negative));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_number_positive));
    ION_ASSERT_OK(ion_decimal_abs(&ion_number_negative, &ion_number_negative, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_abs(&ion_number_positive_result, &ion_number_positive, &g_TestDecimalContext));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_number_negative));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_number_positive));
    ASSERT_FALSE(ion_decimal_is_negative(&ion_number_positive_result));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_positive, &ion_number_negative));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_positive, &ion_number_positive_result));

    ION_DECIMAL_FREE_3(&ion_number_positive, &ion_number_negative, &ion_number_positive_result);
}

/* ION_DECIMAL_BASIC_API_BUILDER tests */

TEST(IonDecimal, ToIntegralValue) {
    ION_DECIMAL ion_quad, ion_quad_expected, ion_number, ion_number_expected, ion_number_result;
    ION_ASSERT_OK(ion_decimal_from_string(&ion_quad, "9999.999e3", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number, "999999999999999999999999999999999999999999.999e3", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_to_integral_value(&ion_quad, &ion_quad, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_to_integral_value(&ion_number_result, &ion_number, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_quad_expected, "9999999", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_expected, "999999999999999999999999999999999999999999999", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&ion_quad_expected, &ion_quad));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_expected, &ion_number_result));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_expected, &ion_number));

    ION_DECIMAL_FREE_5(&ion_quad, &ion_quad_expected, &ion_number, &ion_number_expected, &ion_number_result);
}

TEST(IonDecimal, ToIntegralValueRounded) {
    ION_DECIMAL ion_quad, ion_quad_expected, ion_number, ion_number_expected, ion_number_result;
    ION_ASSERT_OK(ion_decimal_from_string(&ion_quad, "9998.999", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number, "999999999999999999999999999999999999999998.999", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_to_integral_value(&ion_quad, &ion_quad, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_to_integral_value(&ion_number_result, &ion_number, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_quad_expected, "9999", &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_expected, "999999999999999999999999999999999999999999", &g_TestDecimalContext));
    ASSERT_TRUE(assertIonDecimalEq(&ion_quad_expected, &ion_quad));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_expected, &ion_number_result));
    ASSERT_FALSE(assertIonDecimalEq(&ion_number_expected, &ion_number));

    ION_DECIMAL_FREE_5(&ion_quad, &ion_quad_expected, &ion_number, &ion_number_expected, &ion_number_result);
}

/* To/From string tests */

TEST(IonDecimal, ToAndFromString) {
    ION_DECIMAL ion_quad, ion_number_small, ion_number_large;
    ION_DECIMAL ion_quad_after, ion_number_small_after, ion_number_large_after;
    char *quad_str, *small_str, *large_str;
    decQuad quad;
    decNumber number_small;
    decQuadZero(&quad);
    ION_ASSERT_OK(ion_decimal_from_quad(&ion_quad, &quad));
    decNumberZero(&number_small);
    ION_ASSERT_OK(ion_decimal_from_number(&ion_number_small, &number_small));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_large, "-999999999999999999999999999999999999999999.999d-3", &g_TestDecimalContext));

    ASSERT_EQ(DECQUAD_String, ION_DECIMAL_STRLEN(&ion_quad));
    ASSERT_EQ(1 + 14, ION_DECIMAL_STRLEN(&ion_number_small));
    ASSERT_EQ(45 + 14, ION_DECIMAL_STRLEN(&ion_number_large));

    quad_str = (char *)malloc(ION_DECIMAL_STRLEN(&ion_quad));
    small_str = (char *)malloc(ION_DECIMAL_STRLEN(&ion_number_small));
    large_str = (char *)malloc(ION_DECIMAL_STRLEN(&ion_number_large));

    ION_ASSERT_OK(ion_decimal_to_string(&ion_quad, quad_str));
    ION_ASSERT_OK(ion_decimal_to_string(&ion_number_small, small_str));
    ION_ASSERT_OK(ion_decimal_to_string(&ion_number_large, large_str));

    ION_ASSERT_OK(ion_decimal_from_string(&ion_quad_after, quad_str, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_small_after, small_str, &g_TestDecimalContext));
    ION_ASSERT_OK(ion_decimal_from_string(&ion_number_large_after, large_str, &g_TestDecimalContext));

    ASSERT_TRUE(assertIonDecimalEq(&ion_quad, &ion_quad_after));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_small, &ion_number_small_after));
    ASSERT_TRUE(assertIonDecimalEq(&ion_number_large, &ion_number_large_after));

    free(quad_str);
    free(small_str);
    free(large_str);
    ION_DECIMAL_FREE_5(&ion_quad, &ion_number_small, &ion_number_large, &ion_quad_after, &ion_number_small_after);
    ION_DECIMAL_FREE_1(&ion_number_large_after);
}
