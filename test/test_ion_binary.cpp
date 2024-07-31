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
#include "ion_event_stream.h"
#include "ion_event_stream_impl.h"
#include "ion_helpers.h"
#include "ion_test_util.h"
#include "ion_event_equivalence.h"

TEST(IonBinaryLen, UInt64) {

    ASSERT_EQ(0, ion_binary_len_uint_64(0LL));
    ASSERT_EQ(1, ion_binary_len_uint_64(1LL));
    ASSERT_EQ(1, ion_binary_len_uint_64(255LL));
    ASSERT_EQ(2, ion_binary_len_uint_64(256LL));
}

TEST(IonBinaryLen, Int64) {
    ASSERT_EQ(0, ion_binary_len_int_64(0LL));
    ASSERT_EQ(1, ion_binary_len_int_64(1LL));
    ASSERT_EQ(1, ion_binary_len_int_64(-1LL));
    ASSERT_EQ(2, ion_binary_len_int_64(255LL));
    ASSERT_EQ(2, ion_binary_len_int_64(-255LL));
    ASSERT_EQ(2, ion_binary_len_int_64(256LL));
    ASSERT_EQ(2, ion_binary_len_int_64(-256LL));
}

iERR ion_test_add_annotations(BOOL is_binary, BYTE **out, SIZE *len) {
    iENTER;
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    ION_STRING annot1_str, annot2_str;
    IONCHECK(ion_string_from_cstr("annot1", &annot1_str));
    IONCHECK(ion_string_from_cstr("annot2", &annot2_str));
    IONCHECK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    IONCHECK(ion_writer_add_annotation(writer, &annot1_str));
    IONCHECK(ion_writer_add_annotation(writer, &annot2_str));
    IONCHECK(ion_writer_write_int(writer, 42));
    IONCHECK(ion_test_writer_get_bytes(writer, ion_stream, out, len));
    iRETURN;
}

TEST(IonWriterAddAnnotation, SameInTextAndBinary) {
    BYTE *binary_data, *text_data;
    SIZE binary_len, text_len;
    IonEventStream binary_stream, text_stream;
    ION_ASSERT_OK(ion_test_add_annotations(TRUE, &binary_data, &binary_len));
    ION_ASSERT_OK(ion_test_add_annotations(FALSE, &text_data, &text_len));
    ION_ASSERT_OK(ion_event_stream_read_all_from_bytes(binary_data, binary_len, NULL, &binary_stream));
    ION_ASSERT_OK(ion_event_stream_read_all_from_bytes(text_data, text_len, NULL, &text_stream));
    ASSERT_TRUE(ion_compare_streams(&binary_stream, &text_stream));
    free(binary_data);
    free(text_data);
}

TEST(IonBinaryTimestamp, WriterConvertsToUTC) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_TIMESTAMP timestamp;

    ION_ASSERT_OK(ion_timestamp_for_minute(&timestamp, 2008, 3, 1, 0, 0));
    ION_ASSERT_OK(ion_timestamp_set_local_offset(&timestamp, 1));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_timestamp(writer, &timestamp));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // Expected: 2008-02-29T23:59 with offset of +1 minutes.
    assertBytesEqual("\xE0\x01\x00\xEA\x67\x81\x0F\xD8\x82\x9D\x97\xBB", 12, result, result_len);

    free(result);
}

TEST(IonBinaryTimestamp, ReaderConvertsFromUTC) {
    hREADER reader;
    BYTE *timestamp = (BYTE *)"\xE0\x01\x00\xEA\x67\x81\x0F\xD8\x82\x9D\x97\xBB";
    ION_TYPE actual_type;
    ION_TIMESTAMP expected, actual;
    ION_ASSERT_OK(ion_timestamp_for_minute(&expected, 2008, 3, 1, 0, 0));
    ION_ASSERT_OK(ion_timestamp_set_local_offset(&expected, 1));

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, timestamp, 12, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_TIMESTAMP, actual_type);
    ION_ASSERT_OK(ion_reader_read_timestamp(reader, &actual));
    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_TRUE(ion_equals_timestamp(&expected, &actual));
}

TEST(IonBinaryTimestamp, WriterIgnoresSuperfluousOffset) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_TIMESTAMP timestamp;

    ION_ASSERT_OK(ion_timestamp_for_year(&timestamp, 1));
    SET_FLAG_ON(timestamp.precision, ION_TT_BIT_TZ);
    timestamp.tz_offset = 1;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_timestamp(writer, &timestamp));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // Expected: 0001T with unknown local offset (C0 == -0 == unknown local offset).
    assertBytesEqual("\xE0\x01\x00\xEA\x62\xC0\x81", 7, result, result_len);

    free(result);
}

TEST(IonBinaryTimestamp, ReaderIgnoresSuperfluousOffset) {
    hREADER reader;
    BYTE *timestamp = (BYTE *)"\xE0\x01\x00\xEA\x62\x81\x81";
    ION_TYPE actual_type;
    ION_TIMESTAMP expected, actual;
    ION_ASSERT_OK(ion_timestamp_for_year(&expected, 1));

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, timestamp, 7, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_TIMESTAMP, actual_type);
    ION_ASSERT_OK(ion_reader_read_timestamp(reader, &actual));
    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_TRUE(ion_equals_timestamp(&expected, &actual));
}

TEST(IonBinarySymbol, WriterWritesSymbolValueThatLooksLikeSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING symbol_zero;

    ion_string_from_cstr("$0", &symbol_zero);

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &symbol_zero));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: the symbol value refers to \x0A (i.e. SID 10 -- a local symbol), NOT \x00 (SID 0). This is because the
    // ion_writer_write_symbol API, which takes a string from the user, was used.
    assertBytesEqual("\x71\x0A", 2, result + result_len - 2, 2);
    free(result);
}

TEST(IonBinarySymbol, WriterWritesSymbolValueSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: symbol zero is NOT added to the local symbol table. Symbol zero is not present in ANY symbol table.
    assertBytesEqual("\xE0\x01\x00\xEA\x70", 5, result, result_len);
    free(result);
}

TEST(IonBinarySymbol, WriterWritesAnnotationThatLooksLikeSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING symbol_zero;

    ion_string_from_cstr("$0", &symbol_zero);

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &symbol_zero));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: the annotation refers to \x8A (i.e. SID 10 -- a local symbol), NOT \x80 (SID 0). This is because the
    // ion_writer_add_annotation API, which takes a string from the user, was used.
    assertBytesEqual("\xE3\x81\x8A\x70", 4, result + result_len - 4, 4);
    free(result);
}

TEST(IonBinarySymbol, WriterWritesAnnotationSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: symbol zero is NOT added to the local symbol table. Symbol zero is not present in ANY symbol table.
    assertBytesEqual("\xE0\x01\x00\xEA\xE3\x81\x80\x70", 8, result, result_len);
    free(result);
}

TEST(IonBinarySymbol, WriterWritesFieldNameThatLooksLikeSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING symbol_zero;

    ion_string_from_cstr("$0", &symbol_zero);

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &symbol_zero));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &symbol_zero));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: the field name and annotation refer to \x8A (i.e. SID 10 -- a local symbol), NOT \x80 (SID 0).
    // This is due to use of APIs that accept a string from the user.
    assertBytesEqual("\xD5\x8A\xE3\x81\x8A\x70", 6, result + result_len - 6, 6);
    free(result);
}

TEST(IonBinarySymbol, WriterWritesFieldNameSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: symbol zero is NOT added to the local symbol table. Symbol zero is not present in ANY symbol table.
    assertBytesEqual("\xE0\x01\x00\xEA\xD5\x80\xE3\x81\x80\x70", 10, result, result_len);
    free(result);
}

TEST(IonBinarySymbol, ReaderReadsSymbolValueZeroAsString) {
    hREADER reader;
    BYTE *symbol_zero = (BYTE *)"\xE0\x01\x00\xEA\x70";
    ION_TYPE actual_type;
    ION_STRING actual;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, symbol_zero, 5, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_SYMBOL, actual_type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &actual));
    ION_ASSERT_OK(ion_reader_close(reader));
    ASSERT_TRUE(ION_STRING_IS_NULL(&actual));
}

TEST(IonBinarySymbol, ReaderReadsSymbolValueZeroAsSID) {
    hREADER reader;
    BYTE *symbol_zero = (BYTE *)"\xE0\x01\x00\xEA\x70";
    ION_TYPE actual_type;
    SID actual;
    hSYMTAB symbol_table;
    ION_STRING *symbol_value;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, symbol_zero, 5, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_SYMBOL, actual_type);
    ION_ASSERT_OK(ion_test_reader_read_symbol_sid(reader, &actual));

    ASSERT_EQ(0, actual);

    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &symbol_table));
    ION_ASSERT_OK(ion_symbol_table_find_by_sid(symbol_table, 0, &symbol_value));
    ASSERT_TRUE(ION_STRING_IS_NULL(symbol_value));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinarySymbol, WriterWritesSymbolValueIVM) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING ivm_text;

    ION_ASSERT_OK(ion_string_from_cstr("$ion_1_0", &ivm_text));
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));

    ION_ASSERT_OK(ion_writer_write_int(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &ivm_text)); // This is a no-op.
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 2)); // This is a no-op.
    ION_ASSERT_OK(ion_writer_write_int(writer, 2));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertBytesEqual("\xE0\x01\x00\xEA\x20\x21\x01\x21\x02", 9, result, result_len);
    free(result);
}

TEST(IonBinarySymbol, ReaderReadsSymbolValueIVMNoOpAtEOF) {
    hREADER reader;
    BYTE *data = (BYTE *)"\xE0\x01\x00\xEA\x71\x02";
    ION_TYPE actual_type;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, 6, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_EOF, actual_type);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinarySymbol, ReaderReadsSymbolValueIVMNoOp) {
    hREADER reader;
    BYTE *data = (BYTE *)"\xE0\x01\x00\xEA\x71\x02\x71\x04";
    ION_TYPE actual_type;
    SID sid;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, 8, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_SYMBOL, actual_type);
    ION_ASSERT_OK(ion_test_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(4, sid);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinarySymbol, ReaderReadsIVMInsideAnnotationWrapper) {
    hREADER reader;
    BYTE *data = (BYTE *)"\xE0\x01\x00\xEA\xE4\x81\x84\x71\x02";
    ION_TYPE actual_type;
    SID sid;
    ION_STRING annotation;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, 9, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &actual_type));
    ASSERT_EQ(tid_SYMBOL, actual_type);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation));
    assertStringsEqual("name", (char *)annotation.value, annotation.length); // SID 4 is "name"
    ION_ASSERT_OK(ion_test_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(2, sid);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryReader, UnpositionedReaderHasTypeNone) {
    hREADER reader;
    BYTE *data = (BYTE *) "\xE0\x01\x00\xEA";
    ION_TYPE type;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, 4, NULL));
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ASSERT_EQ(tid_none, type);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinarySymbol, ReaderReadsNullSymbol) {
    hREADER reader;
    BYTE *data = (BYTE *) "\xE0\x01\x00\xEA\x7F";
    BOOL is_null;
    ION_TYPE type;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, 5, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_is_null(reader, &is_null));
    ASSERT_TRUE(is_null);
    ION_ASSERT_OK(ion_reader_close(reader));
}

void test_ion_binary_reader_rejects_negative_zero_int64(BYTE *data, size_t len) {
    hREADER reader;
    ION_TYPE type;
    int64_t value;
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(type, tid_INT);
    ION_ASSERT_FAIL(ion_reader_read_int64(reader, &value));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryInt, ReaderRejectsNegativeZeroInt64OneByte) {
    test_ion_binary_reader_rejects_negative_zero_int64((BYTE *)"\xE0\x01\x00\xEA\x30", 5);
}

TEST(IonBinaryInt, ReaderRejectsNegativeZeroInt64TwoByte) {
    test_ion_binary_reader_rejects_negative_zero_int64((BYTE *)"\xE0\x01\x00\xEA\x31\x00", 6);
}

void test_ion_binary_write_from_reader_rejects_negative_zero_int(BYTE *data, size_t len) {
    hREADER reader;
    hWRITER writer;
    ION_STREAM *stream;
    ION_TYPE type;
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, len, NULL));
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, FALSE));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(type, tid_INT);
    ION_ASSERT_FAIL(ion_writer_write_one_value(writer, reader));
    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST(IonBinaryInt, ReaderRejectsNegativeZeroMixedIntOneByte) {
    test_ion_binary_write_from_reader_rejects_negative_zero_int((BYTE *)"\xE0\x01\x00\xEA\x30", 5);
}

TEST(IonBinaryInt, ReaderRejectsNegativeZeroMixedIntTwoByte) {
    test_ion_binary_write_from_reader_rejects_negative_zero_int((BYTE *)"\xE0\x01\x00\xEA\x31\x00", 6);
}

void test_ion_binary_reader_threshold_for_int64_as_big_int(BYTE *data, size_t len, const char *actual_value) {
    hREADER reader;
    ION_TYPE type;
    int64_t value;
    ION_INT *big_int_expected;
    SIZE str_len, written;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);

    // reading value as int64 will throw numeric overflow error
    ASSERT_EQ(IERR_NUMERIC_OVERFLOW,ion_reader_read_int64(reader, &value));

    // initialize ION_INT and read it as big integer
    ION_ASSERT_OK(ion_int_alloc(NULL, &big_int_expected));
    ION_ASSERT_OK(ion_int_init(big_int_expected, NULL));
    ION_ASSERT_OK(ion_reader_read_ion_int(reader, big_int_expected));
    ION_ASSERT_OK(ion_reader_close(reader));

    // convert big integer to string for comparison
    ion_int_char_length(big_int_expected, &str_len);
    char *int_str = (char *)malloc(str_len * sizeof(char));
    ion_int_to_char(big_int_expected, (BYTE *)int_str, str_len, &written);
    ion_int_free(big_int_expected);

    // compare string representation of the value
    ASSERT_STREQ(actual_value, int_str);

    free(int_str);
}

void test_ion_binary_reader_threshold_for_int64_as_int64(BYTE *data, size_t len, int64_t actual_value) {
    hREADER reader;
    ION_TYPE type;
    int64_t value;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);

    // reading value as int64 will not throw numeric overflow error as it fits two's complement representation
    ION_ASSERT_OK(ion_reader_read_int64(reader, &value));

    // compare actual and generated int64 values
    ASSERT_EQ(actual_value, value);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryReader, ReaderPositiveThresholdForInt64) {
    // 2 ** 64
    test_ion_binary_reader_threshold_for_int64_as_big_int((BYTE *)"\xE0\x01\x00\xEA\x28\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 13, "18446744073709551615");
    // 2 ** 63
    test_ion_binary_reader_threshold_for_int64_as_big_int((BYTE *)"\xE0\x01\x00\xEA\x28\x80\x00\x00\x00\x00\x00\x00\x00", 13, "9223372036854775808");
}

TEST(IonBinaryReader, ReaderNegativeThresholdForInt64) {
    // -2 ** 64
    test_ion_binary_reader_threshold_for_int64_as_big_int((BYTE *)"\xE0\x01\x00\xEA\x38\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 13, "-18446744073709551615");
    // -2 ** 63 fits as two's complement representation
    test_ion_binary_reader_threshold_for_int64_as_int64((BYTE *)"\xE0\x01\x00\xEA\x38\x80\x00\x00\x00\x00\x00\x00\x00", 13, 0x8000000000000000 /* -9223372036854775808 */);
}

void test_ion_binary_reader_requires_timestamp_fraction_less_than_one(BYTE *data, size_t len) {
    hREADER reader;
    ION_TYPE type;
    ION_TIMESTAMP ts;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_TIMESTAMP, type);
    ASSERT_EQ(IERR_INVALID_BINARY, ion_reader_read_timestamp(reader, &ts));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryTimestamp, ReaderRequiresTimestampFractionLessThanOne) {
    // 0001-01-01T00:00:00.<1d0>Z
    test_ion_binary_reader_requires_timestamp_fraction_less_than_one((BYTE *) "\xE0\x01\x00\xEA\x69\x80\x81\x81\x81\x80\x80\x80\x80\x01", 14);
    // 0001-01-01T00:00:00.<10d-1>Z
    test_ion_binary_reader_requires_timestamp_fraction_less_than_one((BYTE *) "\xE0\x01\x00\xEA\x69\x80\x81\x81\x81\x80\x80\x80\xC1\x0A", 14);
    // 0001-01-01T00:00:00.<11d-1>Z
    test_ion_binary_reader_requires_timestamp_fraction_less_than_one((BYTE *) "\xE0\x01\x00\xEA\x69\x80\x81\x81\x81\x80\x80\x80\xC1\x0B", 14);
}

void test_ion_binary_reader_supports_32_bit_floats(BYTE *data, size_t len, float expected) {
    hREADER reader;
    ION_TYPE type;
    double actual;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_FLOAT, type);
    ION_ASSERT_OK(ion_reader_read_double(reader, &actual));
    ASSERT_FLOAT_EQ(expected, (float) actual);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryFloat, ReaderSupports32BitFloats) {
    int neg_inf_bits = 0xFF800000;
    float neg_inf = *((float *)&neg_inf_bits);
    int pos_inf_bits = 0x7F800000;
    float pos_inf = *((float *)&pos_inf_bits);

    // See https://amazon-ion.github.io/ion-docs/docs/binary.html#4-float
    // "If L is 0, then the the value is 0e0 and representation is empty."
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x40", 5, 0.);
    // Positive 0 can also be written out with 4 bytes instead
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\x00\x00\x00\x00", 9, 0.);
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\x80\x00\x00\x00", 9, -0.);
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\x40\x86\x66\x66", 9, 4.2);
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\xC0\x86\x66\x66", 9, -4.2);
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\xFF\x80\x00\x00", 9, neg_inf);
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\x7F\x80\x00\x00", 9, pos_inf);
    // minimum 32-bit float
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\xFF\x7F\xFF\xFF", 9, -3.4028235E38);
    // maximum 32-bit float
    test_ion_binary_reader_supports_32_bit_floats((BYTE *) "\xE0\x01\x00\xEA\x44\x7F\x7F\xFF\xFF", 9, 3.4028235E38);
}

TEST(IonBinaryFloat, ReaderSupports32BitFloatNan) {
    hREADER reader;
    ION_TYPE type;
    int nan_bits = 0x7FFFFFFF;
    float nan = *((float *)&nan_bits);
    double actual;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *) "\xE0\x01\x00\xEA\x44\x7F\xFF\xFF\xFF", 9, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_FLOAT, type);
    ION_ASSERT_OK(ion_reader_read_double(reader, &actual));
    ASSERT_TRUE(std::isnan(nan));
    ASSERT_TRUE(std::isnan(actual));
    ION_ASSERT_OK(ion_reader_close(reader));
}

/**
 * Creates a new reader which reads ion_text and asserts that the next value is of expected_type and of expected_lob_size
 */
void open_binary_reader_read_lob_size(const char *ion_text, SIZE buff_size, ION_TYPE expected_type, SIZE expected_lob_size, hREADER &reader) {
    ION_TYPE type;
    SIZE lob_size;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE*)ion_text, buff_size, NULL));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(expected_type, type);

    ION_ASSERT_OK(ion_reader_get_lob_size(reader, &lob_size));
    ASSERT_EQ(expected_lob_size, lob_size);
}

/** Tests the ability to read a CLOB or a BLOB using ion_reader_read_lob_bytes. */
void test_full_binary_lob_read(const char *ion_text, SIZE buff_size, ION_TYPE expected_tid, SIZE expected_size, const char *expected_value) {
    hREADER reader;
    open_binary_reader_read_lob_size(ion_text, buff_size, expected_tid, expected_size, reader);

    BYTE *bytes = (BYTE*)calloc(1, expected_size + 1);

    SIZE bytes_read;
    ION_ASSERT_OK(ion_reader_read_lob_bytes(reader, bytes, expected_size, &bytes_read));
    ASSERT_EQ(expected_size, bytes_read);

    ASSERT_EQ(strcmp(expected_value, (char*)bytes), 0);

    free(bytes);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryClob, CanReadClob) {
    test_full_binary_lob_read(
            "\xE0\x01\x00\xEA\x9E\x97\x54\x68\x69\x73\x20\x69\x73\x20\x61\x20\x43\x4C\x4F\x42\x20\x6F\x66\x20\x74\x65\x78\x74\x2E",
            29, tid_CLOB, 23, "This is a CLOB of text.");
}

TEST(IonBinaryBlob, CanReadBlob) {
    test_full_binary_lob_read(
            "\xE0\x01\x00\xEA\xAE\x97\x54\x68\x69\x73\x20\x69\x73\x20\x61\x20\x42\x4C\x4F\x42\x20\x6F\x66\x20\x74\x65\x78\x74\x2E",
            29, tid_BLOB, 23, "This is a BLOB of text.");
}

/** Tests the ability to read BLOB or CLOB using multiple calls to ion_reader_read_lob_partial_bytes. */
void test_partial_binary_lob_read(const char *ion_text, SIZE buff_size, ION_TYPE expected_tid, SIZE expected_size, const char *expected_value) {
    hREADER reader;
    open_binary_reader_read_lob_size(ion_text, buff_size, expected_tid, expected_size, reader);

    BYTE* bytes = (BYTE*)calloc(1, expected_size + 1);
    SIZE bytes_read, total_bytes_read = 0;

    const size_t READ_SIZE = 5;
    do
    {
        ION_ASSERT_OK(ion_reader_read_lob_partial_bytes(reader, &bytes[total_bytes_read], READ_SIZE, &bytes_read));
        total_bytes_read += bytes_read;
    } while (bytes_read > 0);

    ASSERT_EQ(expected_size, total_bytes_read);
    char* lob_text = (char*)bytes;
    ASSERT_EQ(strcmp(expected_value, lob_text), 0);

    free(bytes);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinaryClob, CanFullyReadClobUsingPartialReads) {
    test_partial_binary_lob_read(
            "\xE0\x01\x00\xEA\x9E\x97\x54\x68\x69\x73\x20\x69\x73\x20\x61\x20\x43\x4C\x4F\x42\x20\x6F\x66\x20\x74\x65\x78\x74\x2E",
            29, tid_CLOB, 23, "This is a CLOB of text.");
}

TEST(IonBinaryBlob, CanFullyReadBlobUsingPartialReads) {
    test_partial_binary_lob_read(
            "\xE0\x01\x00\xEA\xAE\x97\x54\x68\x69\x73\x20\x69\x73\x20\x61\x20\x42\x4C\x4F\x42\x20\x6F\x66\x20\x74\x65\x78\x74\x2E",
            29, tid_BLOB, 23, "This is a BLOB of text.");
}

// Simple test to ensure that if we supply a buffer size of 0 to ion_reader_read_lob_bytes, we don't assert. If the user
// is reading values via the LOB size, and does not specifically handle 0-lengthed LOBs the reader shouldn't fail.
TEST(IonBinaryBlob, CanReadZeroLength) {
    hREADER reader;
    ION_TYPE type;
    const char *buffer = "\xE0\x01\x00\xEA\xA0";
    char bytes[1]; // Shouldn't write any..

    SIZE lob_size, bytes_read;
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE*)buffer, 5, NULL));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_BLOB, type);
    ION_ASSERT_OK(ion_reader_get_lob_size(reader, &lob_size));
    ASSERT_EQ(0, lob_size);
    ION_ASSERT_OK(ion_reader_read_lob_bytes(reader, (BYTE*)bytes, lob_size, &bytes_read));
    ASSERT_EQ(0, bytes_read);

    ION_ASSERT_OK(ion_reader_close(reader));
}

void test_ion_binary_writer_supports_32_bit_floats(float value, const char *expected, SIZE expected_len) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_float(writer, value));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertBytesEqual(expected, expected_len, result, result_len);

    free(result);
}


TEST(IonBinaryFloat, WriterSupports32BitFloats) {
    uint32_t neg_inf_bits = 0xFF800000;
    float neg_inf = *((float *)&neg_inf_bits);
    uint32_t pos_inf_bits = 0x7F800000;
    float pos_inf = *((float *)&pos_inf_bits);
    uint32_t nan_bits = 0x7FFFFFFF;
    float nan = *((float *)&nan_bits);

    // ion-c prefers to write positive zero as a zero-length float
    // see: https://amazon-ion.github.io/ion-docs/docs/binary.html#4-float
    test_ion_binary_writer_supports_32_bit_floats(0., "\xE0\x01\x00\xEA\x40", 5);
    test_ion_binary_writer_supports_32_bit_floats(-0., "\xE0\x01\x00\xEA\x44\x80\x00\x00\x00", 9);
    test_ion_binary_writer_supports_32_bit_floats(4.2, "\xE0\x01\x00\xEA\x44\x40\x86\x66\x66", 9);
    test_ion_binary_writer_supports_32_bit_floats(-4.2, "\xE0\x01\x00\xEA\x44\xC0\x86\x66\x66", 9);

    test_ion_binary_writer_supports_32_bit_floats(neg_inf, "\xE0\x01\x00\xEA\x44\xFF\x80\x00\x00", 9);
    test_ion_binary_writer_supports_32_bit_floats(pos_inf, "\xE0\x01\x00\xEA\x44\x7F\x80\x00\x00", 9);
    test_ion_binary_writer_supports_32_bit_floats(nan, "\xE0\x01\x00\xEA\x44\x7F\xFF\xFF\xFF", 9);

    // minimum 32-bit float
    test_ion_binary_writer_supports_32_bit_floats(-3.4028235E38, "\xE0\x01\x00\xEA\x44\xFF\x7F\xFF\xFF", 9);
    // maximum 32-bit float
    test_ion_binary_writer_supports_32_bit_floats(3.4028235E38, "\xE0\x01\x00\xEA\x44\x7F\x7F\xFF\xFF", 9);
}

void test_ion_binary_writer_supports_compact_floats(BOOL compact_floats, double value, const char *expected,
                                                   SIZE expected_len) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));

    writer->options.compact_floats = compact_floats;

    ION_ASSERT_OK(ion_writer_write_double(writer, value));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertBytesEqual(expected, expected_len, result, result_len);

    free(result);
}

TEST(IonBinaryFloat, WriterSupportsCompactFloatsOption) {
    // ion-c prefers to write positive zero as a zero-length float, whether this option is enabled or not
    // see: https://amazon-ion.github.io/ion-docs/docs/binary.html#4-float
    test_ion_binary_writer_supports_compact_floats(TRUE, 0., "\xE0\x01\x00\xEA\x40", 5);
    test_ion_binary_writer_supports_compact_floats(FALSE, 0., "\xE0\x01\x00\xEA\x40", 5);

    // Negative zero can save some bits with a 32-bit representation
    test_ion_binary_writer_supports_compact_floats(TRUE, -0., "\xE0\x01\x00\xEA\x44\x80\x00\x00\x00", 9);
    test_ion_binary_writer_supports_compact_floats(FALSE, -0., "\xE0\x01\x00\xEA\x48\x80\x00\x00\x00\x00\x00\x00\x00", 13);

    double original = 4.2;
    float truncated = (float)original;
    // The closest double approximation of decimal "4.2":
    // * In single precision is something like: 4.1999998############# (# = precision unachievable by representation)
    // * In double precision is something like: 4.200000000000000##### (# = precision unachievable by representation)
    // s = sign, e = exponent, f = fraction (mantissa, significand, whatever)
    // seee eeee eeee ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff
    // 0100 0000 0001 0000 1100 1100 1100 1100 1100 1100 1100 1100 1100 1100 1100 1101
    //    4    0    1    0    c    c    c    c    c    c    c    c    c    c    c    d
    test_ion_binary_writer_supports_compact_floats(TRUE, original, "\xE0\x01\x00\xEA\x48\x40\x10\xCC\xCC\xCC\xCC\xCC\xCD", 13);
    // seee eeee eeee ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff ffff
    // 0100 0000 0001 0000 1100 1100 1100 1100 1100 0000 0000 0000 0000 0000 0000 0000
    //    4    0    1    0    c    c    c    c    c    0    0    0    0    0    0    0
    test_ion_binary_writer_supports_compact_floats(FALSE, truncated, "\xE0\x01\x00\xEA\x48\x40\x10\xCC\xCC\xC0\x00\x00\x00", 13);
    // seee eeee efff ffff ffff ffff ffff ffff
    // 0100 0000 1000 0110 0110 0110 0110 0110
    //    4    0    8    6    6    6    6    6
    test_ion_binary_writer_supports_compact_floats(TRUE, truncated, "\xE0\x01\x00\xEA\x44\x40\x86\x66\x66", 9);
}
