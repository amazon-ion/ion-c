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
#include "ion_helpers.h"
#include "ion_test_util.h"

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
    ION_ASSERT_OK(read_value_stream_from_bytes(binary_data, binary_len, &binary_stream, NULL));
    ION_ASSERT_OK(read_value_stream_from_bytes(text_data, text_len, &text_stream, NULL));
    assertIonEventStreamEq(&binary_stream, &text_stream, ASSERTION_TYPE_NORMAL);
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

    ASSERT_TRUE(assertIonTimestampEq(&expected, &actual));
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

    ASSERT_TRUE(assertIonTimestampEq(&expected, &actual));
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
}

TEST(IonBinarySymbol, WriterWritesSymbolValueSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: symbol zero is NOT added to the local symbol table. Symbol zero is not present in ANY symbol table.
    assertBytesEqual("\xE0\x01\x00\xEA\x70", 5, result, result_len);
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
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: the annotation refers to \x8A (i.e. SID 10 -- a local symbol), NOT \x80 (SID 0). This is because the
    // ion_writer_add_annotation API, which takes a string from the user, was used.
    assertBytesEqual("\xE3\x81\x8A\x70", 4, result + result_len - 4, 4);
}

TEST(IonBinarySymbol, WriterWritesAnnotationSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: symbol zero is NOT added to the local symbol table. Symbol zero is not present in ANY symbol table.
    assertBytesEqual("\xE0\x01\x00\xEA\xE3\x81\x80\x70", 8, result, result_len);
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
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: the field name and annotation refer to \x8A (i.e. SID 10 -- a local symbol), NOT \x80 (SID 0).
    // This is due to use of APIs that accept a string from the user.
    assertBytesEqual("\xD5\x8A\xE3\x81\x8A\x70", 6, result + result_len - 6, 6);
}

TEST(IonBinarySymbol, WriterWritesFieldNameSymbolZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, TRUE));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    // NOTE: symbol zero is NOT added to the local symbol table. Symbol zero is not present in ANY symbol table.
    assertBytesEqual("\xE0\x01\x00\xEA\xD5\x80\xE3\x81\x80\x70", 10, result, result_len);
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
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &actual));
    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_EQ(0, actual);

    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &symbol_table));
    ION_ASSERT_OK(ion_symbol_table_find_by_sid(symbol_table, 0, &symbol_value));
    ASSERT_TRUE(ION_STRING_IS_NULL(symbol_value));
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
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 2)); // This is a no-op.
    ION_ASSERT_OK(ion_writer_write_int(writer, 2));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertBytesEqual("\xE0\x01\x00\xEA\x20\x21\x01\x21\x02", 9, result, result_len);
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
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
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
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(2, sid);
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonBinarySymbol, ReaderReadsNullSymbol) {
    hREADER reader;
    BYTE *data = (BYTE *) "\xE0\x01\x00\xEA\x7F";
    BOOL is_null;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, 5, NULL));
    ASSERT_TRUE(ion_reader_is_null(reader, &is_null));
    ION_ASSERT_OK(ion_reader_close(reader));
}
