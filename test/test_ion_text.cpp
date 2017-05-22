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

TEST(IonTextSexp, ReaderHandlesNested)
{
    const char* ion_text = "((first)(second))((third)(fourth))";
    char *third, *fourth;

    hREADER reader;
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_TYPE type;
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_read_string_as_chars(reader, &third));
    ASSERT_STREQ("third", third);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_read_string_as_chars(reader, &fourth));
    ASSERT_STREQ("fourth", fourth);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextTimestamp, WriterIgnoresSuperfluousOffset) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_TIMESTAMP timestamp;

    ION_ASSERT_OK(ion_timestamp_for_year(&timestamp, 1));
    SET_FLAG_ON(timestamp.precision, ION_TT_BIT_TZ);
    timestamp.tz_offset = 1;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, FALSE));
    ION_ASSERT_OK(ion_writer_write_timestamp(writer, &timestamp));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertStringsEqual("0001T", (char *)result, result_len);
}

TEST(IonTextSymbol, WriterWritesSymbolValueZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING symbol_zero;

    ion_string_from_cstr("$0", &symbol_zero);

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, FALSE));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &symbol_zero));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertStringsEqual("$0\n'$0'", (char *)result, result_len);
}

TEST(IonTextSymbol, WriterWritesSymbolAnnotationZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING symbol_zero;

    ion_string_from_cstr("$0", &symbol_zero);

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, FALSE));

    ION_ASSERT_OK(ion_writer_add_annotation(writer, &symbol_zero));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));

    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &symbol_zero));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertStringsEqual("'$0'::$0\n$0::$0::'$0'", (char *)result, result_len);
}

TEST(IonTextSymbol, WriterWritesSymbolFieldNameZero) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING symbol_zero;

    ion_string_from_cstr("$0", &symbol_zero);

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, FALSE));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));

    ION_ASSERT_OK(ion_writer_write_field_name(writer, &symbol_zero));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &symbol_zero));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));

    ION_ASSERT_OK(ion_writer_write_field_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &symbol_zero));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));

    ION_ASSERT_OK(ion_writer_write_field_name(writer, &symbol_zero));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));

    ION_ASSERT_OK(ion_writer_write_field_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 0));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 0));

    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertStringsEqual("{'$0':'$0'::$0,$0:'$0'::$0,'$0':$0::$0,$0:$0::$0}", (char *)result, result_len);
}

TEST(IonTextSymbol, ReaderReadsSymbolValueSymbolZero) {
    const char *ion_text = "$0 '$0'";
    hREADER reader;
    ION_TYPE type;
    SID sid;
    ION_STRING symbol_value;
    char *symbol_value_str;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &symbol_value));
    ASSERT_TRUE(ION_STRING_IS_NULL(&symbol_value));
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(0, sid); // Because it was unquoted, this represents symbol zero.

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(10, sid); // This one just looks like symbol zero, but it's actually a user symbol with the text $0
    ION_ASSERT_OK(ion_read_string_as_chars(reader, &symbol_value_str));
    ASSERT_STREQ("$0", symbol_value_str);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, ReaderReadsAnnotationSymbolZero) {
    const char *ion_text = "'$0'::$0 $0::'$0'";
    hREADER reader;
    ION_TYPE type;
    SID symbol_value;
    SID annotations[1];
    ION_STRING annotation_strs[1];
    SIZE num_annotations;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_annotations(reader, annotation_strs, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_STREQ("$0", ion_string_strdup(&annotation_strs[0]));
    ION_ASSERT_OK(ion_reader_get_annotation_sids(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_EQ(10, annotations[0]); // This one just looks like symbol zero, but it's actually a user symbol with the text $0
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &symbol_value));
    ASSERT_EQ(0, symbol_value);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_annotation_sids(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_EQ(0, annotations[0]); // Because it was unquoted, this represents symbol zero.
    ION_ASSERT_OK(ion_reader_get_annotations(reader, annotation_strs, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_TRUE(ION_STRING_IS_NULL(&annotation_strs[0]));
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &symbol_value));
    ASSERT_EQ(10, symbol_value);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, ReaderReadsFieldNameSymbolZero) {
    const char *ion_text = "{'$0':'$0'::$0, $0:$0::'$0', $0:'$0'::$0}";
    hREADER reader;
    ION_TYPE type;
    SID symbol_value, field_name;
    ION_STRING field_name_str;
    SID annotations[1];
    SIZE num_annotations;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name_str));
    ASSERT_STREQ("$0", ion_string_strdup(&field_name_str));
    ION_ASSERT_OK(ion_reader_get_field_sid(reader, &field_name));
    ASSERT_EQ(10, field_name); // This one just looks like symbol zero, but it's actually a user symbol with the text $0
    ION_ASSERT_OK(ion_reader_get_annotation_sids(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_EQ(10, annotations[0]);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &symbol_value));
    ASSERT_EQ(0, symbol_value);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_sid(reader, &field_name));
    ASSERT_EQ(0, field_name); // Because it was unquoted, this represents symbol zero.
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name_str));
    ASSERT_TRUE(ION_STRING_IS_NULL(&field_name_str));
    ION_ASSERT_OK(ion_reader_get_annotation_sids(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_EQ(0, annotations[0]);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &symbol_value));
    ASSERT_EQ(10, symbol_value);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name_str));
    ASSERT_TRUE(ION_STRING_IS_NULL(&field_name_str));
    ION_ASSERT_OK(ion_reader_get_field_sid(reader, &field_name));
    ASSERT_EQ(0, field_name); // Because it was unquoted, this represents symbol zero.
    ION_ASSERT_OK(ion_reader_get_annotation_sids(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_EQ(10, annotations[0]);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &symbol_value));
    ASSERT_EQ(0, symbol_value);

    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_close(reader));
}

void ion_test_write_all_values(hREADER reader, BYTE **result, SIZE *result_len, BOOL to_binary) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, to_binary));

    ION_ASSERT_OK(ion_writer_write_all_values(writer, reader));

    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, result, result_len));
}

void ion_test_write_all_values_from_text(const char *ion_text, BYTE **result, SIZE *result_len, BOOL to_binary) {
    hREADER reader;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ion_test_write_all_values(reader, result, result_len, to_binary);
}

void ion_test_write_all_values_from_binary(BYTE *ion_data, SIZE ion_data_len, BYTE **result, SIZE *result_len, BOOL to_binary) {
    hREADER reader;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, ion_data, ion_data_len, NULL));
    ion_test_write_all_values(reader, result, result_len, to_binary);
}

TEST(IonTextSymbol, WriterWriteAllValuesPreservesSymbolZero) {
    const char *ion_text = "{'$0':'$0'::$0,$0:$0::'$0',$0:'$0'::$0}";
    const BYTE *ion_binary = (BYTE *)"\xDE\x90\x8A\xE3\x81\x8A\x70\x80\xE4\x81\x80\x71\x0A\x80\xE3\x81\x8A\x70";
    const SIZE ion_binary_size = 18;

    BYTE *result = NULL;
    SIZE result_len;

    ion_test_write_all_values_from_text(ion_text, &result, &result_len, FALSE);
    assertStringsEqual(ion_text, (char *)result, result_len);

    ion_test_write_all_values_from_text(ion_text, &result, &result_len, TRUE);
    assertBytesEqual((const char *)ion_binary, ion_binary_size, result + result_len - ion_binary_size, ion_binary_size);

    ion_test_write_all_values_from_binary(result, result_len, &result, &result_len, FALSE);
    assertStringsEqual(ion_text, (char *)result, result_len);

    ion_test_write_all_values_from_text(ion_text, &result, &result_len, TRUE);
    ion_test_write_all_values_from_binary(result, result_len, &result, &result_len, TRUE);
    assertBytesEqual((const char *)ion_binary, ion_binary_size, result + result_len - ion_binary_size, ion_binary_size);
}
