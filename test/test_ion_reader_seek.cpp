/*
 * Copyright 2009 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#include <gtest/gtest.h>
#include "ion_event_util.h"
#include <ionc/ion_types.h>
#include <ionc/ion_stream.h>
#include <ionc/ion_collection.h>
#include "ion_index.h"
#include "ion_stream_impl.h"
#include <ionc/ion.h>
#include "ion_helpers.h"
#include "ion_test_util.h"
#include "ion_assert.h"


class TextAndBinary : public ::testing::TestWithParam<bool> {
    virtual void SetUp() {
        is_binary = GetParam();
    }
public:
    BOOL is_binary;
};


INSTANTIATE_TEST_CASE_P(IonReaderSeek, TextAndBinary, ::testing::Bool());


TEST_P(TextAndBinary, SeekToTopLevelScalar) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, abc_read, def_read;
    int32_t int_written = 123, int_read;
    BYTE *data;
    SIZE data_length;
    POSITION abc_value_position, def_value_position;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &abc_written));
    ION_ASSERT_OK(ion_writer_write_string(writer, &def_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    // Record the desired value's position so it make be seeked to later.
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &abc_value_position));
    // Skip to the next value.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    // Seek back to the desired value without specifying an end.
    ION_ASSERT_OK(ion_reader_seek(reader, abc_value_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &def_value_position));
    ION_ASSERT_OK(ion_reader_seek(reader, 0, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    ION_ASSERT_OK(ion_reader_seek(reader, def_value_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}


TEST_P(TextAndBinary, SeekToTopLevelAnnotatedScalar) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, abc_read, def_read;
    ION_STRING annotation_written, annotation_read_on_123, annotation_read_on_abc, annotation_read_on_def;
    int32_t int_written = 123, int_read;
    BYTE *data;
    SIZE data_length;
    POSITION abc_value_position, def_value_position;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ion_string_from_cstr("str", &annotation_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &abc_written));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_write_string(writer, &def_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    // Record the desired value's position so it make be seeked to later.
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &abc_value_position));
    // Skip to the next value.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    // Seek back to the desired value without specifying an end.
    ION_ASSERT_OK(ion_reader_seek(reader, abc_value_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_abc));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_abc.value, annotation_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &def_value_position));
    ION_ASSERT_OK(ion_reader_seek(reader, 0, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_123));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_123.value, annotation_written.length);
    ION_ASSERT_OK(ion_reader_seek(reader, def_value_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_def));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_def.value, annotation_written.length);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}

TEST_P(TextAndBinary, SeekToTopLevelContainer) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, abc_read, def_read;
    int32_t int_written = 123, int_read;
    double float_written = 0., float_read;
    BYTE *data;
    SIZE data_length;
    POSITION first_struct_position, second_struct_position;
    BOOL is_in_struct;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &abc_written));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &def_written));
    ION_ASSERT_OK(ion_writer_write_double(writer, float_written));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &first_struct_position));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &second_struct_position));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, first_struct_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    ION_ASSERT_OK(ion_reader_is_in_struct(reader, &is_in_struct));
    ASSERT_TRUE(is_in_struct);
    // Seek without stepping out.
    ION_ASSERT_OK(ion_reader_seek(reader, second_struct_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_FLOAT, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_read_double(reader, &float_read));
    ASSERT_EQ(float_written, float_read);
    ION_ASSERT_OK(ion_reader_is_in_struct(reader, &is_in_struct));
    ASSERT_TRUE(is_in_struct);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}

TEST_P(TextAndBinary, SeekToTopLevelAnnotatedContainer) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, abc_read, def_read;
    ION_STRING annotation_written, annotation_read_on_first_struct, annotation_read_on_second_struct;
    int32_t int_written = 123, int_read;
    double float_written = 0., float_read;
    BYTE *data;
    SIZE data_length;
    POSITION first_struct_position, second_struct_position;
    BOOL is_in_struct;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ion_string_from_cstr("str", &annotation_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &abc_written));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &def_written));
    ION_ASSERT_OK(ion_writer_write_double(writer, float_written));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &first_struct_position));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &second_struct_position));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, first_struct_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_first_struct));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_first_struct.value, annotation_written.length);
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    ION_ASSERT_OK(ion_reader_is_in_struct(reader, &is_in_struct));
    ASSERT_TRUE(is_in_struct);
    // Seek without stepping out.
    ION_ASSERT_OK(ion_reader_seek(reader, second_struct_position, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_second_struct));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_second_struct.value, annotation_written.length);
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_FLOAT, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_read_double(reader, &float_read));
    ASSERT_EQ(float_written, float_read);
    ION_ASSERT_OK(ion_reader_is_in_struct(reader, &is_in_struct));
    ASSERT_TRUE(is_in_struct);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}

TEST_P(TextAndBinary, SeekAcrossSymbolTableBoundary) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, abc_read, def_read;
    int32_t int_written = 123, int_read;
    BYTE *data;
    SIZE data_length;
    POSITION abc_value_position, def_value_position;
    ION_SYMBOL_TABLE *abc_table, *def_table, *abc_table_clone, *def_table_clone;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &abc_written));
    // Forces a symbol table boundary.
    ION_ASSERT_OK(ion_writer_finish(writer, NULL));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &def_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    // Record the desired value's position so it make be seeked to later.
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &abc_value_position));
    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &abc_table));
    ION_ASSERT_OK(ion_symbol_table_clone_with_owner(abc_table, &abc_table_clone, reader));
    // Skip to the next value.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    // Seek back to the desired value without specifying an end.
    ION_ASSERT_OK(ion_reader_seek(reader, abc_value_position, -1));
    ION_ASSERT_OK(ion_reader_set_symbol_table(reader, abc_table_clone));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &def_value_position));
    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &def_table));
    ION_ASSERT_OK(ion_symbol_table_clone_with_owner(def_table, &def_table_clone, reader));
    ION_ASSERT_OK(ion_reader_seek(reader, 0, -1));
    // This int value doesn't require a symbol table.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    ION_ASSERT_OK(ion_reader_seek(reader, def_value_position, -1));
    ION_ASSERT_OK(ion_reader_set_symbol_table(reader, def_table_clone));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}

TEST_P(TextAndBinary, SeekWithLimitsUsingGetValueLength) {
    if (!is_binary) {
        // TODO this test is skipped for text because the text reader does not currently provide a useful value
        // from ion_reader_get_value_length. Text readers always provide -1 from this method, which imposes no
        // limit on the amount of data consumed after ion_reader_seek.
        return;
    }
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, abc_read;
    ION_STRING annotation_written, annotation_read_on_abc;
    int32_t int_written = 123, int_read;
    BYTE *data;
    SIZE data_length;
    POSITION int_value_position, list_value_position, abc_value_position;
    SIZE int_value_length, list_value_length, abc_value_length;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("str", &annotation_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_int32(writer, 12345678));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_write_string(writer, &abc_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &int_value_position));
    ION_ASSERT_OK(ion_reader_get_value_length(reader, &int_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &list_value_position));
    ION_ASSERT_OK(ion_reader_get_value_length(reader, &list_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &abc_value_position));
    ION_ASSERT_OK(ion_reader_get_value_length(reader, &abc_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, list_value_position, list_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_LIST, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    // The limit has been reached.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, int_value_position, int_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    // The limit has been reached.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, abc_value_position, abc_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_abc));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_abc.value, annotation_written.length);
    ION_ASSERT_OK(ion_reader_read_string(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    // The limit has been reached.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}

TEST_P(TextAndBinary, SeekWithLimitsUsingLengthCalculatedFromPosition) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, abc_read;
    ION_STRING annotation_written, annotation_read_on_abc;
    int32_t int_written = 123, int_read;
    BYTE *data;
    SIZE data_length;
    POSITION int_value_position, list_value_position, abc_value_position, end_position;
    SIZE int_value_length, list_value_length, abc_value_length;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("str", &annotation_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, int_written));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_int32(writer, 12345678));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &annotation_written));
    ION_ASSERT_OK(ion_writer_write_string(writer, &abc_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &int_value_position));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &list_value_position));
    int_value_length = list_value_position - int_value_position;
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &abc_value_position));
    list_value_length = abc_value_position - list_value_position;
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &end_position));
    abc_value_length = end_position - abc_value_position;
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, list_value_position, list_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_LIST, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_step_out(reader));
    // The limit has been reached.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, int_value_position, int_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_read_int32(reader, &int_read));
    ASSERT_EQ(int_written, int_read);
    // The limit has been reached.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_seek(reader, abc_value_position, abc_value_length));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annotation_read_on_abc));
    assertStringsEqual((char *)annotation_written.value, (char *)annotation_read_on_abc.value, annotation_written.length);
    ION_ASSERT_OK(ion_reader_read_string(reader, &abc_read));
    assertStringsEqual((char *)abc_written.value, (char *)abc_read.value, abc_written.length);
    // The limit has been reached.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(data);
}

TEST_P(TextAndBinary, ReaderHandlesContainerValueOffsetSeek) {
    // Write!

    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    ION_STRING first, second;
    BYTE *data;
    SIZE data_length;

    // 42 (first second)
    ion_string_from_cstr("first", &first);
    ion_string_from_cstr("second", &second);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, 42));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_SEXP));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &first));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &second));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    // Read!

    hREADER reader = NULL;
    ION_TYPE type;
    POSITION pos_init, pos_sexp, pos_first, pos_second;

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));

    // Assemble: Take one pass through the document to capture value offsets

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_init));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_sexp));

    // Enter sexp
    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_first));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_second));

    // Leave sexp
    ION_ASSERT_OK(ion_reader_step_out(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_close(reader));

    // Act: Position the reader so we can seek to captured offsets

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);

    // Assert:

    // Initial value
    ION_ASSERT_OK(ion_reader_seek(reader, pos_init, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);

    // Sexp container
    ION_ASSERT_OK(ion_reader_seek(reader, pos_sexp, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);

    // First sexp element

    ION_ASSERT_OK(ion_reader_seek(reader, pos_first, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    // Second sexp element
    ION_ASSERT_OK(ion_reader_seek(reader, pos_second, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
}

TEST_P(TextAndBinary, ReaderHandlesInitialUnannotatedContainerValueOffsetSeek) {
    // Write!

    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    ION_STRING first, second;
    BYTE *data;
    SIZE data_length;

    // (first second)
    ion_string_from_cstr("first", &first);
    ion_string_from_cstr("second", &second);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_SEXP));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &first));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &second));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    // Read!

    hREADER reader = NULL;
    ION_TYPE type;
    POSITION pos_sexp, pos_first, pos_second;

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));

    // Assemble: Take one pass through the document to capture value offsets

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_sexp));

    // Enter sexp
    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_first));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_second));

    // Leave sexp
    ION_ASSERT_OK(ion_reader_step_out(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    ION_ASSERT_OK(ion_reader_close(reader));

    // Act: Position the reader so we can seek to captured offsets

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);

    // Assert:

    // Sexp container
    ION_ASSERT_OK(ion_reader_seek(reader, pos_sexp, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);

    // First sexp element
    ION_ASSERT_OK(ion_reader_seek(reader, pos_first, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    // Second sexp element
    ION_ASSERT_OK(ion_reader_seek(reader, pos_second, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
}

// What's going on here? If I write a struct {field1:value,field2:value}, then run through to grab the offsets of each
// field, save them for later, then re-seek to them and hydrate, what I expect is:
//
// field1: value
// field2: value
//
// but with the text reader, what I end up with is:
//
// field1: value
// field2: field2
//
// This currently works correctly in the binary reader.
TEST_P(TextAndBinary, ReaderPopulatesStructFieldsOnSeek) {
    // Write!

    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    ION_STRING field1, field2, value;
    BYTE *data;
    SIZE data_length;

    // {field1:value,field2:value}
    ion_string_from_cstr("field1", &field1);
    ion_string_from_cstr("field2", &field2);
    ion_string_from_cstr("value", &value);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &field1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &value));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &field2));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &value));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    // Read!

    hREADER reader = NULL;
    ION_TYPE type;
    POSITION pos_field1, pos_field2;
    ION_STRING read_field1, read_val1, read_field2, read_val2;

    // We use this reader to capture offsets
    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));

    // Assemble: Take one pass through the document to capture value offsets and field names

    ION_ASSERT_OK(ion_reader_next(reader, &type));

    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    // Real code would probably capture these, but it doesn't affect the issue
    //ION_ASSERT_OK(ion_reader_get_field_name(reader, &read_field1));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_field1));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    // Real code would probably capture these, but it doesn't affect the issue
    //ION_ASSERT_OK(ion_reader_get_field_name(reader, &read_field2));
    ION_ASSERT_OK(ion_reader_get_value_offset(reader, &pos_field2));

    ION_ASSERT_OK(ion_reader_step_out(reader));

    // Act: Move back to the original offsets and then re-assemble values

    // Seek to first field
    ION_ASSERT_OK(ion_reader_seek(reader, pos_field1, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    // Read field value
    ION_ASSERT_OK(ion_reader_read_string(reader, &read_val1));

    // Seek to second field
    ION_ASSERT_OK(ion_reader_seek(reader, pos_field2, -1));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    // Read field value
    ION_ASSERT_OK(ion_reader_read_string(reader, &read_val2));

    ION_ASSERT_OK(ion_reader_close(reader));

    // Assert:

    // Easy assertions: there's only one value, "value," and we should have read it both times
    assertStringsEqual((char *)value.value, (char *)read_val1.value, read_val1.length);
    assertStringsEqual((char *)value.value, (char *)read_val2.value, read_val2.length);
}