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
    ION_SYMBOL symbol_value;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(_ion_reader_read_symbol_helper(reader, &symbol_value));
    ASSERT_TRUE(ION_STRING_IS_NULL(&symbol_value.value));
    ASSERT_EQ(0, symbol_value.sid); // Because it was unquoted, this represents symbol zero.

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(_ion_reader_read_symbol_helper(reader, &symbol_value));
    ASSERT_STREQ("$0", ionStringToString(&symbol_value.value)); // This one just looks like symbol zero, but it's actually a user symbol with the text $0

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
    ION_SYMBOL symbol;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_annotations(reader, annotation_strs, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_STREQ("$0", ion_string_strdup(&annotation_strs[0]));
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
    ION_ASSERT_OK(_ion_reader_read_symbol_helper(reader, &symbol));
    ASSERT_STREQ("$0", ionStringToString(&symbol.value)); // This one just looks like symbol zero, but it's actually a user symbol with the text $0

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, ReaderReadsFieldNameSymbolZero) {
    const char *ion_text = "{'$0':'$0'::$0, $0:$0::'$0', $0:'$0'::$0}";
    hREADER reader;
    ION_TYPE type;
    SID symbol_value, field_name;
    ION_STRING field_name_str;
    SID annotation_sids[1];
    ION_STRING annotations[1];
    SIZE num_annotations;
    ION_STRING symbol_text;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name_str));
    ASSERT_STREQ("$0", ion_string_strdup(&field_name_str)); // This one just looks like symbol zero, but it's actually a user symbol with the text $0
    ION_ASSERT_OK(ion_reader_get_annotations(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_STREQ("$0", ion_string_strdup(&annotations[0]));
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &symbol_value));
    ASSERT_EQ(0, symbol_value);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_sid(reader, &field_name));
    ASSERT_EQ(0, field_name); // Because it was unquoted, this represents symbol zero.
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name_str));
    ASSERT_TRUE(ION_STRING_IS_NULL(&field_name_str));
    ION_ASSERT_OK(ion_reader_get_annotation_sids(reader, annotation_sids, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_EQ(0, annotation_sids[0]);
    ION_ASSERT_OK(ion_reader_read_string(reader, &symbol_text));
    ASSERT_STREQ("$0", ion_string_strdup(&symbol_text));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name_str));
    ASSERT_TRUE(ION_STRING_IS_NULL(&field_name_str));
    ION_ASSERT_OK(ion_reader_get_field_sid(reader, &field_name));
    ASSERT_EQ(0, field_name); // Because it was unquoted, this represents symbol zero.
    ION_ASSERT_OK(ion_reader_get_annotations(reader, annotations, 1, &num_annotations));
    ASSERT_EQ(1, num_annotations);
    ASSERT_STREQ("$0", ion_string_strdup(&annotations[0]));
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

TEST(IonTextSymbol, WriterWritesSymbolValueIVMTextAsNoOp) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING ivm_text;

    ION_ASSERT_OK(ion_string_from_cstr("$ion_1_0", &ivm_text));
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, FALSE));

    ION_ASSERT_OK(ion_writer_write_int(writer, 123));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &ivm_text)); // This is a no-op.
    ION_ASSERT_OK(ion_writer_write_int(writer, 456));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 2)); // This is a no-op.
    ION_ASSERT_OK(ion_writer_write_int(writer, 789));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertStringsEqual("123\n456\n789", (char *)result, result_len);
}

TEST(IonTextSymbol, ReaderReadsSymbolValueIVM) {
    // Asserts that '$ion_1_0' is not treated as an IVM or as a symbol value. If it were treated as an IVM, $10 would
    // error for being out of range of the symbol table context. If it were treated as a symbol value, the call to
    // ion_reader_read_string would return $ion_1_0, not foo.
    const char *ion_text = "$ion_symbol_table::{symbols:[\"foo\"]} '$ion_1_0' $10";
    hREADER reader;
    ION_TYPE type;
    ION_STRING result;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &result));
    assertStringsEqual("foo", (char *)result.value, result.length);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, ReaderReadsSymbolValueTrueIVM) {
    // Asserts that $ion_1_0 is treated as an IVM -- it resets the symbol table context. Reading $10 fails because
    // it is out of range.
    const char *ion_text = "$ion_symbol_table::{symbols:[\"foo\"]} $ion_1_0 $10";
    hREADER reader;
    ION_TYPE type;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_reader_next(reader, &type));

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, ReaderReadsSymbolValueAnnotatedIVM) {
    // Asserts that annotated::$ion_1_0 is not treated as an IVM, but is treated as a symbol value. If it were treated
    // as an IVM, $10 would error for being out of range of the symbol table context.
    const char *ion_text = "$ion_symbol_table::{symbols:[\"foo\"]} annotated::$ion_1_0 $10";
    hREADER reader;
    ION_TYPE type;
    SID sid;
    SIZE annot_count;
    ION_STRING annot, result;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(2, sid);
    ION_ASSERT_OK(ion_reader_get_annotation_count(reader, &annot_count));
    ASSERT_EQ(1, annot_count);
    ION_ASSERT_OK(ion_reader_get_an_annotation(reader, 0, &annot));
    assertStringsEqual("annotated", (char *)annot.value, annot.length);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &result));
    assertStringsEqual("foo", (char *)result.value, result.length);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, ReaderReadsSymbolValueIVMFromSID) {
    // Asserts that $2 is not treated as an IVM or as a symbol value. If it were treated as an IVM, $10 would error
    // for being out of range of the symbol table context. If it were treated as a symbol value, the call to
    // ion_reader_read_string would return $ion_1_0, not foo.
    const char *ion_text = "$ion_symbol_table::{symbols:[\"foo\"]} $2 $10";
    hREADER reader;
    ION_TYPE type;
    ION_STRING result;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &result));
    assertStringsEqual("foo", (char *)result.value, result.length);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, WriterWritesKeywordsAsQuotedSymbols) {
    hWRITER writer = NULL;
    ION_STREAM *ion_stream = NULL;
    BYTE *result;
    SIZE result_len;
    ION_STRING str_false, str_true, str_nan;
    ion_string_from_cstr("false", &str_false);
    ion_string_from_cstr("true", &str_true);
    ion_string_from_cstr("nan", &str_nan);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, FALSE));

    ION_ASSERT_OK(ion_writer_write_symbol(writer, &str_false));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &str_true));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &str_nan));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &result, &result_len));

    assertStringsEqual("'false'\n'true'\n'nan'", (char *)result, result_len);
}

TEST(IonTextSymbol, ReaderChoosesLowestSIDForDuplicateSymbol) {
    // Asserts that the reader and symbol table work together to make sure by-name symbol queries return the lowest
    // possible SID for symbols declared multiple times. At the same time, all by-ID queries should return the correct
    // text.
    const char *ion_text = "$ion_symbol_table::{symbols:[\"name\"]} name $10";
    hREADER reader;
    ION_TYPE type;
    ION_STRING result, *lookup;
    SID sid;
    ION_SYMBOL_TABLE *symbol_table;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(4, sid); // SID 4 maps to "name" in the system symbol table.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &result));
    assertStringsEqual("name", (char *)result.value, result.length);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ASSERT_EQ(10, sid); // SID 10 was explicitly declared.

    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &symbol_table));
    ION_ASSERT_OK(ion_symbol_table_find_by_name(symbol_table, &result, &sid));
    ASSERT_EQ(4, sid);
    ION_ASSERT_OK(ion_symbol_table_find_by_sid(symbol_table, 4, &lookup));
    assertStringsEqual("name", (char *)lookup->value, lookup->length);
    ION_ASSERT_OK(ion_symbol_table_find_by_sid(symbol_table, 10, &lookup));
    assertStringsEqual("name", (char *)lookup->value, lookup->length);

    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonTextSymbol, WriterWriteAllValuesPreservesSymbolKeywords) {
    const char *ion_text = "{'false':'true'::'nan'}";
    const BYTE *ion_binary = (BYTE *)"\xD6\x8B\xE4\x81\x8C\x71\x0A";
    const SIZE ion_binary_size = 7;

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

TEST(IonTextSymbol, ReaderReadsUndefinedSymbol) {
    const char *ion_text =
            "$ion_symbol_table::\n"
                    "{\n"
                    "  imports:[ { name: \"not_found\",\n"
                    "              version: 1,\n"
                    "              max_id: 75 },\n"
                    "  ],\n"
                    "  symbols:[ \"rock\", \"paper\", \"scissors\" ]\n"
                    "}\n"
                    "$53\n"
                    "$85";
    // Symbol 53 is a dummy symbol (unknown text), but is valid because it is within the max ID range of the
    // not-found import. Symbol 85 is the first local symbol ("rock").
    // 9 (+1 for SID 0) = 10 system symbols + 75 imports = 85.
    hREADER reader;
    SID sid;
    hSYMTAB symtab;
    ION_STRING *symbol;
    ION_TYPE type;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &symtab));
    ION_ASSERT_OK(ion_symbol_table_find_by_sid(symtab, sid, &symbol));
    ASSERT_TRUE(ION_STRING_IS_NULL(symbol));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, symbol));
    assertStringsEqual("rock", (char *)symbol->value, symbol->length);
    ION_ASSERT_OK(ion_reader_read_symbol_sid(reader, &sid));
    ION_ASSERT_OK(ion_symbol_table_find_by_sid(symtab, sid, &symbol));
    assertStringsEqual("rock", (char *)symbol->value, symbol->length);

    ION_ASSERT_OK(ion_reader_close(reader));

}

TEST(IonTextSymbol, ReaderReadsLocalSymbolsFromIdentifiers) {
    const char * ion_text =
            "$ion_1_0\n"
            "$ion_symbol_table::{\n"
            "    symbols:[ \"foo\", \"bar\", \"baz\" ]\n"
            "}\n"
            "$10\n"
            "$11\n"
            "$12";
    hREADER reader;
    ION_STRING symbol;
    ION_TYPE type;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &symbol));
    assertStringsEqual("foo", (char *)symbol.value, symbol.length);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &symbol));
    assertStringsEqual("bar", (char *)symbol.value, symbol.length);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &symbol));
    assertStringsEqual("baz", (char *)symbol.value, symbol.length);

    ION_ASSERT_OK(ion_reader_close(reader));
}
