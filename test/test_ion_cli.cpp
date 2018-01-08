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

#include <gtest/gtest.h>
#include <ion_event_stream.h>
#include "cli.h"
#include "ion_test_util.h"
#include "gather_vectors.h"
#include "ion_event_util.h"

void test_ion_cli_assert_error_equals(ION_EVENT_ERROR_DESCRIPTION *actual, ION_EVENT_ERROR_TYPE expected_type, iERR expected_code, std::string expected_location_suffix="", int expected_event_index=-1) {
    ASSERT_EQ(expected_type, actual->error_type);
    if (expected_event_index >= 0) {
        ASSERT_TRUE(actual->has_event_index);
        ASSERT_EQ((size_t)expected_event_index, actual->event_index);
    }
    if (!expected_location_suffix.empty()) {
        ASSERT_TRUE(actual->has_location);
        ASSERT_EQ(expected_location_suffix, actual->location.substr(actual->location.size() - expected_location_suffix.length()));
    }
    ASSERT_NE(actual->message.find(ion_error_to_str(expected_code)), actual->message.npos);
}

void test_ion_cli_assert_comparison_result_equals(ION_EVENT_COMPARISON_RESULT *actual, ION_EVENT_COMPARISON_RESULT_TYPE expected_type, std::string expected_lhs_location, std::string expected_rhs_location, size_t expected_lhs_index, size_t expected_rhs_index) {
    ASSERT_EQ(expected_type, actual->result);
    ASSERT_NE(actual->lhs.location.npos, actual->lhs.location.find(expected_lhs_location)) << actual->lhs.location;
    ASSERT_NE(actual->rhs.location.npos, actual->rhs.location.find(expected_rhs_location)) << actual->rhs.location;
    ASSERT_EQ(expected_lhs_index, actual->lhs.event_index);
    ASSERT_EQ(expected_rhs_index, actual->rhs.event_index);
}

void test_ion_cli_process_events(std::string filepath, ION_WRITER_OUTPUT_TYPE output_type, ION_CATALOG *catalog, BYTE **output, SIZE *output_len, IonEventResult *result) {
    ION_EVENT_WRITER_CONTEXT writer_context;
    ION_CLI_COMMON_ARGS common_args;
    memset(&common_args, 0, sizeof(ION_CLI_COMMON_ARGS));
    common_args.input_files.push_back(filepath);
    common_args.output_format = "events";
    ION_ASSERT_OK(ion_event_in_memory_writer_open(&writer_context, filepath, output_type, catalog, NULL, NULL));
    ION_ASSERT_OK(ion_writer_write_symbol(writer_context.writer, &ion_cli_event_stream_symbol));
    ion_cli_command_process_standard(&writer_context, &common_args, catalog, result);
    ION_ASSERT_OK(ion_event_in_memory_writer_close(&writer_context, output, output_len));
}

void test_ion_cli_process_standard(std::string filepath, ION_WRITER_OUTPUT_TYPE output_type, ION_CATALOG *catalog, BYTE **output, SIZE *output_len, IonEventResult *result) {
    ION_EVENT_WRITER_CONTEXT writer_context;
    ION_CLI_COMMON_ARGS common_args;
    memset(&common_args, 0, sizeof(ION_CLI_COMMON_ARGS));
    common_args.input_files.push_back(filepath);
    ION_ASSERT_OK(ion_event_in_memory_writer_open(&writer_context, filepath, output_type, catalog, NULL, NULL));
    ion_cli_command_process_standard(&writer_context, &common_args, catalog, result);
    ION_ASSERT_OK(ion_event_in_memory_writer_close(&writer_context, output, output_len));
}

void test_ion_cli_read_stream_successfully(BYTE *data, size_t data_len, IonEventStream *stream) {
    ION_CLI_READER_CONTEXT reader_context;
    memset(&reader_context, 0, sizeof(ION_CLI_READER_CONTEXT));
    IonEventResult result;
    reader_context.event_stream = stream;
    ION_ASSERT_OK(ion_test_new_reader(data, (SIZE)data_len, &reader_context.reader));
    ION_ASSERT_OK(ion_cli_read_stream(&reader_context, NULL, stream, &result));
    ION_ASSERT_OK(ion_reader_close(reader_context.reader));
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
}

void test_ion_cli_assert_streams_equal(const char *expected_str, BYTE *actual_bytes, SIZE actual_bytes_len) {
    IonEventStream expected("expected"), actual("actual");
    IonEventResult result;

    test_ion_cli_read_stream_successfully((BYTE *)expected_str, strlen(expected_str), &expected);
    test_ion_cli_read_stream_successfully(actual_bytes, (size_t)actual_bytes_len, &actual);

    ASSERT_TRUE(assertIonEventStreamEq(&expected, &actual, &result));
    ASSERT_FALSE(result.has_comparison_result);
    ASSERT_FALSE(result.has_error_description);
}

TEST(IonCli, ProcessBasic) {
    BYTE *output = NULL;
    SIZE len;
    IonEventResult result;
    test_ion_cli_process_events(join_path(full_good_path, "one.ion"), ION_WRITER_OUTPUT_TYPE_TEXT_UGLY, NULL, &output, &len, &result);
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
    test_ion_cli_assert_streams_equal("1", output, len);
    test_ion_cli_assert_streams_equal("$ion_event_stream {event_type:SCALAR, ion_type:INT, value_text:\"1\", value_binary:[0xE0, 0x01, 0x00, 0xEA, 0x21, 0x01], depth: 0} {event_type: STREAM_END, depth: 0}", output, len);
    free(output);
}

TEST(IonCli, UnequalValueTextAndValueBinaryFails) {
    // Text value is integer 1, binary value is integer 2
    const char *test_name = "unequal text and binary";
    const char *event_stream = "$ion_event_stream {event_type:SCALAR, ion_type:INT, value_text:\"1\", value_binary:[0xE0, 0x01, 0x00, 0xEA, 0x21, 0x02], depth: 0} {event_type: STREAM_END, depth: 0}";
    IonEventResult result;
    ION_CLI_READER_CONTEXT reader;
    memset(&reader, 0, sizeof(ION_CLI_READER_CONTEXT));
    IonEventStream stream(test_name);
    reader.event_stream = &stream;
    ION_ASSERT_OK(ion_test_new_text_reader(event_stream, &reader.reader));
    ION_ASSERT_FAIL(ion_cli_read_stream(&reader, NULL, &stream, &result));
    ION_ASSERT_OK(ion_reader_close(reader.reader));
    ASSERT_TRUE(result.has_error_description);
    test_ion_cli_assert_error_equals(&result.error_description, ERROR_TYPE_STATE, IERR_INVALID_ARG, test_name);
    ASSERT_TRUE(result.has_comparison_result);
    test_ion_cli_assert_comparison_result_equals(&result.comparison_result, COMPARISON_RESULT_NOT_EQUAL, test_name, test_name, 0, 0);
}

void test_ion_cli_assert_comparison(std::string *files, size_t num_files, COMPARISON_TYPE comparison_type, IonEventReport *report) {
    ION_CLI_COMMON_ARGS common_args;
    memset(&common_args, 0, sizeof(ION_CLI_COMMON_ARGS));
    for (size_t i = 0; i < num_files; i++) {
        common_args.input_files.push_back(files[i]);
    }
    ION_ASSERT_OK(ion_cli_command_compare_standard(&common_args, comparison_type, /*catalog=*/NULL, report, NULL));
}

TEST(IonCli, CompareNullsBasic) {
    std::string test_file = join_path(full_good_path, "allNulls.ion");
    IonEventReport report;
    test_ion_cli_assert_comparison(&test_file, 1, COMPARISON_TYPE_BASIC, &report);
    ASSERT_FALSE(report.hasErrors());
    ASSERT_FALSE(report.hasComparisonFailures());
}

TEST(IonCli, CompareListsEquivs) {
    std::string test_file = join_path(full_good_equivs_path, "lists.ion");
    IonEventReport report;
    test_ion_cli_assert_comparison(&test_file, 1, COMPARISON_TYPE_EQUIVS, &report);
    ASSERT_FALSE(report.hasErrors());
    ASSERT_FALSE(report.hasComparisonFailures());
}

TEST(IonCli, CompareSexpsNonequivs) {
    std::string test_file = join_path(full_good_nonequivs_path, "sexps.ion");
    IonEventReport report;
    test_ion_cli_assert_comparison(&test_file, 1, COMPARISON_TYPE_NONEQUIVS, &report);
    ASSERT_FALSE(report.hasErrors());
    ASSERT_FALSE(report.hasComparisonFailures());
}

TEST(IonCli, CompareAnnotatedIvmsEmbeddedNonequivs) {
    std::string test_file = join_path(full_good_nonequivs_path, "annotatedIvms.ion");
    IonEventReport report;
    // NOTE: this is a non-equivs file being compared for equivs; comparison failures are expected.
    test_ion_cli_assert_comparison(&test_file, 1, COMPARISON_TYPE_EQUIVS, &report);
    ASSERT_FALSE(report.hasErrors());
    ASSERT_TRUE(report.hasComparisonFailures());
    std::vector<ION_EVENT_COMPARISON_RESULT> *comparison_results = report.getComparisonResults();
    ASSERT_EQ(1, comparison_results->size());
    test_ion_cli_assert_comparison_result_equals(&comparison_results->at(0), COMPARISON_RESULT_NOT_EQUAL, test_file, test_file, 1, 3);
}

TEST(IonCli, CompareMultipleInputFiles) {
    std::string test_files[3];
    test_files[0] = join_path(full_good_path, "one.ion");
    test_files[1] = join_path(full_good_path, "one.ion");
    test_files[2] = join_path(full_good_path, "empty.ion");
    IonEventReport report;
    test_ion_cli_assert_comparison(test_files, 3, COMPARISON_TYPE_BASIC, &report);
    ASSERT_FALSE(report.hasErrors());
    ASSERT_TRUE(report.hasComparisonFailures());
    std::vector<ION_EVENT_COMPARISON_RESULT> *comparison_results = report.getComparisonResults();
    ASSERT_EQ(4, comparison_results->size()); // one vs empty, one vs empty, empty vs one, empty vs one
    test_ion_cli_assert_comparison_result_equals(&comparison_results->at(0), COMPARISON_RESULT_NOT_EQUAL, test_files[0], test_files[2], 0, 0);
    test_ion_cli_assert_comparison_result_equals(&comparison_results->at(1), COMPARISON_RESULT_NOT_EQUAL, test_files[1], test_files[2], 0, 0);
    test_ion_cli_assert_comparison_result_equals(&comparison_results->at(2), COMPARISON_RESULT_NOT_EQUAL, test_files[2], test_files[0], 0, 0);
    test_ion_cli_assert_comparison_result_equals(&comparison_results->at(3), COMPARISON_RESULT_NOT_EQUAL, test_files[2], test_files[1], 0, 0);
}

TEST(IonCli, ErrorIsConveyed) {
    std::string test_file = join_path(full_bad_path, "annotationFalse.ion");
    BYTE *output = NULL;
    SIZE len;
    IonEventResult result;
    test_ion_cli_process_standard(test_file, ION_WRITER_OUTPUT_TYPE_TEXT_UGLY, NULL, &output, &len, &result);
    ASSERT_TRUE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
    test_ion_cli_assert_error_equals(&result.error_description, ERROR_TYPE_READ, IERR_INVALID_SYNTAX, test_file);
    free(output);
}

TEST(IonCli, ErrorIsConveyedEvents) {
    std::string test_file = join_path(full_bad_path, "fieldNameFalse.ion");
    BYTE *output = NULL;
    SIZE len;
    IonEventResult result;
    test_ion_cli_process_events(test_file, ION_WRITER_OUTPUT_TYPE_TEXT_UGLY, NULL, &output, &len, &result);
    ASSERT_TRUE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
    test_ion_cli_assert_error_equals(&result.error_description, ERROR_TYPE_READ, IERR_INVALID_FIELDNAME, test_file);
    test_ion_cli_assert_streams_equal("$ion_event_stream {event_type:CONTAINER_START, ion_type:STRUCT, depth: 0}", output, len);
    free(output);
}

TEST(IonCli, AnnotatedIvmsEmbedded) {
    std::string test_file = join_path(full_good_nonequivs_path, "annotatedIvms.ion");
    ION_CLI_READER_CONTEXT reader_context;
    BYTE *output = NULL;
    SIZE len;
    IonEventResult result;
    IonEventStream stream(test_file);
    test_ion_cli_process_events(test_file, ION_WRITER_OUTPUT_TYPE_TEXT_UGLY, NULL, &output, &len, &result);
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
    ASSERT_TRUE(output != NULL);
    test_ion_cli_read_stream_successfully(output, (size_t)len, &stream);
    ASSERT_EQ(CONTAINER_START, stream.at(0)->event_type);
    ASSERT_EQ(0, stream.at(0)->depth);
    ASSERT_EQ(SCALAR, stream.at(1)->event_type);
    ASSERT_EQ(tid_SYMBOL, stream.at(1)->ion_type);
    ASSERT_EQ(1, stream.at(1)->num_annotations);
    ASSERT_EQ(0, stream.at(1)->depth);
    ASSERT_EQ(STREAM_END, stream.at(2)->event_type);
    ASSERT_EQ(STREAM_END, stream.at(3)->event_type);
    ASSERT_EQ(CONTAINER_END, stream.at(4)->event_type);
    ASSERT_EQ(0, stream.at(4)->depth);
    free(output);
}

// TODO consider having a list of (location, index) in error report to better convey errors during comparison
// TODO test piping output of one command through stdin to another.

// TODO process command with catalog and/or imports
// TODO compare command with catalog and/or imports

TEST(IonCli, ComparisonSetsDifferentLengthsEquivsSucceeds) {
    const char *lhs_data = "(1 0x1 0b1)";
    const char *rhs_data = "(0x1 1)";
    IonEventResult result;
    IonEventStream lhs(lhs_data), rhs(rhs_data);
    test_ion_cli_read_stream_successfully((BYTE *)lhs_data, strlen(lhs_data), &lhs);
    test_ion_cli_read_stream_successfully((BYTE *)rhs_data, strlen(rhs_data), &rhs);
    ion_cli_command_compare_streams(COMPARISON_TYPE_EQUIVS, &lhs, &rhs, &result);
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
    ion_cli_command_compare_streams(COMPARISON_TYPE_EQUIVS, &rhs, &lhs, &result);
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
}

TEST(IonCli, ComparisonSetsDifferentLengthsNonequivsSucceeds) {
    const char *lhs_data = "(1)";
    const char *rhs_data = "(2 3)";
    IonEventResult result;
    IonEventStream lhs(lhs_data), rhs(rhs_data);
    test_ion_cli_read_stream_successfully((BYTE *)lhs_data, strlen(lhs_data), &lhs);
    test_ion_cli_read_stream_successfully((BYTE *)rhs_data, strlen(rhs_data), &rhs);
    ion_cli_command_compare_streams(COMPARISON_TYPE_NONEQUIVS, &lhs, &rhs, &result);
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
    ion_cli_command_compare_streams(COMPARISON_TYPE_NONEQUIVS, &rhs, &lhs, &result);
    ASSERT_FALSE(result.has_error_description);
    ASSERT_FALSE(result.has_comparison_result);
}

TEST(IonCli, BasicProcessWithCatalog) {
    const char *foo = "$ion_shared_symbol_table::{name:\"foo\", version:1, symbols[\"abc\"]}";
    const char *data = "$ion_symbol_table::{symbols:[\"def\"], imports[{name:\"foo\", version:1, max_id:1}]} $10::$11";
    // TODO
}
