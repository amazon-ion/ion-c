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

TEST(IonBinaryTimestampStoredInUTC, WriterConvertsToUTC) {
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

TEST(IonBinaryTimestampStoredInUTC, ReaderConvertsFromUTC) {
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
