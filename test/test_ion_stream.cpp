/*
 * Copyright 2009-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

struct _test_in_memory_paged_stream_context {
    BYTE *data;
    int32_t data_len;
    int32_t page_size;
    int32_t number_of_handler_invocations;
};

iERR ion_test_input_stream_handler(_ion_user_stream *stream) {
    iENTER;
    _test_in_memory_paged_stream_context *context = (_test_in_memory_paged_stream_context *)stream->handler_state;
    context->number_of_handler_invocations++;
    if (!stream->curr) {
        stream->curr = context->data;
    }
    int32_t remaining = (int32_t)((context->data + context->data_len) - stream->curr);
    if (context->page_size < remaining) {
        stream->limit = stream->curr + context->page_size;
    }
    else {
        stream->limit = stream->curr + remaining;
    }
    iRETURN;
}

iERR ion_test_new_paged_input_stream(ION_STREAM **p_stream, _test_in_memory_paged_stream_context *context) {
    iENTER;
    ION_STREAM *stream = NULL;
    struct _ion_user_stream *user_stream = NULL;
    ION_STREAM_FLAG flags = ION_STREAM_USER_IN;

    if (!p_stream) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_stream_open_helper(flags, context->page_size, &stream));

    user_stream = &(((ION_STREAM_USER_PAGED *)stream)->_user_stream);

    user_stream->handler_state = context;
    user_stream->handler = &ion_test_input_stream_handler;

    IONCHECK(_ion_stream_fetch_position(stream, 0));

    *p_stream = stream;

    iRETURN;
}

TEST(IonStream, ContinuesOverPageBoundary) {
    ION_STREAM *stream = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_TIMESTAMP ts;
    ION_READER_OPTIONS options;
    ION_TIMESTAMP expected;
    const char *expected_str = "2000-08-07T00:00:00.015Z";
    SIZE chars_used;
    BOOL is_equal;

    _test_in_memory_paged_stream_context context;
    memset(&options, 0, sizeof(ION_READER_OPTIONS));
    memset(&context, 0, sizeof(_test_in_memory_paged_stream_context));
    // 2000-08-07T00:00:00.015Z
    // Byte length: 15
    BYTE *data = (BYTE *)"\xE0\x01\x00\xEA\x6A\x80\x0F\xD0\x88\x87\x80\x80\x80\xC3\x0F";
    context.data = data;
    context.data_len = 15;
    context.page_size = 14;
    // Page boundary just before the last byte, which represents the mantissa of the fractional seconds decimal.
    ION_ASSERT_OK(ion_test_new_paged_input_stream(&stream, &context));
    ION_ASSERT_OK(ion_reader_open(&reader, stream, &options));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_TIMESTAMP, type);
    ION_ASSERT_OK(ion_reader_read_timestamp(reader, &ts));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_stream_close(stream));

    ASSERT_EQ(3, context.number_of_handler_invocations);
    ION_ASSERT_OK(ion_timestamp_parse(&expected, (char *)expected_str, (SIZE)strlen(expected_str), &chars_used, &g_IonEventDecimalContext));
    ION_ASSERT_OK(ion_timestamp_equals(&expected, &ts, &is_equal, &g_IonEventDecimalContext));
    ASSERT_TRUE(is_equal);
}

TEST(IonStream, BufferTooSmall) {
    iENTER;
    hWRITER writer = NULL;
    uint8_t buf[2]; // This buffer is too small to hold the output.
    ION_WRITER_OPTIONS options = { 0 };
    options.output_as_binary = TRUE;
    ION_ASSERT_OK(ion_writer_open_buffer(&writer, buf, sizeof(buf), &options));
    ION_ASSERT_OK(ion_writer_write_int32(writer, 1));

    SIZE len;
    ION_ASSERT_FAIL(ion_writer_finish(writer, &len));
    ION_ASSERT_FAIL(ion_writer_close(writer));
}

iERR grow_buffer_if_necessary(_ion_user_stream *stream) {
    iENTER;
    _test_in_memory_paged_stream_context *context = (_test_in_memory_paged_stream_context *)stream->handler_state;
    context->data_len = stream->curr - context->data;
    if (stream->curr == stream->limit) {
        // The buffer is full and must grow.
        context->page_size *= 2;
        stream->curr = (BYTE *)malloc(context->page_size);
        stream->limit = stream->curr + context->page_size;
        memcpy(stream->curr, context->data, context->data_len);
        free(context->data);
        context->data = stream->curr;
        stream->curr += context->data_len;
    }
    iRETURN;
}

TEST(IonStream, WriteToUserStream) {
    // Write output to a user-controlled stream backed by a buffer. The user handler will grow the buffer
    // when necessary.

    hWRITER writer = NULL;
    SIZE image_length;
    SIZE initial_buffer_length = 1;
    ION_STREAM *stream = NULL;

    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(ION_WRITER_OPTIONS));
    options.output_as_binary = TRUE;

    _test_in_memory_paged_stream_context context;
    memset(&context, 0, sizeof(_test_in_memory_paged_stream_context));
    context.data = (BYTE *)(malloc(initial_buffer_length));
    context.page_size = initial_buffer_length;

    ION_ASSERT_OK(ion_stream_open_handler_out(&grow_buffer_if_necessary, &context, &stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &options));

    ION_STREAM_USER_PAGED *paged_stream = (ION_STREAM_USER_PAGED *)stream;
    paged_stream->_user_stream.curr = context.data;
    paged_stream->_user_stream.limit = context.data + initial_buffer_length;

    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_STRING fieldNameString;
    ion_string_assign_cstr(&fieldNameString, "str_col1", strlen("str_col1"));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &fieldNameString));

    ION_STRING value_string;
    ion_string_assign_cstr(&value_string, "str_val1", strlen("str_val1"));
    ION_ASSERT_OK(ion_writer_write_string(writer, &value_string));

    ion_string_assign_cstr(&fieldNameString, "str_col2", strlen("str_col2"));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &fieldNameString));

    ion_string_assign_cstr(&value_string, "str_val1", strlen("str_val1"));
    ION_ASSERT_OK(ion_writer_write_string(writer, &value_string));

    ion_string_assign_cstr(&fieldNameString, "str_col3", strlen("str_col3"));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &fieldNameString));

    ion_string_assign_cstr(&value_string, "str_val1", strlen("str_val1"));
    ION_ASSERT_OK(ion_writer_write_string(writer, &value_string));

    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_flush(writer, &image_length));
    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_stream_close(stream));

    ASSERT_EQ(image_length, context.data_len); // 72 bytes in this case.
    ASSERT_EQ(128, context.page_size); // The next power of 2 above 72 (we doubled the size on each grow).

    // Verify the output.

    hREADER reader = NULL;
    ION_TYPE type;
    ION_STRING stringValue;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, context.data, context.data_len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &fieldNameString));
    assertStringsEqual("str_col1", (char *)fieldNameString.value, fieldNameString.length);
    ION_ASSERT_OK(ion_reader_read_string(reader, &stringValue));
    assertStringsEqual("str_val1", (char *)stringValue.value, stringValue.length);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &fieldNameString));
    assertStringsEqual("str_col2", (char *)fieldNameString.value, fieldNameString.length);
    ION_ASSERT_OK(ion_reader_read_string(reader, &stringValue));
    assertStringsEqual("str_val1", (char *)stringValue.value, stringValue.length);

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &fieldNameString));
    assertStringsEqual("str_col3", (char *)fieldNameString.value, fieldNameString.length);
    ION_ASSERT_OK(ion_reader_read_string(reader, &stringValue));
    assertStringsEqual("str_val1", (char *)stringValue.value, stringValue.length);

    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    free(context.data);
}
