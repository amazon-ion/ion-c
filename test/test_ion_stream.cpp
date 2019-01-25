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
#include <ion_event_util.h>
#include "ion_types.h"
#include "ion_stream.h"
#include "ion_collection.h"
#include "ion_index.h"
#include "ion_stream_impl.h"
#include "ion.h"
#include "ion_helpers.h"
#include "ion_test_util.h"

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

    ASSERT_EQ(2, context.number_of_handler_invocations);
    ION_ASSERT_OK(ion_timestamp_parse(&expected, (char *)expected_str, (SIZE)strlen(expected_str), &chars_used, &g_IonEventDecimalContext));
    ION_ASSERT_OK(ion_timestamp_equals(&expected, &ts, &is_equal, &g_IonEventDecimalContext));
    ASSERT_TRUE(is_equal);
}
