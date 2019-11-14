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
#include "ion_event_equivalence.h"

iERR test_stream_handler(struct _ion_user_stream *pstream) {
    iENTER;
    _ion_user_stream *next_stream = (_ion_user_stream *)pstream->handler_state;
    if (next_stream) {
        memcpy(pstream, next_stream, sizeof(_ion_user_stream));
    }
    else {
        FAILWITH(IERR_UNEXPECTED_EOF);
    }
    iRETURN;
}

TEST(IonUserStream, SucceedsInTheMiddleOfAValue) {
    const char *chunk1 = "{\"foo\": \"ba";
    const char *chunk2 = "r\"}";
    _ion_user_stream chunk1_stream, chunk2_stream;
    chunk1_stream.curr = (BYTE *)chunk1;
    chunk1_stream.limit = (BYTE *)(chunk1 + strlen(chunk1));
    chunk1_stream.handler_state = &chunk2_stream;
    chunk1_stream.handler = &test_stream_handler;
    chunk2_stream.curr = (BYTE *)chunk2;
    chunk2_stream.limit = (BYTE *)(chunk2 + strlen(chunk2));
    chunk2_stream.handler_state = NULL;
    chunk2_stream.handler = &test_stream_handler;

    ION_STREAM *ion_stream = NULL;
    hREADER reader;
    ION_TYPE type;
    ION_STRING value;
    ION_ASSERT_OK(ion_stream_open_handler_in(&test_stream_handler, &chunk1_stream, &ion_stream));
    ION_ASSERT_OK(ion_reader_open(&reader, ion_stream, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRING, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &value));
    ASSERT_EQ(0, strncmp("foo", (char *)value.value, value.length));
    ION_ASSERT_OK(ion_reader_read_string(reader, &value));
    ASSERT_EQ(0, strncmp("bar", (char *)value.value, value.length));
    ION_ASSERT_OK(ion_reader_step_out(reader));

    ION_ASSERT_OK(ion_reader_close(reader));
}
