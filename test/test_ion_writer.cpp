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

#include <ion_event_util.h>
#include <ion_event_stream_impl.h>
#include "ion_assert.h"
#include "ion_event_stream.h"
#include "ion_helpers.h"
#include "ion_test_util.h"

class WriterTest : public ::testing::Test {
protected:
    void SetUp() {
        out = tmpfile();
    }

    void TearDown() {
        fclose(out);
    }

    FILE *out;
};

iERR ion_test_open_file_writer(hWRITER *writer, FILE *out, BOOL is_binary) {
    iENTER;
    ION_WRITER_OPTIONS options;
    ION_STREAM *ion_stream = NULL;

    ion_event_initialize_writer_options(&options);
    options.output_as_binary = is_binary;

    IONCHECK(ion_stream_open_file_out(out, &ion_stream));
    IONCHECK(ion_writer_open(writer, ion_stream, &options));

    iRETURN;
}

TEST_F(WriterTest, BinaryWriterCloseMustFlushStream) {
    hWRITER writer = NULL;

    long file_size;

    ion_test_open_file_writer(&writer, out, TRUE);

    ION_ASSERT_OK(ion_writer_write_bool(writer, TRUE));

    ION_ASSERT_OK(ion_writer_close(writer));

    // get the size of the file after closing the writer
    fseek(out, 0L, SEEK_END);
    file_size = ftell(out);

    // 4 bytes for the IVM 1 byte for Ion bool
    ASSERT_EQ(file_size, 4 + 1);
}

TEST_F(WriterTest, BinaryWriterCloseMustTextStream) {
    hWRITER writer = NULL;

    long file_size;

    ion_test_open_file_writer(&writer, out, FALSE);

    ION_ASSERT_OK(ion_writer_write_bool(writer, TRUE));

    ION_ASSERT_OK(ion_writer_close(writer));

    // get the size of the file after closing the writer
    fseek(out, 0L, SEEK_END);
    file_size = ftell(out);

    ASSERT_EQ(file_size, 4);
}
