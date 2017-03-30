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

iERR test_open_string_reader(const char* ion_text, hREADER* reader)
{
    iENTER;

    size_t buffer_length = strlen(ion_text);
    BYTE* buffer = (BYTE *)calloc(buffer_length, sizeof(BYTE));
    memcpy((char *)(&buffer[0]), ion_text, buffer_length);

    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.return_system_values = TRUE;

    IONCHECK(ion_reader_open_buffer(reader, buffer, buffer_length, &options));
    iRETURN;
}

iERR test_read_string_as_char(hREADER reader, char **out)
{
    iENTER;
    ION_STRING ion_string;
    IONCHECK(ion_reader_read_string(reader, &ion_string));
    *out = ion_string_strdup(&ion_string);
    iRETURN;
}

TEST(TextReader, HandlesNestedSexps)
{
    const char* ion_text = "((first)(second))((third)(fourth))";
    char *third, *fourth;

    hREADER reader;
    ASSERT_EQ(IERR_OK, test_open_string_reader(ion_text, &reader));

    ION_TYPE type;
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ASSERT_EQ(IERR_OK, ion_reader_step_in(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ASSERT_EQ(IERR_OK, ion_reader_step_out(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ASSERT_EQ(IERR_OK, ion_reader_step_in(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ASSERT_EQ(IERR_OK, ion_reader_step_in(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ASSERT_EQ(IERR_OK, test_read_string_as_char(reader, &third));
    ASSERT_STREQ("third", third);
    ASSERT_EQ(IERR_OK, ion_reader_step_out(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SEXP, type);
    ASSERT_EQ(IERR_OK, ion_reader_step_in(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ASSERT_EQ(IERR_OK, test_read_string_as_char(reader, &fourth));
    ASSERT_STREQ("fourth", fourth);
    ASSERT_EQ(IERR_OK, ion_reader_step_out(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(IERR_OK, ion_reader_step_out(reader));
    ASSERT_EQ(IERR_OK, ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);

    if (reader != NULL) {
        ion_reader_close(reader);
    }
}
