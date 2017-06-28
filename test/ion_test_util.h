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

#ifndef IONC_ION_TEST_UTIL_H
#define IONC_ION_TEST_UTIL_H

#include "ion.h"

/**
 * Initializes the given writer options to the test defaults, which provide
 * arbitrarily high limits.
 * @param options - the options to initialize.
 */
void ion_test_initialize_writer_options(ION_WRITER_OPTIONS *options);

/**
 * Initializes and opens a new in-memory writer.
 * @param writer - the writer to initialize and open.
 * @param ion_stream - output parameter for the underlying in-memory stream.
 * @param is_binary - TRUE if the writer should be a binary writer; else FALSE.
 * @return IERR_OK, unless the writer or stream fails to open.
 */
iERR ion_test_new_writer(hWRITER *writer, ION_STREAM **ion_stream, BOOL is_binary);

/**
 * Closes a writer and its in-memory stream and copies the written bytes.
 * @param writer - the writer to finish.
 * @param ion_stream - the stream to close.
 * @param out - output parameter for the copied written bytes.
 * @param len - the length of the written bytes.
 * @return IERR_OK, unless the writer or stream fails to close.
 */
iERR ion_test_writer_get_bytes(hWRITER writer, ION_STREAM *ion_stream, BYTE **out, SIZE *len);

/**
 * Assigns the given char * to an ION_STRING, without copying.
 * @param cstr - the char * to assign.
 * @param out - the ION_STRING to assign to.
 * @return IERR_OK, unless out is NULL.
 */
iERR ion_string_from_cstr(const char *cstr, ION_STRING *out);

/**
 * Initializes the given reader options to the test defaults, which provide
 * arbitrarily high limits.
 * @param options - the options to initialize.
 */
void ion_test_initialize_reader_options(ION_READER_OPTIONS *options);

/**
 * Initializes and opens a new in-memory reader over the given buffer of Ion data.
 * @param ion_data - the Ion data to read.
 * @param buffer_length - the length of the buffer of Ion data.
 * @param reader - the reader to initialize and open.
 * @return IERR_OK, unless the reader fails to open.
 */
iERR ion_test_new_reader(BYTE *ion_data, SIZE buffer_length, hREADER *reader);

/**
 * Initializes and opens a new in-memory reader over the given string of Ion text.
 * @param ion_text - the Ion text to read.
 * @param reader - the reader to initialize and open.
 * @return IERR_OK, unless the reader fails to open.
 */
iERR ion_test_new_text_reader(const char *ion_text, hREADER *reader);

/**
 * Reads the Ion string at the given reader's current position and assigns its contents
 * to a char *.
 * @param reader - the reader from which to read the string.
 * @param out - output parameter for the copied string.
 * @return IERR_OK, unless the read or the copy fails.
 */
iERR ion_read_string_as_chars(hREADER reader, char **out);

#endif //IONC_ION_TEST_UTIL_H
