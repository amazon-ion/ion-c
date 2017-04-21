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

#include <cstdlib>
#include "ion_helpers.h"
#include "ion_test_util.h"

iERR ion_test_new_writer(hWRITER *writer, ION_STREAM **ion_stream, BOOL is_binary) {
    iENTER;
    IONCHECK(ion_stream_open_memory_only(ion_stream));
    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = is_binary;
    IONCHECK(ion_writer_open(writer, *ion_stream, &options));
    iRETURN;
}

iERR ion_test_writer_get_bytes(hWRITER writer, ION_STREAM *ion_stream, BYTE **out, SIZE *len) {
    iENTER;
    POSITION pos;
    IONCHECK(ion_writer_close(writer));
    pos = ion_stream_get_position(ion_stream);
    IONCHECK(ion_stream_seek(ion_stream, 0));
    *out = (BYTE *)(malloc((size_t)pos));
    IONCHECK(ion_stream_read(ion_stream, *out, (SIZE)pos, len));
    IONCHECK(ion_stream_close(ion_stream));
    if (*len != (SIZE)pos) {
        FAILWITH(IERR_EOF);
    }
    iRETURN;
}

iERR ion_string_from_cstr(const char *cstr, ION_STRING *out) {
    iENTER;
    if (!out) FAILWITH(IERR_INVALID_ARG);
    out->value = (BYTE *)cstr;
    out->length = (SIZE)strlen(cstr);
    iRETURN;
}
