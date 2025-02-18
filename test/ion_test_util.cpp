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
#include "ion_event_util.h"

void BinaryAndTextTest::SetUp() {
    is_binary = GetParam();
}

iERR ion_test_new_writer(hWRITER *writer, ION_STREAM **ion_stream, BOOL is_binary) {
    iENTER;
    IONCHECK(ion_stream_open_memory_only(ion_stream));
    ION_WRITER_OPTIONS options;
    ion_event_initialize_writer_options(&options);
    options.output_as_binary = is_binary;
    IONCHECK(ion_writer_open(writer, *ion_stream, &options));
    iRETURN;
}

iERR ion_test_writer_get_bytes(hWRITER writer, ION_STREAM *ion_stream, BYTE **out, SIZE *len) {
    iENTER;
    POSITION pos;
    UPDATEERROR(ion_writer_close(writer));
    pos = ion_stream_get_position(ion_stream);
    UPDATEERROR(ion_stream_seek(ion_stream, 0));
    *out = (BYTE *)(malloc((size_t)pos));
    UPDATEERROR(ion_stream_read(ion_stream, *out, (SIZE)pos, len));
    UPDATEERROR(ion_stream_close(ion_stream));
    if (*len != (SIZE)pos) {
        UPDATEERROR(IERR_EOF);
    }
    RETURN(__location_name__, __line__, __count__++, err);
}

iERR ion_test_writer_write_symbol_sid(ION_WRITER *writer, SID sid) {
    iENTER;
    ION_SYMBOL symbol;
    memset(&symbol, 0, sizeof(ION_SYMBOL));
    symbol.sid = sid;
    IONCHECK(ion_writer_write_ion_symbol(writer, &symbol));
    iRETURN;
}

iERR ion_test_writer_add_annotation_sid(ION_WRITER *writer, SID sid) {
    iENTER;
    ION_SYMBOL symbol;
    memset(&symbol, 0, sizeof(ION_SYMBOL));
    symbol.sid = sid;
    IONCHECK(ion_writer_add_annotation_symbol(writer, &symbol));
    iRETURN;
}

iERR ion_test_writer_write_field_name_sid(ION_WRITER *writer, SID sid) {
    iENTER;
    ION_SYMBOL symbol;
    memset(&symbol, 0, sizeof(ION_SYMBOL));
    symbol.sid = sid;
    IONCHECK(ion_writer_write_field_name_symbol(writer, &symbol));
    iRETURN;
}

iERR ion_string_from_cstr(const char *cstr, ION_STRING *out) {
    iENTER;
    if (!out) FAILWITH(IERR_INVALID_ARG);
    out->value = (BYTE *)cstr;
    out->length = (SIZE)strlen(cstr);
    iRETURN;
}

iERR ion_test_new_reader(BYTE *ion_data, SIZE buffer_length, hREADER *reader) {
    iENTER;
    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    IONCHECK(ion_reader_open_buffer(reader, ion_data, buffer_length, &options));
    iRETURN;
}

iERR ion_test_new_text_reader(const char *ion_text, hREADER *reader) {
    iENTER;
    IONCHECK(ion_test_new_reader((BYTE *)ion_text, (SIZE)strlen(ion_text), reader));
    iRETURN;
}

iERR ion_test_reader_read_symbol_sid(ION_READER *reader, SID *sid) {
    iENTER;
    ASSERT(sid);
    ION_SYMBOL symbol;
    IONCHECK(ion_reader_read_ion_symbol(reader, &symbol));
    *sid = symbol.sid;
    iRETURN;
}

iERR ion_read_string_as_chars(hREADER reader, char **out) {
    iENTER;
    ION_STRING ion_string;
    IONCHECK(ion_reader_read_string(reader, &ion_string));
    *out = ion_string_strdup(&ion_string);
    iRETURN;
}

void ion_test_print_bytes(BYTE *bytes, SIZE length) {
    for(int i = 0; i < length; i++) {
        printf("\\x%02X", bytes[i]);
    }
    printf("\n");
}
