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

#include "ion_event_util.h"
#include <ion_helpers.h>

decContext g_IonEventDecimalContext = {
        ION_EVENT_DECIMAL_MAX_DIGITS,   // max digits
        DEC_MAX_MATH,                   // max exponent
        -DEC_MAX_MATH,                  // min exponent
        DEC_ROUND_HALF_EVEN,            // rounding mode
        DEC_Errors,                     // trap conditions
        0,                              // status flags
        0                               // apply exponent clamp?
};

void ion_event_initialize_writer_options(ION_WRITER_OPTIONS *options) {
    memset(options, 0, sizeof(ION_WRITER_OPTIONS));
    options->decimal_context = &g_IonEventDecimalContext;
    options->max_container_depth = ION_EVENT_CONTAINER_DEPTH_MAX;
    options->max_annotation_count = ION_EVENT_ANNOTATION_MAX;
}

void ion_event_initialize_reader_options(ION_READER_OPTIONS *options) {
    memset(options, 0, sizeof(ION_READER_OPTIONS));
    options->decimal_context = &g_IonEventDecimalContext;
    options->max_container_depth = ION_EVENT_CONTAINER_DEPTH_MAX;
    options->max_annotation_count = ION_EVENT_ANNOTATION_MAX;
}

iERR ion_event_in_memory_writer_open(ION_EVENT_WRITER_CONTEXT *writer_context, BOOL is_binary, ION_CATALOG *catalog, ION_COLLECTION *imports) {
    iENTER;
    memset(writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    IONCHECK(ion_stream_open_memory_only(&writer_context->ion_stream));
    ion_event_initialize_writer_options(&writer_context->options);
    writer_context->options.output_as_binary = is_binary;
    writer_context->options.pcatalog = catalog;
    if (imports) {
        IONCHECK(ion_writer_options_initialize_shared_imports(&writer_context->options));
        IONCHECK(ion_writer_options_add_shared_imports(&writer_context->options, imports));
        writer_context->has_imports = TRUE;
    }
    IONCHECK(ion_writer_open(&writer_context->writer, writer_context->ion_stream, &writer_context->options));
    iRETURN;
}

iERR ion_event_in_memory_writer_close(ION_EVENT_WRITER_CONTEXT *writer_context, BYTE **bytes, SIZE *bytes_len) {
    iENTER;
    POSITION pos;
    IONCHECK(ion_writer_close(writer_context->writer));
    pos = ion_stream_get_position(writer_context->ion_stream);
    IONCHECK(ion_stream_seek(writer_context->ion_stream, 0));
    *bytes = (BYTE *)(malloc((size_t)pos));
    SIZE bytes_read;
    IONCHECK(ion_stream_read(writer_context->ion_stream, *bytes, (SIZE)pos, &bytes_read));

    IONCHECK(ion_stream_close(writer_context->ion_stream));
    if (bytes_read != (SIZE)pos) {
        FAILWITH(IERR_EOF);
    }
    if (writer_context->has_imports) {
        IONCHECK(ion_writer_options_close_shared_imports(&writer_context->options));
    }
    *bytes_len = bytes_read;
    iRETURN;
}
