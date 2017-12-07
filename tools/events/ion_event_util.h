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

#ifndef IONC_ION_EVENT_UTIL_H
#define IONC_ION_EVENT_UTIL_H

#include "ion.h"

/**
 * Converts an ION_TYPE to a switchable int representing the given type's ID.
 */
#define ION_TID_INT(type) (int)(ION_TYPE_INT(type) >> 8)

// The following limits are arbitrarily high.
#define ION_EVENT_CONTAINER_DEPTH_MAX 100
#define ION_EVENT_ANNOTATION_MAX 100
#define ION_EVENT_DECIMAL_MAX_DIGITS 10000
#define ION_EVENT_DECIMAL_MAX_STRLEN (ION_EVENT_DECIMAL_MAX_DIGITS + 14) // 14 extra bytes as specified by decNumber.

/**
 * Global variable that holds the decimal context to be used throughout the tools and
 * tests. Initialized to contain arbitrarily high limits, which may be raised if
 * necessary, to avoid loss of precision.
 */
extern decContext g_IonEventDecimalContext;

typedef struct _ion_cli_writer_context {
    ION_WRITER_OPTIONS options;
    hWRITER writer;
    FILE *file_stream;
    ION_STREAM *ion_stream;
    bool has_imports;
} ION_EVENT_WRITER_CONTEXT;

/**
 * Initializes the given reader options using arbitrarily high limits.
 * @param options - the options to initialize.
 */
void ion_event_initialize_reader_options(ION_READER_OPTIONS *options);

/**
 * Initializes the given writer options using arbitrarily high limits.
 * @param options - the options to initialize.
 */
void ion_event_initialize_writer_options(ION_WRITER_OPTIONS *options);

iERR ion_event_in_memory_writer_open(ION_EVENT_WRITER_CONTEXT *writer_context, BOOL is_binary, ION_CATALOG *catalog, ION_COLLECTION *imports);
iERR ion_event_in_memory_writer_close(ION_EVENT_WRITER_CONTEXT *writer_context, BYTE **bytes, SIZE *bytes_len);

#endif //IONC_ION_EVENT_UTIL_H
