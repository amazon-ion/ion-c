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
#include "ion_event_stream.h"

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

// Event stream marker
static ION_STRING ion_cli_event_stream_symbol = {17, (BYTE *)"$ion_event_stream"}; // TODO where should this go?

// Event fields
static ION_STRING ion_event_event_type_field = {10, (BYTE *)"event_type"};
static ION_STRING ion_event_ion_type_field = {8, (BYTE *)"ion_type"};
static ION_STRING ion_event_field_name_field = {10, (BYTE *)"field_name"};
static ION_STRING ion_event_annotations_field = {11, (BYTE *)"annotations"};
static ION_STRING ion_event_value_text_field = {10, (BYTE *)"value_text"};
static ION_STRING ion_event_value_binary_field = {12, (BYTE *)"value_binary"};
static ION_STRING ion_event_imports_field = {7, (BYTE *)"imports"};
static ION_STRING ion_event_depth_field = {5, (BYTE *)"depth"};

// Symbol token fields
static ION_STRING ion_event_text_field = {4, (BYTE *)"text"};
static ION_STRING ion_event_import_location_field = {15, (BYTE *)"import_location"};

// Import location fields
static ION_STRING ion_event_name_field = {4, (BYTE *)"name"};
static ION_STRING ion_event_import_sid_field = {8, (BYTE *)"location"};

// Symbol table import fields
static ION_STRING ion_event_import_name_field = {11, (BYTE *)"import_name"};
static ION_STRING ion_event_max_id_field = {6, (BYTE *)"max_id"};
static ION_STRING ion_event_version_field = {7, (BYTE *)"version"};

// Event type string representations
static ION_STRING ion_event_event_type_scalar = {6, (BYTE *)"SCALAR"};
static ION_STRING ion_event_event_type_container_start = {15, (BYTE *)"CONTAINER_START"};
static ION_STRING ion_event_event_type_container_end = {13, (BYTE *)"CONTAINER_END"};
static ION_STRING ion_event_event_type_symbol_table = {12, (BYTE *)"SYMBOL_TABLE"};
static ION_STRING ion_event_event_type_stream_end = {10, (BYTE *)"STREAM_END"};

// Ion type string representations
static ION_STRING ion_event_ion_type_null = {4, (BYTE *)"NULL"};
static ION_STRING ion_event_ion_type_bool = {4, (BYTE *)"BOOL"};
static ION_STRING ion_event_ion_type_int = {3, (BYTE *)"INT"};
static ION_STRING ion_event_ion_type_float = {5, (BYTE *)"FLOAT"};
static ION_STRING ion_event_ion_type_decimal = {7, (BYTE *)"DECIMAL"};
static ION_STRING ion_event_ion_type_timestamp = {9, (BYTE *)"TIMESTAMP"};
static ION_STRING ion_event_ion_type_symbol = {6, (BYTE *)"SYMBOL"};
static ION_STRING ion_event_ion_type_string = {6, (BYTE *)"STRING"};
static ION_STRING ion_event_ion_type_blob = {4, (BYTE *)"BLOB"};
static ION_STRING ion_event_ion_type_clob = {4, (BYTE *)"CLOB"};
static ION_STRING ion_event_ion_type_list = {4, (BYTE *)"LIST"};
static ION_STRING ion_event_ion_type_sexp = {4, (BYTE *)"SEXP"};
static ION_STRING ion_event_ion_type_struct = {6, (BYTE *)"STRUCT"};

typedef struct _ion_cli_writer_context {
    ION_WRITER_OPTIONS options;
    hWRITER writer;
    FILE *file_stream;
    ION_STREAM *ion_stream;
    bool has_imports;
} ION_EVENT_WRITER_CONTEXT;

ION_STRING *ion_event_type_to_string(ION_EVENT_TYPE type);
ION_EVENT_TYPE ion_event_type_from_string(ION_STRING *type_str);
ION_STRING *ion_event_ion_type_to_string(ION_TYPE type);
ION_TYPE ion_event_ion_type_from_string(ION_STRING *type_str);

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
