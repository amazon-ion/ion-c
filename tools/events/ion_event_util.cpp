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
#include <ion_event_stream.h>
#include <sstream>

decContext g_IonEventDecimalContext = {
        ION_EVENT_DECIMAL_MAX_DIGITS,   // max digits
        DEC_MAX_MATH,                   // max exponent
        -DEC_MAX_MATH,                  // min exponent
        DEC_ROUND_HALF_EVEN,            // rounding mode
        DEC_Errors,                     // trap conditions
        0,                              // status flags
        0                               // apply exponent clamp?
};

ION_STRING *ion_event_type_to_string(ION_EVENT_TYPE type) {
    switch (type) {
        case SCALAR: return &ion_event_event_type_scalar;
        case CONTAINER_START: return &ion_event_event_type_container_start;
        case CONTAINER_END: return &ion_event_event_type_container_end;
        case SYMBOL_TABLE: return &ion_event_event_type_symbol_table;
        case STREAM_END: return &ion_event_event_type_stream_end;
        default: return NULL;
    }
}

ION_EVENT_TYPE ion_event_type_from_string(ION_STRING *type_str) {
    if (ION_STRING_EQUALS(&ion_event_event_type_scalar, type_str)) {
        return SCALAR;
    }
    if (ION_STRING_EQUALS(&ion_event_event_type_container_start, type_str)) {
        return CONTAINER_START;
    }
    if (ION_STRING_EQUALS(&ion_event_event_type_container_end, type_str)) {
        return CONTAINER_END;
    }
    if (ION_STRING_EQUALS(&ion_event_event_type_symbol_table, type_str)) {
        return SYMBOL_TABLE;
    }
    if (ION_STRING_EQUALS(&ion_event_event_type_stream_end, type_str)) {
        return STREAM_END;
    }
    return UNKNOWN;
}

ION_STRING *ion_event_ion_type_to_string(ION_TYPE type) {
    switch (ION_TID_INT(type)) {
        case TID_NULL: return &ion_event_ion_type_null;
        case TID_BOOL: return &ion_event_ion_type_bool;
        case TID_POS_INT:
        case TID_NEG_INT: return &ion_event_ion_type_int;
        case TID_FLOAT: return &ion_event_ion_type_float;
        case TID_DECIMAL: return &ion_event_ion_type_decimal;
        case TID_TIMESTAMP: return &ion_event_ion_type_timestamp;
        case TID_SYMBOL: return &ion_event_ion_type_symbol;
        case TID_STRING: return &ion_event_ion_type_string;
        case TID_BLOB: return &ion_event_ion_type_blob;
        case TID_CLOB: return &ion_event_ion_type_clob;
        case TID_LIST: return &ion_event_ion_type_list;
        case TID_SEXP: return &ion_event_ion_type_sexp;
        case TID_STRUCT: return &ion_event_ion_type_struct;
        default: return NULL;
    }
}

ION_TYPE ion_event_ion_type_from_string(ION_STRING *type_str) {
    if (ION_STRING_EQUALS(&ion_event_ion_type_null, type_str)) {
        return tid_NULL;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_bool, type_str)) {
        return tid_BOOL;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_int, type_str)) {
        return tid_INT;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_float, type_str)) {
        return tid_FLOAT;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_decimal, type_str)) {
        return tid_DECIMAL;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_timestamp, type_str)) {
        return tid_TIMESTAMP;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_symbol, type_str)) {
        return tid_SYMBOL;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_string, type_str)) {
        return tid_STRING;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_blob, type_str)) {
        return tid_BLOB;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_clob, type_str)) {
        return tid_CLOB;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_list, type_str)) {
        return tid_LIST;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_sexp, type_str)) {
        return tid_SEXP;
    }
    if (ION_STRING_EQUALS(&ion_event_ion_type_struct, type_str)) {
        return tid_STRUCT;
    }
    return tid_none;
}

ION_STRING *ion_event_error_type_to_string(ION_EVENT_ERROR_TYPE type) {
    switch (type) {
        case ERROR_TYPE_READ: return &ion_event_error_type_read;
        case ERROR_TYPE_WRITE: return &ion_event_error_type_write;
        case ERROR_TYPE_STATE: return &ion_event_error_type_state;
        default: return NULL;
    }
}

ION_STRING *ion_event_comparison_result_type_to_string(ION_EVENT_COMPARISON_RESULT_TYPE type) {
    switch (type) {
        case COMPARISON_RESULT_EQUAL: return &ion_event_comparison_result_type_equal;
        case COMPARISON_RESULT_NOT_EQUAL: return &ion_event_comparison_result_type_not_equal;
        case COMPARISON_RESULT_ERROR: return &ion_event_comparison_result_type_error;
        default: return NULL;
    }
}

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

iERR ion_event_in_memory_writer_open(IonEventWriterContext *writer_context, std::string location, ION_EVENT_OUTPUT_TYPE output_type, ION_CATALOG *catalog, ION_COLLECTION *imports, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&location, NULL);
    IONCWRITE(ion_stream_open_memory_only(&writer_context->ion_stream));
    ion_event_initialize_writer_options(&writer_context->options);
    writer_context->options.output_as_binary = (output_type == OUTPUT_TYPE_BINARY);
    writer_context->options.pretty_print = output_type == OUTPUT_TYPE_TEXT_PRETTY || output_type == OUTPUT_TYPE_EVENTS;
    writer_context->options.pcatalog = catalog;
    if (imports) {
        IONCWRITE(ion_writer_options_initialize_shared_imports(&writer_context->options));
        IONCWRITE(ion_writer_options_add_shared_imports(&writer_context->options, imports));
        writer_context->has_imports = TRUE;
    }
    IONCWRITE(ion_writer_open(&writer_context->writer, writer_context->ion_stream, &writer_context->options));
    cRETURN;
}

iERR ion_event_writer_close(IonEventWriterContext *writer_context, IonEventResult *result, iERR err, bool in_memory,
                            BYTE **bytes, SIZE *bytes_len) {
    ION_SET_ERROR_CONTEXT(&writer_context->output_location, NULL);
    if (writer_context->writer) {
        ION_NON_FATAL(ion_writer_close(writer_context->writer), "Failed to close writer.");
        writer_context->writer = NULL;
    }
    if (writer_context->has_imports) {
        ION_NON_FATAL(ion_writer_options_close_shared_imports(&writer_context->options), "Failed to close writer imports.");
        writer_context->has_imports = FALSE;
    }
    if (writer_context->ion_stream) {
        ASSERT(!writer_context->file_stream);
        if (in_memory) {
            ASSERT(bytes);
            SIZE bytes_read;
            POSITION pos = ion_stream_get_position(writer_context->ion_stream);
            ION_NON_FATAL(ion_stream_seek(writer_context->ion_stream, 0), "Failed to seek the output stream to the beginning.");
            *bytes = (BYTE *)malloc((size_t)pos);
            ION_NON_FATAL(ion_stream_read(writer_context->ion_stream, *bytes, (SIZE) pos, &bytes_read),
                          "Failed to retrieve bytes from the output stream.");

            if (bytes_read != (SIZE)pos) {
                ION_NON_FATAL(IERR_EOF, "Read an invalid number of bytes from the output stream.");
            }
            *bytes_len = bytes_read;
        }
        ION_NON_FATAL(ion_stream_close(writer_context->ion_stream), "Failed to close ION_STREAM.");
        writer_context->ion_stream = NULL;
    }
    if (writer_context->file_stream) {
        ASSERT(!in_memory);
        fclose(writer_context->file_stream);
        writer_context->file_stream = NULL;
    }
    iRETURN;
}

iERR ion_event_in_memory_writer_close(IonEventWriterContext *writer_context, BYTE **bytes, SIZE *bytes_len, iERR err, IonEventResult *result) {
    UPDATEERROR(ion_event_writer_close(writer_context, result, err, true, bytes, bytes_len));
    cRETURN;
}

std::string _ion_error_message(iERR error_code, std::string msg, const char *file, int line) {
    std::ostringstream ss;
    ss << file << ":" << line << " : "
       << ion_error_to_str(error_code);
       if (!msg.empty()) {
           ss << ": " << msg;
       }
    return ss.str();
}

void _ion_event_set_error(IonEventResult *result, ION_EVENT_ERROR_TYPE error_type, iERR error_code, std::string msg,
                          std::string *location, size_t *event_index, const char *file, int line) {
    if (result != NULL) {
        result->error_description.error_type = error_type;
        result->error_description.message = _ion_error_message(error_code, msg, file, line);
        if (location != NULL) {
            result->error_description.location = *location; // TODO can there be multiple locations? Like when there's an error during comparison. Or multiple contexts?
            result->error_description.has_location = true;
        }
        if (event_index != NULL) {
            result->error_description.event_index = *event_index;
            result->error_description.has_event_index = true;
        }
        result->has_error_description = true;
    }
}

std::string ion_event_symbol_to_string(ION_SYMBOL *symbol) {
    std::ostringstream ss;
    ss << "(text=" << ION_EVENT_STRING_OR_NULL(&symbol->value) << ", local_sid=" << symbol->sid
       << ", location=(" << ION_EVENT_STRING_OR_NULL(&symbol->import_location.name) << ", "
       << symbol->import_location.location << "))";
    return ss.str();
}
