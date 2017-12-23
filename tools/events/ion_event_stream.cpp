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

#include "ion_event_stream.h"
#include <ion_helpers.h>
#include <sstream>
#include "ion_event_util.h"
#include "ion_event_equivalence.h"

void free_ion_string(ION_STRING *str) {
    if (str) {
        if (str->value) {
            free(str->value);
        }
        free(str);
    }
}

void free_ion_symbol_components(ION_SYMBOL *symbol) {
    if (symbol) {
        if (symbol->value.value) {
            free(symbol->value.value);
        }
        if (symbol->import_location.name.value) {
            free(symbol->import_location.name.value);
        }
    }
}

void free_ion_symbol(ION_SYMBOL *symbol) {
    if (symbol) {
        free_ion_symbol_components(symbol);
        free(symbol);
    }
}

void free_ion_symbols(ION_SYMBOL *symbols, SIZE len) {
    if (symbols) {
        for (int i = 0; i < len; i++) {
            free_ion_symbol_components(&symbols[i]);
        }
        free(symbols);
    }
}

void free_ion_event_value(void *value, ION_TYPE ion_type, ION_EVENT_TYPE event_type) {
    if (event_type == SYMBOL_TABLE) {
        ion_free_owner(value);
    }
    else if (value) {
        switch (ION_TID_INT(ion_type)) {
            case TID_POS_INT:
            case TID_NEG_INT:
                ion_int_free((ION_INT *)value);
                break;
            case TID_DECIMAL:
                ion_decimal_free((ION_DECIMAL *) value);
                free(value);
                break;
            case TID_SYMBOL:
                free_ion_symbol((ION_SYMBOL *)value);
                break;
            case TID_STRING:
            case TID_CLOB:
            case TID_BLOB:
                free_ion_string((ION_STRING *) value);
                break;
            default:
                free(value);
                break;
        }
    }
}

ION_STRING *copy_ion_string(ION_STRING *src) {
    ION_STRING *string_value = (ION_STRING *)malloc(sizeof(ION_STRING));
    size_t len = (size_t)src->length;
    if (src->value == NULL) {
        ASSERT(len == 0);
        string_value->value = NULL;
    }
    else {
        string_value->value = (BYTE *) malloc(len);
        memcpy(string_value->value, src->value, len);
    }
    string_value->length = (int32_t)len;
    return string_value;
}

void copy_ion_symbol_into(ION_SYMBOL *copy, ION_SYMBOL *src) {
    copy->value.length = src->value.length;
    if (src->value.value != NULL) {
        copy->value.value = (BYTE *) malloc(sizeof(BYTE) * copy->value.length);
        memcpy(copy->value.value, src->value.value, (size_t) copy->value.length);
    }
    ION_STRING_INIT(&copy->import_location.name);
    if (!ION_SYMBOL_IMPORT_LOCATION_IS_NULL(src)) {
        copy->import_location.name.length = src->import_location.name.length;
        copy->import_location.name.value = (BYTE *) malloc(sizeof(BYTE) * copy->import_location.name.length);
        memcpy(copy->import_location.name.value, src->import_location.name.value,
               (size_t) copy->import_location.name.length);
        copy->import_location.location = src->import_location.location;
    }
    copy->sid = src->sid;
}

void copy_ion_symbol(ION_SYMBOL **dst, ION_SYMBOL *src) {
    ION_SYMBOL *copy = NULL;
    if (src != NULL) {
        copy = (ION_SYMBOL *) calloc(1, sizeof(ION_SYMBOL));
        copy_ion_symbol_into(copy, src);
    }
    *dst = copy;
}

void copy_ion_symbols(ION_SYMBOL **dst, ION_SYMBOL *src, size_t count) {
    ION_SYMBOL *copy = NULL;
    if (count > 0) {
        copy = (ION_SYMBOL *)calloc((size_t) count, sizeof(ION_SYMBOL));
        for (int i = 0; i < count; i++) {
            copy_ion_symbol_into(&copy[i], &src[i]);
        }
    }
    *dst = copy;
}

IonEvent::IonEvent(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name, ION_SYMBOL *annotations, SIZE num_annotations, int depth) {
    // TODO perform the symbols copies here and remove them everywhere else. Also add a destructor.
    this->event_type = event_type;
    this->ion_type = ion_type;
    copy_ion_symbol(&this->field_name, field_name);
    copy_ion_symbols(&this->annotations, annotations, (size_t)num_annotations);
    this->num_annotations = num_annotations;
    this->depth = depth;
    value = NULL;
}
/*
IonEvent::IonEvent(IonEvent *that) {
    this = IonEvent(that->event_type, that->ion_type, that->field_name, that->annotations, that->num_annotations, that->depth);
    ion_event_copy_value(that, &this->value, NULL);
}
*/
IonEvent::~IonEvent() {
    free_ion_symbols(annotations, num_annotations);
    free_ion_symbol(field_name);
    free_ion_event_value(value, ion_type, event_type);
}

IonEventStream::IonEventStream(std::string location) {
    event_stream = new std::vector<IonEvent *>();
    this->location = location;
}

IonEventStream::~IonEventStream() {
    for (size_t i = 0; i < event_stream->size(); i++) {
        delete event_stream->at(i);
    }
    delete event_stream;
}

IonEvent * IonEventStream::append_new(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name,
                                      ION_SYMBOL *annotations, SIZE num_annotations, int depth) {
    IonEvent *event = new IonEvent(event_type, ion_type, field_name, annotations, num_annotations, depth);
    event_stream->push_back(event);
    return event;
}

void IonEventReport::addResult(IonEventResult *result) {
    if (result->has_error_description) {
        error_report.push_back(result->error_description);
    }
    if (result->has_comparison_result) {
        comparison_report.push_back(result->comparison_result);
    }
}

iERR IonEventReport::writeErrorsTo(hWRITER writer) {
    iENTER;
    for (size_t i = 0; i < error_report.size(); i++) {
        IONREPORT(ion_event_stream_write_error(writer, &error_report.at(i)));
    }
    cRETURN;
}

iERR IonEventReport::writeComparisonResultsTo(hWRITER writer) {
    iENTER;
    for (size_t i = 0; i < comparison_report.size(); i++) {
        IONREPORT(ion_event_stream_write_comparison_result(writer, &comparison_report.at(i)));
    }
    cRETURN;
}

iERR ion_event_stream_write_error_report(hWRITER writer, IonEventReport *report) {
    iENTER;
    ASSERT(report);
    IONREPORT(report->writeErrorsTo(writer));
    cRETURN;
}

iERR ion_event_stream_write_comparison_report(hWRITER writer, IonEventReport *report) {
    iENTER;
    ASSERT(report);
    IONREPORT(report->writeComparisonResultsTo(writer));
    cRETURN;
}

size_t valueEventLength(IonEventStream *stream, size_t start_index) {
    IonEvent *start = stream->at(start_index);
    if (CONTAINER_START == start->event_type) {
        size_t length;
        size_t i = start_index;
        while (TRUE) {
            IonEvent *curr = stream->at(++i);
            if (curr->event_type == CONTAINER_END && curr->depth == start->depth) {
                length = ++i - start_index;
                break;
            }
        }
        return length;
    }
    return 1;
}

iERR _ion_event_stream_read_all_recursive(hREADER hreader, IonEventStream *stream, BOOL in_struct, int depth, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    ION_TYPE t;
    for (;;) {
        IONCREAD(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            break;
        }
        IONREPORT(ion_event_stream_read(hreader, stream, t, in_struct, depth, result));
    }
    cRETURN;
}

/**
 * Copies the given SCALAR event's value so that it may be used outside the scope of the event's owning stream. The
 * copied value is allocated such that it may be freed safely by free_ion_event_value.
 */
iERR ion_event_copy_value(IonEvent *event, void **value, IonEventResult *result) {
    iENTER;
    BOOL *bool_val = NULL;
    ION_INT *int_val = NULL;
    double *float_val = NULL;
    ION_DECIMAL *decimal = NULL;
    ION_TIMESTAMP *timestamp = NULL;
    ION_SYMBOL *symbol = NULL;

    ASSERT(value);
    ASSERT(event);

    if (event->event_type != SCALAR) {
        IONFAILSTATE(IERR_INVALID_ARG, "Illegal state: cannot copy the value of a non-SCALAR event.", result);
    }
    switch (ION_TID_INT(event->ion_type)) {
        case TID_NULL:
            break;
        case TID_BOOL:
            bool_val = (BOOL *)malloc(sizeof(BOOL));
            *bool_val = *(BOOL *)event->value;
            *value = bool_val;
            break;
        case TID_POS_INT:
        case TID_NEG_INT:
            // NOTE: owner must be NULL; otherwise, this may be unexpectedly freed.
            IONCSTATE(ion_int_alloc(NULL, &int_val), "Failed to allocate a new ION_INT.");
            *value = int_val;
            IONCSTATE(ion_int_copy(int_val, (ION_INT *)event->value, int_val->_owner), "Failed to copy int value.");
            break;
        case TID_FLOAT:
            float_val = (double *)malloc(sizeof(double));
            *float_val = *(double *)event->value;
            *value = float_val;
            break;
        case TID_DECIMAL:
            decimal = (ION_DECIMAL *)calloc(1, sizeof(ION_DECIMAL));
            *value = decimal;
            IONCSTATE(ion_decimal_copy(decimal, (ION_DECIMAL *)event->value), "Failed to copy decimal value.");
            break;
        case TID_TIMESTAMP:
            timestamp = (ION_TIMESTAMP *)malloc(sizeof(ION_TIMESTAMP));
            // TODO it will not be this simple if ION_TIMESTAMP's fraction field is upgraded to use ION_DECIMAL.
            memcpy(timestamp, (ION_TIMESTAMP *)event->value, sizeof(ION_TIMESTAMP));
            *value = timestamp;
            break;
        case TID_SYMBOL:
            copy_ion_symbol(&symbol, (ION_SYMBOL *)event->value);
            *value = symbol;
            break;
        case TID_STRING:
        case TID_CLOB:
        case TID_BLOB:
            *value = copy_ion_string((ION_STRING *)event->value);
            break;
        default:
            IONFAILSTATE(IERR_INVALID_ARG, "Illegal state: unknown Ion type in event.", result);
    }
    cRETURN;
}

iERR ion_event_copy(IonEvent **dst, IonEvent *src, IonEventResult *result) {
    iENTER;
    ASSERT(dst != NULL);
    ASSERT(*dst == NULL);
    ASSERT(src != NULL);
    *dst = new IonEvent(src->event_type, src->ion_type, src->field_name, src->annotations, src->num_annotations, src->depth);
    IONREPORT(ion_event_copy_value(src, &(*dst)->value, result));
    cRETURN;
}

iERR record_symbol_table_context_change(void *stream, ION_COLLECTION *imports) {
    iENTER;
    IonEventStream *event_stream = (IonEventStream *)stream;
    IonEvent *event = event_stream->append_new(SYMBOL_TABLE, tid_none, NULL, NULL, 0, 0);
    ION_COLLECTION *copied_imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
    _ion_collection_initialize(copied_imports, copied_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    event->value = copied_imports;
    IONCHECK(_ion_collection_copy(copied_imports, imports, &_ion_symbol_table_local_import_copy_new_owner, copied_imports));
    iRETURN;
}

void ion_event_register_symbol_table_callback(ION_READER_OPTIONS *options, IonEventStream *stream) {
    options->context_change_notifier.notify = &record_symbol_table_context_change;
    options->context_change_notifier.context = stream;
}

/**
 * Reads the reader's current value into an IonEvent and appends that event to the given IonEventStream. The event's
 * value is allocated such that it may be freed safely by free_ion_event_value.
 */
iERR ion_event_stream_read(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    BOOL is_null;
    SIZE annotation_count = 0;
    ION_SYMBOL *field_name = NULL;
    ION_SYMBOL *annotations = NULL;
    BYTE *lob_tmp = NULL;
    IonEvent *event = NULL;
    ION_EVENT_TYPE event_type = SCALAR;
    int ion_type = ION_TID_INT(t);

    if (in_struct) {
        IONCREAD(ion_reader_get_field_name_symbol(hreader, &field_name));
    }

    IONCREAD(ion_reader_get_annotation_count(hreader, &annotation_count));
    if (annotation_count > 0) {
        annotations = (ION_SYMBOL *)calloc((size_t)annotation_count, sizeof(ION_SYMBOL));
        IONCREAD(ion_reader_get_annotation_symbols(hreader, annotations, annotation_count, &annotation_count));
    }

    IONCREAD(ion_reader_is_null(hreader, &is_null));
    if (is_null) {
        IONCREAD(ion_reader_read_null(hreader, &t));
        event_type = SCALAR;
    }
    else if (ion_type == TID_STRUCT || ion_type == TID_LIST || ion_type == TID_SEXP) {
        event_type = CONTAINER_START;
    }
    event = stream->append_new(event_type, t, field_name, annotations, annotation_count, depth);

    if (is_null) {
        IONCLEANEXIT;
    }
    switch (ion_type) {
        case TID_EOF:
            IONCLEANEXIT;
        case TID_BOOL:
            event->value = (BOOL *)malloc(sizeof(BOOL));
            IONCREAD(ion_reader_read_bool(hreader, (BOOL *)event->value));
            break;
        case TID_POS_INT:
        case TID_NEG_INT:
        {
            ION_INT *ion_int_value = NULL;
            IONCREAD(ion_int_alloc(NULL, &ion_int_value)); // NOTE: owner must be NULL; otherwise, this may be unexpectedly freed.
            event->value = ion_int_value;
            IONCREAD(ion_reader_read_ion_int(hreader, (ION_INT *)event->value));
            break;
        }
        case TID_FLOAT:
            event->value = (double *)malloc(sizeof(double));
            IONCREAD(ion_reader_read_double(hreader, (double *)event->value));
            break;
        case TID_DECIMAL:
            event->value = (ION_DECIMAL *)malloc(sizeof(ION_DECIMAL));
            IONCREAD(ion_reader_read_ion_decimal(hreader, (ION_DECIMAL *)event->value));
            IONCREAD(ion_decimal_claim((ION_DECIMAL *)event->value));
            break;
        case TID_TIMESTAMP:
            event->value = (ION_TIMESTAMP *)malloc(sizeof(ION_TIMESTAMP));
            IONCREAD(ion_reader_read_timestamp(hreader, (ION_TIMESTAMP *)event->value));
            break;
        case TID_SYMBOL:
        {
            ION_SYMBOL tmp, *symbol_value;
            IONCREAD(ion_reader_read_ion_symbol(hreader, &tmp));
            copy_ion_symbol(&symbol_value, &tmp);
            event->value = symbol_value;
            break;
        }
        case TID_STRING:
        {
            ION_STRING tmp;
            IONCREAD(ion_reader_read_string(hreader, &tmp));
            ION_STRING *string_value = copy_ion_string(&tmp);
            event->value = string_value;
            break;
        }
        case TID_CLOB: // intentional fall-through
        case TID_BLOB:
        {
            SIZE length, bytes_read;
            IONCREAD(ion_reader_get_lob_size(hreader, &length));
            lob_tmp = (BYTE*)malloc((size_t)length * sizeof(BYTE));
            if (length) {
                IONCREAD(ion_reader_read_lob_bytes(hreader, lob_tmp, length, &bytes_read));
                if (length != bytes_read) {
                    IONFAILSTATE(IERR_EOF, "Lob bytes read did not match the number expected.", result);
                }
            }
            ION_LOB *lob_value = (ION_LOB *)malloc(sizeof(ION_LOB));
            lob_value->value = lob_tmp;
            lob_tmp = NULL; // This is now owned by the value, and will be freed by the IonEventStream destructor.
            lob_value->length = length;
            event->value = lob_value;
            break;
        }
        case TID_STRUCT: // intentional fall-through
        case TID_SEXP: // intentional fall-through
        case TID_LIST:
            IONCREAD(ion_reader_step_in(hreader));
            IONREPORT(_ion_event_stream_read_all_recursive(hreader, stream, t == tid_STRUCT, depth + 1, result));
            IONCREAD(ion_reader_step_out(hreader));
            stream->append_new(CONTAINER_END, t, /*field_name=*/NULL, /*annotations=*/NULL, /*annotation_count=*/0,
                               depth);
            break;
        case TID_DATAGRAM:
        default: IONFAILSTATE(IERR_INVALID_STATE, "Unknown Ion type.", result);
    }
cleanup:
    if (annotations) {
        free(annotations);
    }
    if (lob_tmp) {
        free(lob_tmp);
    }
    iRETURN;
}

iERR ion_event_stream_read_all(hREADER hreader, IonEventStream *stream, IonEventResult *result) {
    iENTER;
    IONREPORT(_ion_event_stream_read_all_recursive(hreader, stream, /*in_struct=*/FALSE, /*depth=*/0, result));
    stream->append_new(STREAM_END, tid_none, NULL, NULL, 0, 0);
    cRETURN;
}

iERR ion_event_stream_read_import_location(hREADER reader, ION_SYMBOL_IMPORT_LOCATION *import_location, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_TYPE ion_type;
    ION_STRING field_name;
    BOOL is_null;
    ASSERT(import_location);

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_import_name_field, &field_name)) {
            if (is_null || ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportLocation import_name must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &import_location->name));
        }
        else if (ION_STRING_EQUALS(&ion_event_import_sid_field, &field_name)) {
            if (is_null || ion_type != tid_INT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: Location import_sid must be an int.", result);
            }
            IONCREAD(ion_reader_read_int(reader, &import_location->location));
        }
        // Open content is ignored.
    }

    if (ION_STRING_IS_NULL(&import_location->name) || import_location->location <= UNKNOWN_SID) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: Import location import_name and import_sid are required", result);
    }

    cRETURN;
}

iERR ion_event_stream_read_symbol_token(hREADER reader, ION_SYMBOL *symbol, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_TYPE ion_type;
    ION_STRING field_name;
    BOOL is_null;
    ASSERT(symbol);

    symbol->add_count = 0;
    symbol->sid = UNKNOWN_SID;
    ION_STRING_INIT(&symbol->value);
    ION_STRING_INIT(&symbol->import_location.name);
    symbol->import_location.location = UNKNOWN_SID;

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_text_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: SymbolToken text must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &symbol->value));
        }
        else if (ION_STRING_EQUALS(&ion_event_import_location_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_STRUCT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: SymbolToken location must be an ImportLocation struct.", result);
            }
            IONCREAD(ion_reader_step_in(reader));
            IONREPORT(ion_event_stream_read_import_location(reader, &symbol->import_location, location, result));
            IONCREAD(ion_reader_step_out(reader));
        }
        // Open content is ignored.
    }

    if (ION_STRING_IS_NULL(&symbol->value) && ION_STRING_IS_NULL(&symbol->import_location.name)) {
        symbol->sid = 0;
    }
    cRETURN;
}

iERR ion_event_stream_read_import(hREADER reader, ION_COLLECTION *imports, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_TYPE ion_type;
    ION_STRING field_name;
    BOOL is_null;
    ION_SYMBOL_TABLE_IMPORT *import = NULL;
    ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR descriptor;
    memset(&descriptor, 0, sizeof(ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR));
    descriptor.version = 1;
    descriptor.max_id = -1;

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_name_field, &field_name)) {
            if (is_null || ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor name must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &descriptor.name));
        }
        else if (ION_STRING_EQUALS(&ion_event_version_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_INT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor version must be an int.", result);
            }
            IONCREAD(ion_reader_read_int(reader, &descriptor.version));
        }
        else if (ION_STRING_EQUALS(&ion_event_max_id_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_INT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor max_id must be an int.", result);
            }
            IONCREAD(ion_reader_read_int(reader, &descriptor.max_id));
        }
        // Open content is ignored.
    }

    if (ION_STRING_IS_NULL(&descriptor.name)) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor name is required.", result);
    }

    import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(imports);
    import->shared_symbol_table = NULL;
    memcpy(&import->descriptor, &descriptor, sizeof(ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR));
    cRETURN;
}

iERR ion_event_stream_read_imports(hREADER reader, ION_COLLECTION **imports, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_TYPE ion_type;
    ASSERT(imports);

    *imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
    _ion_collection_initialize(*imports, *imports, sizeof (ION_SYMBOL_TABLE_IMPORT));

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        if (ion_type != tid_STRUCT) {
            IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor must be a struct.", result);
        }
        IONCREAD(ion_reader_step_in(reader));
        IONREPORT(ion_event_stream_read_import(reader, *imports, location, result));
        IONCREAD(ion_reader_step_out(reader));
    }
    cRETURN;
}

iERR ion_event_stream_get_consensus_value(ION_CATALOG *catalog, std::string value_text, BYTE *value_binary,
                                          size_t value_binary_len, void **consensus_value, IonEventResult *result) {
    iENTER;
    IonEventStream binary_stream("Binary scalar"), text_stream("Text scalar");
    ASSERT(!value_text.empty());
    ASSERT(value_binary);
    ASSERT(consensus_value);

    IONREPORT(read_value_stream_from_bytes(value_binary, (SIZE)value_binary_len, &binary_stream, catalog, result));
    IONREPORT(read_value_stream_from_bytes((BYTE *)value_text.c_str(), (SIZE)value_text.length(), &text_stream, catalog, result));

    if (assertIonEventStreamEq(&binary_stream, &text_stream, result)) {
        // Because the last event is always STREAM_END, the second-to-last event contains the scalar value.
        // NOTE: an IonEvent's value is freed during destruction of the event's IonEventStream. Since these event streams
        // are temporary, the value needs to be copied out.
        IONREPORT(ion_event_copy_value(binary_stream.at(binary_stream.size() - 2), consensus_value, result));
    }
    else {
        std::string message;
        if (result->has_comparison_result) {
            ION_EVENT_WRITER_CONTEXT writer_context;
            BYTE *comparison_result_str;
            SIZE comparison_result_str_len;
            IONREPORT(ion_event_in_memory_writer_open(&writer_context, ION_WRITER_OUTPUT_TYPE_TEXT_PRETTY, NULL, NULL));
            IONREPORT(ion_event_stream_write_comparison_result(writer_context.writer, &result->comparison_result));
            IONREPORT(ion_event_in_memory_writer_close(&writer_context, &comparison_result_str, &comparison_result_str_len));
            if (comparison_result_str) {
                message = std::string((char *) comparison_result_str, (size_t) comparison_result_str_len);
                free(comparison_result_str);
            }
        }
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event; text and binary scalar representations are not equal: " + message, result);
    }

    cRETURN;
}

iERR ion_event_stream_read_event(hREADER reader, IonEventStream *stream, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    // Variables that describe the current reader state.
    BOOL is_null;
    ION_STRING field_name;
    IonEvent *event = NULL;
    ION_TYPE ion_type;
    // Variables that describe the members of the IonEvent struct being read.
    ION_STRING value_ion_type_str, value_event_type_str;
    ION_TYPE value_ion_type = tid_none;
    ION_SYMBOL value_field_name;
    ION_SYMBOL *p_value_field_name = NULL;
    ION_EVENT_TYPE value_event_type = UNKNOWN;
    int value_depth = -1;
    ION_COLLECTION *value_imports = NULL;
    ION_SYMBOL value_annotation;
    std::vector<ION_SYMBOL> value_annotations;
    ION_SYMBOL *p_value_annotations = NULL;
    int value_binary_byte;
    std::vector<BYTE> value_binary;
    ION_STRING value_text_str;
    std::string value_text;
    void *consensus_value = NULL;

    // TODO error on repeated fields

    ION_SYMBOL_INIT(&value_field_name);
    ION_STRING_INIT(&value_text_str);

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_event_type_field, &field_name)) {
            if (is_null || ion_type != tid_SYMBOL) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: invalid event type.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &value_event_type_str));
            if (!ION_STRING_IS_NULL(&value_event_type_str)) {
                value_event_type = ion_event_type_from_string(&value_event_type_str);
            }
        }
        else if (ION_STRING_EQUALS(&ion_event_ion_type_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_SYMBOL) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: invalid Ion type.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &value_ion_type_str));
            if (!ION_STRING_IS_NULL(&value_ion_type_str)) {
                value_ion_type = ion_event_ion_type_from_string(&value_ion_type_str);
            }
        }
        else if (ION_STRING_EQUALS(&ion_event_imports_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_LIST) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: imports must be a list.", result);
            }
            IONCREAD(ion_reader_step_in(reader));
            IONREPORT(ion_event_stream_read_imports(reader, &value_imports, &stream->location, result));
            IONCREAD(ion_reader_step_out(reader));
        }
        else if (ION_STRING_EQUALS(&ion_event_depth_field, &field_name)) {
            if (is_null || ion_type != tid_INT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: depth must be an int.", result);
            }
            IONCREAD(ion_reader_read_int(reader, &value_depth));
        }
        else if (ION_STRING_EQUALS(&ion_event_value_text_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: value_text must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &value_text_str));
            ASSERT(!ION_STRING_IS_NULL(&value_text_str));
            value_text = std::string((char *)value_text_str.value, (size_t)value_text_str.length);
        }
        else if (ION_STRING_EQUALS(&ion_event_value_binary_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_LIST) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: value_binary must be a list of ints.", result);
            }
            IONCREAD(ion_reader_step_in(reader));
            for (;;) {
                IONCREAD(ion_reader_next(reader, &ion_type));
                if (ion_type == tid_EOF) break;
                if (ion_type != tid_INT) {
                    IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: value_binary must be a list of ints.", result);
                }
                IONCREAD(ion_reader_read_int(reader, &value_binary_byte));
                value_binary.push_back((BYTE)value_binary_byte);
            }
            IONCREAD(ion_reader_step_out(reader));
        }
        else if (ION_STRING_EQUALS(&ion_event_field_name_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_STRUCT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: field_name must be a SymbolToken struct.", result);
            }
            IONCREAD(ion_reader_step_in(reader));
            IONREPORT(ion_event_stream_read_symbol_token(reader, &value_field_name, &stream->location, result));
            IONCREAD(ion_reader_step_out(reader));
            p_value_field_name = &value_field_name;
        }
        else if (ION_STRING_EQUALS(&ion_event_annotations_field, &field_name)) {
            if (is_null) {
                continue;
            }
            if (ion_type != tid_LIST) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: annotations must be a list of SymbolToken structs.", result);
            }
            IONCREAD(ion_reader_step_in(reader));
            for (;;) {
                IONCREAD(ion_reader_next(reader, &ion_type));
                if (ion_type == tid_EOF) break;
                IONCREAD(ion_reader_is_null(reader, &is_null));
                if (is_null || ion_type != tid_STRUCT) {
                    IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: annotations must be a list of SymbolToken structs.", result);
                }
                IONCREAD(ion_reader_step_in(reader));
                IONREPORT(ion_event_stream_read_symbol_token(reader, &value_annotation, &stream->location, result));
                value_annotations.push_back(value_annotation);
                IONCREAD(ion_reader_step_out(reader));
            }
            IONCREAD(ion_reader_step_out(reader));
            p_value_annotations = &value_annotations[0];
        }
        // NOTE: there is no need to fail in the else{} case. Open content is ignored.
    }

    if (value_event_type == UNKNOWN || value_depth < 0) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: unknown event_type.", result);
    }
    if (value_depth < 0) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: depth less than zero.", result);
    }
    if (value_ion_type == tid_none && value_event_type != STREAM_END && value_event_type != SYMBOL_TABLE) {
        FAILWITHMSG(IERR_INVALID_ARG, "Invalid event: ion_type was required but not found.");
    }
    if (value_binary.empty() ^ value_text.empty()) {
        FAILWITHMSG(IERR_INVALID_ARG, "Invalid event: value_text and value_binary must both be set.");
    }
    if (value_event_type == SCALAR) {
        IONREPORT(ion_event_stream_get_consensus_value(catalog, value_text, &value_binary[0], value_binary.size(),
                                                       &consensus_value, result));
    }
    else {
        if (!value_text.empty()) {
            IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: value_binary and value_text are only applicable for SCALAR events.", result);
        }
    }
    if (value_imports != NULL && value_event_type != SYMBOL_TABLE) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: imports must only be present with SYMBOL_TABLE events.", result);
    }
    if (value_annotations.size() > 0 && (value_event_type == CONTAINER_END || value_event_type == STREAM_END
                                 || value_event_type == SYMBOL_TABLE)) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: only SCALAR and CONTAINER_START events may have annotations.", result);
    }
    if (p_value_field_name && (value_event_type == CONTAINER_END || value_event_type == STREAM_END
                                     || value_event_type == SYMBOL_TABLE)) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: only SCALAR and CONTAINER_START events may have a field_name.", result);
    }
    event = stream->append_new(value_event_type, value_ion_type, p_value_field_name, p_value_annotations, (SIZE)value_annotations.size(), value_depth);
    if (value_event_type == SCALAR) {
        event->value = consensus_value;
    }
    else if (value_event_type == SYMBOL_TABLE) {
        event->value = value_imports;
    }
    // These are now owned by the IonEventStream and should not be freed locally.
    value_imports = NULL;
    consensus_value = NULL;

cleanup:
    if (consensus_value) {
        free_ion_event_value(consensus_value, value_ion_type, value_event_type);
    }
    if (value_imports) {
        ion_free_owner(value_imports);
    }
    iRETURN;
}

iERR ion_event_stream_read_all_events(hREADER reader, IonEventStream *stream, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    ION_TYPE ion_type;
    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        if (ion_type != tid_STRUCT) {
            IONFAILSTATE(IERR_INVALID_ARG, "The given Ion stream does not contain an event stream.", result);
        }
        IONCREAD(ion_reader_step_in(reader));
        IONREPORT(ion_event_stream_read_event(reader, stream, catalog, result));
        IONCREAD(ion_reader_step_out(reader));
    }
    cRETURN;
}

iERR read_value_stream_from_bytes(const BYTE *ion_string, SIZE len, IonEventStream *stream, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    hREADER      reader;
    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    if (catalog) {
        options.pcatalog = catalog;
    }
    ion_event_register_symbol_table_callback(&options, stream);

    IONCREAD(ion_reader_open_buffer(&reader, (BYTE *)ion_string, len, &options));
    IONREPORT(ion_event_stream_read_all(reader, stream, result));
cleanup:
    ION_NON_FATAL(ion_reader_close(reader), "Failed to close reader.");
    iRETURN;
}

iERR write_scalar(hWRITER writer, IonEvent *event) {
    iENTER;
    int tid = ION_TID_INT(event->ion_type);
    if (!event->value) {
        IONCHECK(ion_writer_write_typed_null(writer, event->ion_type));
        SUCCEED();
    }
    switch (tid) {
        case TID_BOOL:
            IONCHECK(ion_writer_write_bool(writer, *(BOOL *)event->value));
            break;
        case TID_POS_INT:
        case TID_NEG_INT:
            IONCHECK(ion_writer_write_ion_int(writer, (ION_INT *)event->value));
            break;
        case TID_FLOAT:
            IONCHECK(ion_writer_write_double(writer, *(double *)event->value));
            break;
        case TID_DECIMAL:
            IONCHECK(ion_writer_write_ion_decimal(writer, (ION_DECIMAL *)event->value));
            break;
        case TID_TIMESTAMP:
            IONCHECK(ion_writer_write_timestamp(writer, (ION_TIMESTAMP *)event->value));
            break;
        case TID_SYMBOL:
            IONCHECK(ion_writer_write_ion_symbol(writer, (ION_SYMBOL *)event->value));
            break;
        case TID_STRING:
            IONCHECK(ion_writer_write_string(writer, (ION_STRING *)event->value));
            break;
        case TID_CLOB:
            IONCHECK(ion_writer_write_clob(writer, ((ION_STRING *)event->value)->value, ((ION_STRING *)event->value)->length));
            break;
        case TID_BLOB:
            IONCHECK(ion_writer_write_blob(writer, ((ION_STRING *)event->value)->value, ((ION_STRING *)event->value)->length));
            break;
        case TID_NULL: // NOTE: null events can only have NULL values; this is handled before the switch.
        default:
            FAILWITH(IERR_INVALID_ARG);
    }
    iRETURN;
}

iERR write_event(hWRITER writer, IonEvent *event) {
    iENTER;
    if (event->field_name) {
        IONCHECK(ion_writer_write_field_name_symbol(writer, event->field_name));
    }
    if (event->num_annotations) {
        IONCHECK(ion_writer_write_annotation_symbols(writer, event->annotations, event->num_annotations));
    }
    switch (event->event_type) {
        case CONTAINER_START:
            IONCHECK(ion_writer_start_container(writer, event->ion_type));
            break;
        case CONTAINER_END:
            IONCHECK(ion_writer_finish_container(writer));
            break;
        case SCALAR:
            IONCHECK(write_scalar(writer, event));
            break;
        case SYMBOL_TABLE:
            IONCHECK(ion_writer_add_imported_tables(writer, (ION_COLLECTION *)event->value));
            break;
        case STREAM_END:
            break;
        default:
            FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_event_stream_write_all(hWRITER writer, IonEventStream *stream) {
    iENTER;
    for (size_t i = 0; i < stream->size(); i++) {
        IONCHECK(write_event(writer, stream->at(i)));
    }
    iRETURN;
}

iERR ion_event_stream_write_symbol_token(hWRITER writer, ION_SYMBOL *symbol) {
    iENTER;
    ASSERT(symbol);

    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_text_field));
    if (!ION_STRING_IS_NULL(&symbol->value)) {
        IONCHECK(ion_writer_write_string(writer, &symbol->value));
    }
    else {
        IONCHECK(ion_writer_write_null(writer));
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_import_location_field));
        IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_name_field));
        IONCHECK(ion_writer_write_string(writer, &symbol->import_location.name));
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_import_sid_field));
        IONCHECK(ion_writer_write_int(writer, symbol->import_location.location));
        IONCHECK(ion_writer_finish_container(writer));
    }
    IONCHECK(ion_writer_finish_container(writer));
    iRETURN;
}

iERR ion_event_stream_write_symbol_table_imports(hWRITER writer, ION_COLLECTION *imports) {
    iENTER;
    ION_COLLECTION_CURSOR import_cursor;
    ION_SYMBOL_TABLE_IMPORT *import;
    IONCHECK(ion_writer_start_container(writer, tid_LIST));
    ION_COLLECTION_OPEN(imports, import_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(import_cursor, import);
        if (!import) break;
        IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_import_name_field));
        IONCHECK(ion_writer_write_string(writer, &import->descriptor.name));
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_max_id_field));
        IONCHECK(ion_writer_write_int(writer, import->descriptor.max_id));
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_version_field));
        IONCHECK(ion_writer_write_int(writer, import->descriptor.version));
        IONCHECK(ion_writer_finish_container(writer));
    }
    ION_COLLECTION_CLOSE(import_cursor);
    IONCHECK(ion_writer_finish_container(writer));
    iRETURN;
}

iERR ion_event_stream_write_scalar_event(hWRITER writer, IonEvent *event, ION_COLLECTION *imports, ION_CATALOG *catalog) {
    iENTER;
    ION_EVENT_WRITER_CONTEXT text_context, binary_context;
    BYTE *text_value, *binary_value;
    SIZE text_len, binary_len;
    ION_STRING text_stream;

    IONCHECK(ion_event_in_memory_writer_open(&text_context, ION_WRITER_OUTPUT_TYPE_TEXT_UGLY, catalog, (event->ion_type == tid_SYMBOL) ? imports : NULL));
    IONCHECK(ion_event_in_memory_writer_open(&binary_context, ION_WRITER_OUTPUT_TYPE_BINARY, catalog, (event->ion_type == tid_SYMBOL) ? imports : NULL));
    IONCHECK(write_scalar(text_context.writer, event));
    IONCHECK(write_scalar(binary_context.writer, event));
    IONCHECK(ion_event_in_memory_writer_close(&text_context, &text_value, &text_len));
    IONCHECK(ion_event_in_memory_writer_close(&binary_context, &binary_value, &binary_len));

    ion_string_assign_cstr(&text_stream, (char *)text_value, text_len);
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_value_text_field));
    IONCHECK(ion_writer_write_string(writer, &text_stream));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_value_binary_field));
    IONCHECK(ion_writer_start_container(writer, tid_LIST));
    for (int i = 0; i < binary_len; i++) {
        IONCHECK(ion_writer_write_int(writer, binary_value[i]));
    }
    IONCHECK(ion_writer_finish_container(writer));

cleanup:
    // TODO need to free unconditionally
    if (text_value) {
        free(text_value);
    }
    if (binary_value) {
        free(binary_value);
    }
    iRETURN;
}

iERR ion_event_stream_write_event(hWRITER writer, IonEvent *event, ION_CATALOG *catalog) {
    iENTER;
    ION_COLLECTION *imports = NULL;
    ION_STRING *ion_type_str = NULL;
    ION_STRING *event_type_str = ion_event_type_to_string(event->event_type);
    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_event_type_field));
    IONCHECK(ion_writer_write_symbol(writer, event_type_str));
    if (event->ion_type != tid_none) {
        ion_type_str = ion_event_ion_type_to_string(event->ion_type);
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_ion_type_field));
        IONCHECK(ion_writer_write_symbol(writer, ion_type_str));
    }
    if (event->field_name) {
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_field_name_field));
        IONCHECK(ion_event_stream_write_symbol_token(writer, event->field_name));
    }
    if (event->num_annotations > 0) {
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_annotations_field));
        IONCHECK(ion_writer_start_container(writer, tid_LIST));
        for (int j = 0; j < event->num_annotations; j++) {
            IONCHECK(ion_event_stream_write_symbol_token(writer, &event->annotations[j]));
        }
        IONCHECK(ion_writer_finish_container(writer));
    }
    if (event->event_type == SYMBOL_TABLE) {
        imports = (ION_COLLECTION *)event->value;
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_imports_field));
        IONCHECK(ion_event_stream_write_symbol_table_imports(writer, imports));
    }
    else if (event->event_type == SCALAR){
        IONCHECK(ion_event_stream_write_scalar_event(writer, event, imports, catalog));
    }
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_depth_field));
    IONCHECK(ion_writer_write_int(writer, event->depth));
    IONCHECK(ion_writer_finish_container(writer));
    iRETURN;
}

iERR ion_event_stream_write_all_events(hWRITER writer, IonEventStream *stream, ION_CATALOG *catalog) {
    iENTER;
    for (size_t i = 0; i < stream->size(); i++) {
        IONCHECK(ion_event_stream_write_event(writer, stream->at(i), catalog));
    }
    iRETURN;
}

iERR ion_event_stream_write_error(hWRITER writer, ION_EVENT_ERROR_DESCRIPTION *error_description) {
    iENTER;
    ION_STRING message, location;
    ASSERT(error_description);
    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_error_type_field));
    IONCHECK(ion_writer_write_symbol(writer, ion_event_error_type_to_string(error_description->error_type)));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_error_message_field));
    ION_EVENT_ION_STRING_FROM_STRING(&message, error_description->message);
    IONCHECK(ion_writer_write_string(writer, &message));
    if (error_description->has_location) {
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_error_location_field));
        ION_EVENT_ION_STRING_FROM_STRING(&location, error_description->location);
        IONCHECK(ion_writer_write_string(writer, &location));
    }
    if (error_description->has_event_index) {
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_error_event_index_field));
        IONCHECK(ion_writer_write_int(writer, (int)error_description->event_index));
    }
    IONCHECK(ion_writer_finish_container(writer));
    iRETURN;
}

iERR ion_event_stream_write_comparison_context(hWRITER writer, ION_EVENT_COMPARISON_CONTEXT *comparison_context) {
    iENTER;
    ION_STRING location;
    ASSERT(comparison_context);
    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_context_location_field));
    ION_EVENT_ION_STRING_FROM_STRING(&location, comparison_context->location);
    IONCHECK(ion_writer_write_string(writer, &location));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_context_event_field));
    IONCHECK(ion_event_stream_write_event(writer, comparison_context->event, NULL)); // TODO what about catalog?
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_context_event_index_field));
    IONCHECK(ion_writer_write_int(writer, (int)comparison_context->event_index));
    IONCHECK(ion_writer_finish_container(writer));
    iRETURN;
}

iERR ion_event_stream_write_comparison_result(hWRITER writer, ION_EVENT_COMPARISON_RESULT *comparison_result) {
    iENTER;
    ION_STRING message;
    ASSERT(comparison_result);
    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_result_type_field));
    IONCHECK(ion_writer_write_string(writer, ion_event_comparison_result_type_to_string(comparison_result->result)));
    if (!comparison_result->message.empty()) {
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_result_message_field));
        ION_EVENT_ION_STRING_FROM_STRING(&message, comparison_result->message);
        IONCHECK(ion_writer_write_string(writer, &message));
    }
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_result_lhs_field));
    IONCHECK(ion_event_stream_write_comparison_context(writer, &comparison_result->lhs));
    IONCHECK(ion_writer_write_field_name(writer, &ion_event_comparison_result_rhs_field));
    IONCHECK(ion_event_stream_write_comparison_context(writer, &comparison_result->rhs));
    IONCHECK(ion_writer_finish_container(writer));
    iRETURN;
}
