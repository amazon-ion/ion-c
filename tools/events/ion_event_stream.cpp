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

void copy_ion_string_into(ION_STRING *copy, ION_STRING *src) {
    size_t len = (size_t)src->length;
    if (src->value == NULL) {
        ASSERT(len == 0);
        copy->value = NULL;
    }
    else {
        copy->value = (BYTE *)malloc(len);
        memcpy(copy->value, src->value, len);
    }
    copy->length = (int32_t)len;
}

ION_STRING *copy_ion_string(ION_STRING *src) {
    ION_STRING *string_value = (ION_STRING *)malloc(sizeof(ION_STRING));
    copy_ion_string_into(string_value, src);
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
    this->event_type = event_type;
    this->ion_type = ion_type;
    copy_ion_symbol(&this->field_name, field_name);
    copy_ion_symbols(&this->annotations, annotations, (size_t)num_annotations);
    this->num_annotations = num_annotations;
    this->depth = depth;
    value = NULL;
}

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

iERR IonEventReport::writeComparisonResultsTo(hWRITER writer, std::string *location, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    for (size_t i = 0; i < comparison_report.size(); i++) {
        IONREPORT(ion_event_stream_write_comparison_result(writer, &comparison_report.at(i), location, catalog, result));
    }
    cRETURN;
}

iERR ion_event_stream_write_error_report(hWRITER writer, IonEventReport *report, std::string *location, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ASSERT(report);
    IONREPORT(report->writeErrorsTo(writer));
    cRETURN;
}

iERR ion_event_stream_write_comparison_report(hWRITER writer, IonEventReport *report, std::string *location, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ASSERT(report);
    IONREPORT(report->writeComparisonResultsTo(writer, location, catalog, result));
    cRETURN;
}

size_t valueEventLength(IonEventStream *stream, size_t start_index) {
    size_t length = 1;
    IonEvent *start = stream->at(start_index);
    if (CONTAINER_START == start->event_type) {
        size_t i = start_index;
        while (++i < stream->size()) {
            IonEvent *curr = stream->at(i);
            if (curr->event_type == CONTAINER_END && curr->depth == start->depth) {
                length = ++i - start_index;
                break;
            }
        }
    }
    return length;
}

iERR ion_event_stream_read_embedded_stream(hREADER reader, IonEventStream *stream, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    ION_STRING embedded_stream;

    IONCREAD(ion_reader_read_string(reader, &embedded_stream));
    if (ION_STRING_IS_NULL(&embedded_stream)) {
        IONFAILSTATE(IERR_INVALID_ARG, "Embedded streams must not be null.", result);
    }
    // NOTE: this means the embedded stream will use the same catalog as the outer reader. If the embedded streams
    // contain shared symbol table imports, those shared symbol tables should be made available in that reader's
    // catalog.
    IONREPORT(read_value_stream_from_bytes(embedded_stream.value, embedded_stream.length, stream, reader->_catalog, result));
    cRETURN;
}

iERR _ion_event_stream_read_all_recursive(hREADER hreader, IonEventStream *stream, BOOL in_struct, int depth, BOOL is_embedded_stream_set, IonEventResult *result);
/**
 * Reads the reader's current value into an IonEvent and appends that event to the given IonEventStream. The event's
 * value is allocated such that it may be freed safely by free_ion_event_value.
 */
iERR ion_event_stream_read(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth, BOOL is_embedded_stream_set, IonEventResult *result) {
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
    if (is_embedded_stream_set) {
        if (ion_type != TID_STRING) {
            IONFAILSTATE(IERR_INVALID_ARG, "Elements of embedded streams sets must be strings.", result);
        }
        IONREPORT(ion_event_stream_read_embedded_stream(hreader, stream, result));
        IONCLEANEXIT;
    }
    else if (is_null) {
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
            ION_STRING string_value;
            IONCREAD(ion_reader_read_string(hreader, &string_value));
            event->value = copy_ion_string(&string_value);
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
        case TID_SEXP: // intentional fall-through
        case TID_LIST:
            is_embedded_stream_set = depth == 0 && annotation_count > 0 && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, &annotations[0].value);
            // intentional fall-through
        case TID_STRUCT:
            IONCREAD(ion_reader_step_in(hreader));
            IONREPORT(_ion_event_stream_read_all_recursive(hreader, stream, t == tid_STRUCT, depth + 1, is_embedded_stream_set, result));
            IONCREAD(ion_reader_step_out(hreader));
            stream->append_new(CONTAINER_END, t, /*field_name=*/NULL, /*annotations=*/NULL, /*num_annotations=*/0,
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

iERR _ion_event_stream_read_all_recursive(hREADER hreader, IonEventStream *stream, BOOL in_struct, int depth, BOOL is_embedded_stream_set, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    ION_TYPE t;
    for (;;) {
        IONCREAD(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            break;
        }
        IONREPORT(ion_event_stream_read(hreader, stream, t, in_struct, depth, is_embedded_stream_set, result));
    }
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

iERR ion_event_stream_read_all_values(hREADER hreader, IonEventStream *stream, IonEventResult *result) {
    iENTER;
    IONREPORT(_ion_event_stream_read_all_recursive(hreader, stream, /*in_struct=*/FALSE, /*depth=*/0, /*is_embedded_stream_set=*/FALSE, result));
    stream->append_new(STREAM_END, tid_none, NULL, NULL, 0, 0);
    cRETURN;
}


#define ION_EVENT_REQUIRE_UNIQUE_FIELD(fields, mask, name) \
    if ((fields) & (mask)) { \
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: repeated " + std::string(name) + " field.", result); \
    } \
    (fields) |= (mask);

iERR ion_event_stream_read_import_location(hREADER reader, ION_SYMBOL_IMPORT_LOCATION *import_location, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_TYPE ion_type;
    ION_STRING field_name, location_name;
    BOOL is_null;
    uint8_t visited_fields = 0;
    ASSERT(import_location);

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_import_name_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x1, "import_name");
            if (is_null || ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportLocation import_name must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &location_name));
            copy_ion_string_into(&import_location->name, &location_name);
        }
        else if (ION_STRING_EQUALS(&ion_event_import_sid_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x2, "import_sid");
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
    ION_STRING field_name, text;
    BOOL is_null;
    uint8_t visited_fields = 0;
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
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x1, "text");
            if (is_null) {
                continue;
            }
            if (ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: SymbolToken text must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &text));
            copy_ion_string_into(&symbol->value, &text);
        }
        else if (ION_STRING_EQUALS(&ion_event_import_location_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x2, "import_location");
            if (is_null) {
                continue;
            }
            if (ion_type != tid_STRUCT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: SymbolToken import_location must be an ImportLocation struct.", result);
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
    ION_STRING field_name, import_name;
    BOOL is_null;
    ION_SYMBOL_TABLE_IMPORT *import = NULL;
    ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR descriptor;
    memset(&descriptor, 0, sizeof(ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR));
    descriptor.version = 1;
    descriptor.max_id = -1;
    uint8_t visited_fields = 0;

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_name_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x1, "name");
            if (is_null || ion_type != tid_STRING) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor name must be a string.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &import_name));
            IONCREAD(ion_string_copy_to_owner(imports, &descriptor.name, &import_name));
        }
        else if (ION_STRING_EQUALS(&ion_event_version_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x2, "version");
            if (is_null) {
                continue;
            }
            if (ion_type != tid_INT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor version must be an int.", result);
            }
            IONCREAD(ion_reader_read_int(reader, &descriptor.version));
        }
        else if (ION_STRING_EQUALS(&ion_event_max_id_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x4, "max_id");
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

iERR ion_event_stream_read_imports(hREADER reader, ION_COLLECTION *imports, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_TYPE ion_type;
    ASSERT(imports);

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        if (ion_type != tid_STRUCT) {
            IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: ImportDescriptor must be a struct.", result);
        }
        IONCREAD(ion_reader_step_in(reader));
        IONREPORT(ion_event_stream_read_import(reader, imports, location, result));
        IONCREAD(ion_reader_step_out(reader));
    }
    cRETURN;
}

iERR ion_event_stream_write_scalar_value_comparison_result(std::string location, std::string *comparison_report, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&location, NULL);
    IonEventWriterContext writer_context;
    BYTE *value = NULL;
    SIZE len;
    ASSERT(comparison_report);

    IONREPORT(ion_event_in_memory_writer_open(&writer_context, location, OUTPUT_TYPE_TEXT_PRETTY, NULL, NULL, result));
    IONREPORT(ion_event_stream_write_comparison_result(writer_context.writer, &result->comparison_result, &location, catalog, result));
cleanup:
    UPDATEERROR(ion_event_in_memory_writer_close(&writer_context, &value, &len, err, result));
    if (value) {
        *comparison_report = std::string((char *)value, (size_t)len);
        free(value);
    }
    iRETURN;
}

ION_SYMBOL *ion_event_stream_new_ivm_symbol() {
    ION_SYMBOL *ivm_symbol = (ION_SYMBOL *)calloc(1, sizeof(ION_SYMBOL));
    ivm_symbol->value.value = (BYTE *)malloc(sizeof(BYTE) * ION_SYS_STRLEN_IVM);
    memcpy(ivm_symbol->value.value, (BYTE *)ION_SYS_SYMBOL_IVM, ION_SYS_STRLEN_IVM);
    ivm_symbol->value.length = ION_SYS_STRLEN_IVM;
    ivm_symbol->sid = UNKNOWN_SID; // Not needed; text is known.
    return ivm_symbol;
}

/**
 * Copies the given SCALAR event's value so that it may be used outside the scope of the event's owning stream. The
 * copied value is allocated such that it may be freed safely by free_ion_event_value.
 */
iERR ion_event_copy_value(IonEvent *event, void **value, std::string location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&location, NULL);
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

iERR ion_event_copy(IonEvent **dst, IonEvent *src, std::string location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&location, NULL);
    ASSERT(dst != NULL);
    ASSERT(*dst == NULL);
    ASSERT(src != NULL);
    if (src->event_type == SYMBOL_TABLE) {
        IONFAILSTATE(IERR_INVALID_ARG, "Cannot copy a SYMBOL_TABLE event.", result);
    }
    *dst = new IonEvent(src->event_type, src->ion_type, src->field_name, src->annotations, src->num_annotations, src->depth);
    if (src->event_type == SCALAR) {
        IONREPORT(ion_event_copy_value(src, &(*dst)->value, location, result));
    }
    cRETURN;
}

iERR ion_event_stream_get_consensus_value(std::string location, ION_CATALOG *catalog, std::string value_text, BYTE *value_binary,
                                          size_t value_binary_len, void **consensus_value, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&location, NULL);
    IonEventStream binary_stream(location + " binary scalar"), text_stream(location + " text scalar");
    ASSERT(consensus_value);

    if (value_binary && value_binary_len > 0) {
        IONREPORT(read_value_stream_from_bytes(value_binary, (SIZE) value_binary_len, &binary_stream, catalog, result));
    }
    if (!value_text.empty()) {
        IONREPORT(read_value_stream_from_bytes((BYTE *) value_text.c_str(), (SIZE) value_text.length(), &text_stream,
                                               catalog, result));
    }

    if (assertIonEventStreamEq(&binary_stream, &text_stream, result)) {
        // Because the last event is always STREAM_END, the second-to-last event contains the scalar value.
        // NOTE: an IonEvent's value is freed during destruction of the event's IonEventStream. Since these event streams
        // are temporary, the value needs to be copied out.
        if (binary_stream.size() > 1) {
            IONREPORT(ion_event_copy_value(binary_stream.at(binary_stream.size() - 2), consensus_value, location,
                                           result));
        }
        else {
            ASSERT(binary_stream.size() == 0 || binary_stream.at(0)->event_type == STREAM_END);
            ASSERT(text_stream.size() == 0 || text_stream.at(0)->event_type == STREAM_END);
            // NOTE: the only value that can produce an empty value_binary and value_text is the IVM symbol.
            *consensus_value = ion_event_stream_new_ivm_symbol();
        }
    }
    else {
        std::string message;
        if (result->has_comparison_result) {
            IONREPORT(ion_event_stream_write_scalar_value_comparison_result(value_text + " scalar comparison result", &message, catalog, result));
        }
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid event; text and binary scalar representations are not equal. " + message, result);
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
    uint8_t visited_fields = 0;

    ION_SYMBOL_INIT(&value_field_name);
    ION_STRING_INIT(&value_text_str);

    for (;;) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) break;
        IONCREAD(ion_reader_get_field_name(reader, &field_name));
        IONCREAD(ion_reader_is_null(reader, &is_null));
        if (ION_STRING_EQUALS(&ion_event_event_type_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x1, "event_type");
            if (is_null || ion_type != tid_SYMBOL) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: invalid event type.", result);
            }
            IONCREAD(ion_reader_read_string(reader, &value_event_type_str));
            if (!ION_STRING_IS_NULL(&value_event_type_str)) {
                value_event_type = ion_event_type_from_string(&value_event_type_str);
            }
        }
        else if (ION_STRING_EQUALS(&ion_event_ion_type_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x2, "ion_type");
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
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x4, "imports");
            if (is_null) {
                continue;
            }
            if (ion_type != tid_LIST) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: imports must be a list.", result);
            }
            IONCREAD(ion_reader_step_in(reader));
            value_imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
            _ion_collection_initialize(value_imports, value_imports, sizeof (ION_SYMBOL_TABLE_IMPORT));
            IONREPORT(ion_event_stream_read_imports(reader, value_imports, &stream->location, result));
            IONCREAD(ion_reader_step_out(reader));
        }
        else if (ION_STRING_EQUALS(&ion_event_depth_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x8, "depth");
            if (is_null || ion_type != tid_INT) {
                IONFAILSTATE(IERR_INVALID_ARG, "Invalid event: depth must be an int.", result);
            }
            IONCREAD(ion_reader_read_int(reader, &value_depth));
        }
        else if (ION_STRING_EQUALS(&ion_event_value_text_field, &field_name)) {
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x10, "value_text");
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
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x20, "value_binary");
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
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x40, "field_name");
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
            ION_EVENT_REQUIRE_UNIQUE_FIELD(visited_fields, 0x80, "annotations");
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
        IONREPORT(ion_event_stream_get_consensus_value(stream->location, catalog, value_text, &value_binary[0], value_binary.size(),
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
    if (!value_annotations.empty() && (value_event_type == CONTAINER_END || value_event_type == STREAM_END
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

iERR ion_event_stream_is_event_stream(hREADER reader, IonEventStream *stream, bool *is_event_stream, bool *has_more_events, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, NULL);
    ION_TYPE ion_type;
    ION_SYMBOL *symbol_value = NULL;
    IonEvent *event;
    size_t i = 0;
    ASSERT(is_event_stream);

    *is_event_stream = FALSE;
    *has_more_events = TRUE;
    for (;; i++) {
        IONCREAD(ion_reader_next(reader, &ion_type));
        if (ion_type == tid_EOF) {
            IONCLEANEXIT;
        }
        IONREPORT(ion_event_stream_read(reader, stream, ion_type, FALSE, 0, FALSE, result));
        ASSERT(stream->size() > 0);
        event = stream->at(i);
        if (event->event_type == SYMBOL_TABLE) {
            // It's unlikely, but event streams could be serialized with imports. If this is true, skip to the next
            // event.
            continue;
        }
        if (event->event_type == SCALAR && event->ion_type == tid_SYMBOL
            && event->num_annotations == 0 && event->depth == 0) {
            symbol_value = (ION_SYMBOL *) event->value;
            if (!ION_SYMBOL_IS_NULL(symbol_value)
                && ION_STRING_EQUALS(&ion_event_stream_marker, &symbol_value->value)) {
                *is_event_stream = TRUE;
                stream->remove(i); // Toss this event -- it's not part of the user data.
            }
        }
        else if (event->event_type == STREAM_END) {
            *has_more_events = FALSE;
        }
        break;
    }
    cRETURN;
}

iERR ion_event_stream_read_all(hREADER reader, ION_CATALOG *catalog, IonEventStream *stream, IonEventResult *result) {
    iENTER;
    bool is_event_stream, has_more_events;

    IONREPORT(ion_event_stream_is_event_stream(reader, stream, &is_event_stream, &has_more_events, result));
    if (has_more_events) {
        if (is_event_stream) {
            IONREPORT(ion_event_stream_read_all_events(reader, stream, catalog, result));
        }
        else {
            IONREPORT(ion_event_stream_read_all_values(reader, stream, result));
        }
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
    IONREPORT(ion_event_stream_read_all(reader, catalog, stream, result));
cleanup:
    ION_NON_FATAL(ion_reader_close(reader), "Failed to close reader.");
    iRETURN;
}

iERR write_scalar(hWRITER writer, IonEvent *event, std::string *location, size_t *index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, index);
    int tid = ION_TID_INT(event->ion_type);
    if (!event->value) {
        IONCWRITE(ion_writer_write_typed_null(writer, event->ion_type));
        IONCLEANEXIT;
    }
    switch (tid) {
        case TID_BOOL:
            IONCWRITE(ion_writer_write_bool(writer, *(BOOL *)event->value));
            break;
        case TID_POS_INT:
        case TID_NEG_INT:
            IONCWRITE(ion_writer_write_ion_int(writer, (ION_INT *)event->value));
            break;
        case TID_FLOAT:
            IONCWRITE(ion_writer_write_double(writer, *(double *)event->value));
            break;
        case TID_DECIMAL:
            IONCWRITE(ion_writer_write_ion_decimal(writer, (ION_DECIMAL *)event->value));
            break;
        case TID_TIMESTAMP:
            IONCWRITE(ion_writer_write_timestamp(writer, (ION_TIMESTAMP *)event->value));
            break;
        case TID_SYMBOL:
            IONCWRITE(ion_writer_write_ion_symbol(writer, (ION_SYMBOL *)event->value));
            break;
        case TID_STRING:
            IONCWRITE(ion_writer_write_string(writer, (ION_STRING *)event->value));
            break;
        case TID_CLOB:
            IONCWRITE(ion_writer_write_clob(writer, ((ION_STRING *)event->value)->value, ((ION_STRING *)event->value)->length));
            break;
        case TID_BLOB:
            IONCWRITE(ion_writer_write_blob(writer, ((ION_STRING *)event->value)->value, ((ION_STRING *)event->value)->length));
            break;
        case TID_NULL: // NOTE: null events can only have NULL values; this is handled before the switch.
        default:
            IONFAILSTATE(IERR_INVALID_ARG, "Invalid ion_type", result);
    }
    cRETURN;
}
// TODO pass down object with location, index, and result?
iERR write_event(hWRITER writer, IonEvent *event, std::string *location, size_t index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, &index);
    if (event->field_name) {
        IONCWRITE(ion_writer_write_field_name_symbol(writer, event->field_name));
    }
    if (event->num_annotations) {
        IONCWRITE(ion_writer_write_annotation_symbols(writer, event->annotations, event->num_annotations));
    }
    switch (event->event_type) {
        case CONTAINER_START:
            IONCWRITE(ion_writer_start_container(writer, event->ion_type));
            break;
        case CONTAINER_END:
            IONCWRITE(ion_writer_finish_container(writer));
            break;
        case SCALAR:
            IONCWRITE(write_scalar(writer, event, location, &index, result));
            break;
        case SYMBOL_TABLE:
            IONCWRITE(ion_writer_add_imported_tables(writer, (ION_COLLECTION *)event->value));
            break;
        case STREAM_END:
            break;
        default:
            IONFAILSTATE(IERR_INVALID_ARG, "Invalid event_type.", result);
    }
    cRETURN;
}

iERR _ion_event_stream_write_all_recursive(hWRITER writer, IonEventStream *stream, size_t start_index, size_t end_index, IonEventResult *result);

iERR _ion_event_stream_write_all_to_bytes_helper(IonEventStream *stream, size_t start_index, size_t end_index,
                                                 ION_EVENT_OUTPUT_TYPE output_type, ION_CATALOG *catalog, BYTE **out,
                                                 SIZE *len, IonEventResult *result) {
    iENTER;
    IonEventWriterContext writer_context;
    IONREPORT(ion_event_in_memory_writer_open(&writer_context, stream->location, output_type, catalog, /*imports=*/NULL, result));
    IONREPORT(_ion_event_stream_write_all_recursive(writer_context.writer, stream, start_index, end_index, result));
cleanup:
    UPDATEERROR(ion_event_in_memory_writer_close(&writer_context, out, len, err));
    iRETURN;
}

/**
 * Constructs a writer using the given test type and catalog and uses it to write the given IonEventStream to BYTEs.
 */
iERR ion_event_stream_write_all_to_bytes(IonEventStream *stream, ION_EVENT_OUTPUT_TYPE output_type,
                                         ION_CATALOG *catalog, BYTE **out, SIZE *len, IonEventResult *result) {
    iENTER;
    IONREPORT(_ion_event_stream_write_all_to_bytes_helper(stream, 0, stream->size(), output_type, catalog, out, len, result));
    cRETURN;
}

iERR ion_event_stream_write_embedded_stream(hWRITER writer, IonEventStream *stream, size_t start_index, size_t end_index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&stream->location, &start_index);
    ION_STRING embedded_string;
    ION_STRING_INIT(&embedded_string);

    IONREPORT(_ion_event_stream_write_all_to_bytes_helper(stream, start_index, end_index,
                                                          OUTPUT_TYPE_TEXT_UGLY, writer->pcatalog,
                                                          &embedded_string.value, &embedded_string.length,
                                                          result));
    IONCWRITE(ion_writer_write_string(writer, &embedded_string));
cleanup:
    if (embedded_string.value) {
        free(embedded_string.value);
    }
    iRETURN;
}

size_t ion_event_stream_length(IonEventStream *stream, size_t index) {
    // NOTE: this will break if embedded streams themselves contain embedded streams. This should be explicitly
    // disallowed, as nested embedded streams are not a useful concept.
    size_t end_index = index;
    while (stream->size() - end_index > 0 && stream->at(end_index++)->event_type != STREAM_END);
    return  end_index - index;
}

size_t ion_event_embedded_set_length(IonEventStream *stream, size_t index) {
    IonEvent *event = stream->at(index);
    ASSERT(event->event_type == CONTAINER_START);
    size_t end_index = index;
    int target_depth = event->depth;
    BOOL active_stream = FALSE;
    while (++end_index < stream->size()) {
        event = stream->at(end_index);
        if (!active_stream && event->event_type == CONTAINER_END && event->depth == target_depth) {
            break;
        }
        active_stream = event->event_type != STREAM_END;
    }
    return end_index - index;
}

iERR _ion_event_stream_write_all_recursive(hWRITER writer, IonEventStream *stream, size_t start_index, size_t end_index, IonEventResult *result) {
    iENTER;
    size_t i = start_index;
    while (i < end_index) {
        IonEvent *event = stream->at(i);
        IONREPORT(write_event(writer, event, &stream->location, i, result));
        if (event->depth == 0 && event->event_type == CONTAINER_START && event->num_annotations > 0
            && (event->ion_type == tid_LIST || event->ion_type == tid_SEXP)
            && ION_STRING_EQUALS(&ion_event_embedded_streams_annotation, &event->annotations[0].value)) {
            size_t container_end = i + ion_event_embedded_set_length(stream, i);
            i++; // Skip past the CONTAINER_START
            while (i < container_end) {
                size_t stream_length = ion_event_stream_length(stream, i);
                IONREPORT(ion_event_stream_write_embedded_stream(writer, stream, i, i + stream_length, result));
                i += stream_length;
            }
            ASSERT(stream->at(i)->event_type == CONTAINER_END);
        }
        else {
            i++;
        }
    }
    cRETURN;
}

iERR ion_event_stream_write_all(hWRITER writer, IonEventStream *stream, IonEventResult *result) {
    iENTER;
    IONREPORT(_ion_event_stream_write_all_recursive(writer, stream, 0, stream->size(), result));
    cRETURN;
}

iERR ion_event_stream_write_symbol_token(hWRITER writer, ION_SYMBOL *symbol, std::string *location, size_t *index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, index);
    ASSERT(symbol);

    IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_text_field));
    if (!ION_STRING_IS_NULL(&symbol->value)) {
        IONCWRITE(ion_writer_write_string(writer, &symbol->value));
    }
    else {
        IONCWRITE(ion_writer_write_null(writer));
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_import_location_field));
        IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_import_name_field));
        IONCWRITE(ion_writer_write_string(writer, &symbol->import_location.name));
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_import_sid_field));
        IONCWRITE(ion_writer_write_int(writer, symbol->import_location.location));
        IONCWRITE(ion_writer_finish_container(writer));
    }
    IONCWRITE(ion_writer_finish_container(writer));
    cRETURN;
}

iERR ion_event_stream_write_symbol_table_imports(hWRITER writer, ION_COLLECTION *imports, std::string *location, size_t *index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, index);
    ION_COLLECTION_CURSOR import_cursor;
    ION_SYMBOL_TABLE_IMPORT *import;
    IONCWRITE(ion_writer_start_container(writer, tid_LIST));
    ION_COLLECTION_OPEN(imports, import_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(import_cursor, import);
        if (!import) break;
        IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_name_field));
        IONCWRITE(ion_writer_write_string(writer, &import->descriptor.name));
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_max_id_field));
        IONCWRITE(ion_writer_write_int(writer, import->descriptor.max_id));
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_version_field));
        IONCWRITE(ion_writer_write_int(writer, import->descriptor.version));
        IONCWRITE(ion_writer_finish_container(writer));
    }
    ION_COLLECTION_CLOSE(import_cursor);
    IONCWRITE(ion_writer_finish_container(writer));
    cRETURN;
}

iERR ion_event_stream_write_scalar_value(ION_EVENT_OUTPUT_TYPE output_type, IonEvent *event, ION_CATALOG *catalog, std::string location, size_t *index, BYTE **value, SIZE *len, IonEventResult *result) {
    iENTER;
    std::string scalar_location = location + ((output_type == OUTPUT_TYPE_BINARY) ? " binary scalar" : " text scalar");
    ION_SET_ERROR_CONTEXT(&scalar_location, index);
    IonEventWriterContext writer_context;
    ION_COLLECTION *imports = NULL;
    ASSERT(value);
    ASSERT(len);

    if (event->ion_type == tid_SYMBOL) {
        ION_SYMBOL *symbol = (ION_SYMBOL *)event->value;
        if (!ION_SYMBOL_IS_NULL(symbol) && ION_STRING_IS_NULL(&symbol->value)) {
            ASSERT(!ION_SYMBOL_IMPORT_LOCATION_IS_NULL(symbol));
            // This is a symbol with unknown text. Its shared symbol table must be included.
            imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
            _ion_collection_initialize(imports, imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
            ION_SYMBOL_TABLE_IMPORT *import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(imports);
            ION_STRING_ASSIGN(&import->descriptor.name, &symbol->import_location.name);
            import->descriptor.max_id = symbol->import_location.location + 1;
            import->descriptor.version = -1; // The highest version that matches the given name will be selected.
            import->shared_symbol_table = NULL;
            IONREPORT(ion_event_in_memory_writer_open(&writer_context, scalar_location, output_type, catalog, imports, result));
            IONREPORT(write_scalar(writer_context.writer, event, &scalar_location, index, result));
            IONCLEANEXIT;
        }
    }
    IONREPORT(ion_event_in_memory_writer_open(&writer_context, scalar_location, output_type, NULL, NULL, result));
    IONREPORT(write_scalar(writer_context.writer, event, &scalar_location, index, result));
cleanup:
    UPDATEERROR(ion_event_in_memory_writer_close(&writer_context, value, len, err, result));
    if (imports) {
        ion_free_owner(imports);
    }
    iRETURN;
}

iERR ion_event_stream_write_scalar_event(hWRITER writer, IonEvent *event, ION_CATALOG *catalog, std::string *location, size_t *index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, index);
    BYTE *text_value = NULL, *binary_value = NULL;
    SIZE text_len, binary_len;
    ION_STRING text_stream;
    ASSERT(location);

    IONREPORT(ion_event_stream_write_scalar_value(OUTPUT_TYPE_TEXT_UGLY, event, catalog, *location, index, &text_value, &text_len, result));
    IONREPORT(ion_event_stream_write_scalar_value(OUTPUT_TYPE_BINARY, event, catalog, *location, index, &binary_value, &binary_len, result));

    ion_string_assign_cstr(&text_stream, (char *)text_value, text_len);
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_value_text_field));
    IONCWRITE(ion_writer_write_string(writer, &text_stream));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_value_binary_field));
    IONCWRITE(ion_writer_start_container(writer, tid_LIST));
    for (int i = 0; i < binary_len; i++) {
        IONCWRITE(ion_writer_write_int(writer, binary_value[i]));
    }
    IONCWRITE(ion_writer_finish_container(writer));

cleanup:
    if (text_value) {
        free(text_value);
    }
    if (binary_value) {
        free(binary_value);
    }
    iRETURN;
}

iERR ion_event_stream_write_event(hWRITER writer, IonEvent *event, ION_CATALOG *catalog, std::string *location, size_t *index, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, index);
    ION_COLLECTION *imports = NULL;
    ION_STRING *ion_type_str = NULL;
    ION_STRING *event_type_str = ion_event_type_to_string(event->event_type);
    IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_event_type_field));
    IONCWRITE(ion_writer_write_symbol(writer, event_type_str));
    if (event->ion_type != tid_none) {
        ion_type_str = ion_event_ion_type_to_string(event->ion_type);
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_ion_type_field));
        IONCWRITE(ion_writer_write_symbol(writer, ion_type_str));
    }
    if (event->field_name) {
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_field_name_field));
        IONREPORT(ion_event_stream_write_symbol_token(writer, event->field_name, location, index, result));
    }
    if (event->num_annotations > 0) {
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_annotations_field));
        IONCWRITE(ion_writer_start_container(writer, tid_LIST));
        for (int j = 0; j < event->num_annotations; j++) {
            IONREPORT(ion_event_stream_write_symbol_token(writer, &event->annotations[j], location, index, result));
        }
        IONCWRITE(ion_writer_finish_container(writer));
    }
    if (event->event_type == SYMBOL_TABLE) {
        imports = (ION_COLLECTION *)event->value;
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_imports_field));
        IONREPORT(ion_event_stream_write_symbol_table_imports(writer, imports, location, index, result));
    }
    else if (event->event_type == SCALAR){
        IONREPORT(ion_event_stream_write_scalar_event(writer, event, catalog, location, index, result));
    }
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_depth_field));
    IONCWRITE(ion_writer_write_int(writer, event->depth));
    IONCWRITE(ion_writer_finish_container(writer));
    cRETURN;
}

iERR ion_event_stream_write_all_events(hWRITER writer, IonEventStream *stream, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    for (size_t i = 0; i < stream->size(); i++) {
        IONCHECK(ion_event_stream_write_event(writer, stream->at(i), catalog, &stream->location, &i, result));
    }
    iRETURN;
}

iERR ion_event_stream_write_error(hWRITER writer, IonEventErrorDescription *error_description) {
    iENTER;
    ION_SET_ERROR_CONTEXT(NULL, NULL); // In the event that writing the error report fails, this information can't be used anyway
    IonEventResult *result = NULL;
    ION_STRING message, location;
    ASSERT(error_description);
    IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_error_type_field));
    IONCWRITE(ion_writer_write_symbol(writer, ion_event_error_type_to_string(error_description->error_type)));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_error_message_field));
    ION_EVENT_ION_STRING_FROM_STRING(&message, error_description->message);
    IONCWRITE(ion_writer_write_string(writer, &message));
    if (error_description->has_location) {
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_error_location_field));
        ION_EVENT_ION_STRING_FROM_STRING(&location, error_description->location);
        IONCWRITE(ion_writer_write_string(writer, &location));
    }
    if (error_description->has_event_index) {
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_error_event_index_field));
        IONCWRITE(ion_writer_write_int(writer, (int)error_description->event_index));
    }
    IONCWRITE(ion_writer_finish_container(writer));
    cRETURN;
}

iERR ion_event_stream_write_comparison_context(hWRITER writer, IonEventComparisonContext *comparison_context, std::string *location, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_STRING event_location;
    ASSERT(comparison_context);
    IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_context_location_field));
    ION_EVENT_ION_STRING_FROM_STRING(&event_location, comparison_context->location);
    IONCWRITE(ion_writer_write_string(writer, &event_location));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_context_event_field));
    IONREPORT(ion_event_stream_write_event(writer, comparison_context->event, catalog, location, NULL, result));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_context_event_index_field));
    IONCWRITE(ion_writer_write_int(writer, (int)comparison_context->event_index));
    IONCWRITE(ion_writer_finish_container(writer));
    cRETURN;
}

iERR ion_event_stream_write_comparison_result(hWRITER writer, IonEventComparisonResult *comparison_result, std::string *location, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    ION_STRING message;
    ASSERT(comparison_result);
    IONCWRITE(ion_writer_start_container(writer, tid_STRUCT));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_result_type_field));
    IONCWRITE(ion_writer_write_string(writer, ion_event_comparison_result_type_to_string(comparison_result->result)));
    if (!comparison_result->message.empty()) {
        IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_result_message_field));
        ION_EVENT_ION_STRING_FROM_STRING(&message, comparison_result->message);
        IONCWRITE(ion_writer_write_string(writer, &message));
    }
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_result_lhs_field));
    IONREPORT(ion_event_stream_write_comparison_context(writer, &comparison_result->lhs, location, catalog, result));
    IONCWRITE(ion_writer_write_field_name(writer, &ion_event_comparison_result_rhs_field));
    IONREPORT(ion_event_stream_write_comparison_context(writer, &comparison_result->rhs, location, catalog, result));
    IONCWRITE(ion_writer_finish_container(writer));
    cRETURN;
}
