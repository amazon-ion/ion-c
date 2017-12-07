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
#include "ion_event_util.h"

#define IONCHECKORFREE(x, y) { \
    err = x; \
    if (err) { \
        free_ion_symbols(annotations, annotation_count); annotations = NULL; \
        free_ion_symbol(field_name); field_name = NULL; \
        if (y) free(y); y = NULL; \
        goto fail; \
    } \
} \

#define IONCHECKORFREE2(x) { \
    err = x; \
    if (err) { \
        free_ion_symbols(annotations, annotation_count); annotations = NULL; \
        free_ion_symbol(field_name); field_name = NULL; \
        goto fail; \
    } \
} \

void free_ion_string(ION_STRING *str) {
    if (str) {
        if (str->value) {
            free(str->value);
        }
        free(str);
    }
}

void free_ion_symbol(ION_SYMBOL *symbol) {
    if (symbol) {
        if (symbol->value.value) {
            free(symbol->value.value);
        }
        if (symbol->import_location.name.value) {
            free(symbol->import_location.name.value);
        }
        free(symbol);
    }
}

void free_ion_symbols(ION_SYMBOL **symbols, SIZE len) {
    if (symbols) {
        for (int q = 0; q < len; q++) {
            free_ion_symbol(symbols[q]);
        }
        free(symbols);
    }
}

IonEventStream::IonEventStream() {
    event_stream = new std::vector<IonEvent *>();
}

IonEventStream::~IonEventStream() {
    for (int i = 0; i < event_stream->size(); i++) {
        IonEvent *event = event_stream->at(i);
        if (event) {
            free_ion_symbols(event->annotations, event->num_annotations);
            free_ion_symbol(event->field_name);
            if (event->event_type == SYMBOL_TABLE) {
                ion_free_owner(event->value);
            }
            else if (event->value) {
                int tid = (int)(ION_TYPE_INT(event->ion_type) >> 8);
                switch (tid) {
                    case TID_POS_INT:
                    case TID_NEG_INT:
                        ion_int_free((ION_INT *)event->value);
                        break;
                    case TID_DECIMAL:
                        ion_decimal_free((ION_DECIMAL *) event->value);
                        break;
                    case TID_SYMBOL:
                        free_ion_symbol((ION_SYMBOL *)event->value);
                        break;
                    case TID_STRING:
                    case TID_CLOB:
                    case TID_BLOB:
                        free_ion_string((ION_STRING *) event->value);
                        break;
                    default:
                        free(event->value);
                        break;
                }
            }
            delete event;
        }
    }
    delete event_stream;
}

IonEvent * IonEventStream::append_new(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name,
                                      ION_SYMBOL **annotations, SIZE num_annotations, int depth) {
    IonEvent *event = new IonEvent(event_type, ion_type, field_name, annotations, num_annotations, depth);
    event_stream->push_back(event);
    return event;
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

iERR read_next_value(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth);

iERR read_all(hREADER hreader, IonEventStream *stream, BOOL in_struct, int depth) {
    iENTER;
    ION_TYPE t;
    for (;;) {
        IONCHECK(ion_reader_next(hreader, &t));
        if (t == tid_EOF) {
            assert(t == tid_EOF && "next() at end");
            break;
        }
        IONCHECK(read_next_value(hreader, stream, t, in_struct, depth));
    }
    iRETURN;
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

void copy_ion_symbol(ION_SYMBOL **dst, ION_SYMBOL *src) {
    ION_SYMBOL *copy = (ION_SYMBOL *)calloc(1, sizeof(ION_SYMBOL));
    copy->value.length = src->value.length;
    if (src->value.value != NULL) {
        copy->value.value = (BYTE *)malloc(sizeof(BYTE) * copy->value.length);
        memcpy(copy->value.value, src->value.value, (size_t)copy->value.length);
    }
    ION_STRING_INIT(&copy->import_location.name);
    if (!ION_SYMBOL_IMPORT_LOCATION_IS_NULL(src)) {
        copy->import_location.name.length = src->import_location.name.length;
        copy->import_location.name.value = (BYTE *)malloc(sizeof(BYTE) * copy->import_location.name.length);
        memcpy(copy->import_location.name.value, src->import_location.name.value, (size_t)copy->import_location.name.length);
        copy->import_location.location = src->import_location.location;
    }
    copy->sid = src->sid;
    *dst = copy;
}

iERR record_symbol_table_context_change(void *stream, ION_COLLECTION *imports) {
    iENTER;
    IonEventStream *event_stream = (IonEventStream *)stream;
    IonEvent *event = event_stream->append_new(SYMBOL_TABLE, tid_none, NULL, NULL, 0, 0);
    ION_COLLECTION *copied_imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
    _ion_collection_initialize(copied_imports, copied_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    IONCHECK(_ion_collection_copy(copied_imports, imports, &_ion_symbol_table_local_import_copy_new_owner, copied_imports));
    event->value = copied_imports;
    iRETURN;
}

void ion_event_register_symbol_table_callback(ION_READER_OPTIONS *options, IonEventStream *stream) {
    options->context_change_notifier.notify = &record_symbol_table_context_change;
    options->context_change_notifier.context = stream;
}

iERR read_next_value(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth) {
    iENTER;

    BOOL        is_null;
    ION_SYMBOL *field_name = NULL;
    SIZE        annotation_count = 0;
    ION_SYMBOL *annotations_tmp = NULL;
    ION_SYMBOL **annotations = NULL;
    IonEvent *event = NULL;
    ION_EVENT_TYPE event_type = SCALAR;
    int ion_type = ION_TID_INT(t);

    if (in_struct) {
        ION_SYMBOL *field_name_tmp;
        IONCHECKORFREE2(ion_reader_get_field_name_symbol(hreader, &field_name_tmp));
        copy_ion_symbol(&field_name, field_name_tmp);
    }

    IONCHECKORFREE2(ion_reader_get_annotation_count(hreader, &annotation_count));
    if (annotation_count > 0) {
        annotations_tmp = (ION_SYMBOL *)calloc((size_t)annotation_count, sizeof(ION_SYMBOL));
        annotations = (ION_SYMBOL **)calloc((size_t)annotation_count, sizeof(ION_SYMBOL *));
        IONCHECKORFREE(ion_reader_get_annotation_symbols(hreader, annotations_tmp, annotation_count, &annotation_count),
                       annotations_tmp);
        for (int i = 0; i < annotation_count; i++) {
            copy_ion_symbol(&annotations[i], &annotations_tmp[i]);
        }
        free(annotations_tmp);
    }

    IONCHECKORFREE2(ion_reader_is_null(hreader, &is_null));
    if (is_null) {
        IONCHECKORFREE2(ion_reader_read_null(hreader, &t));
        stream->append_new(SCALAR, t, field_name, annotations, annotation_count, depth);
        SUCCEED();
    }

    if (ion_type == TID_STRUCT || ion_type == TID_LIST || ion_type == TID_SEXP) {
        event_type = CONTAINER_START;
    }
    event = stream->append_new(event_type, t, field_name, annotations, annotation_count, depth);

    switch (ion_type) {
        case TID_EOF:
            SUCCEED();
        case TID_BOOL:
        {
            BOOL *bool_value = (BOOL *)malloc(sizeof(BOOL));
            IONCHECKORFREE(ion_reader_read_bool(hreader, bool_value), bool_value);
            event->value = bool_value;
            break;
        }
        case TID_POS_INT:
        case TID_NEG_INT:
        {
            ION_INT *ion_int_value = NULL;
            IONCHECKORFREE2(ion_int_alloc(NULL, &ion_int_value)); // NOTE: owner must be NULL; otherwise, this may be unexpectedly freed.
            IONCHECKORFREE(ion_reader_read_ion_int(hreader, ion_int_value), ion_int_value);
            event->value = ion_int_value;
            break;
        }
        case TID_FLOAT:
        {
            double *double_value = (double *)malloc(sizeof(double));
            IONCHECKORFREE(ion_reader_read_double(hreader, double_value), double_value);
            event->value = double_value;
            break;
        }
        case TID_DECIMAL:
        {
            ION_DECIMAL *decimal_value = (ION_DECIMAL *)malloc(sizeof(ION_DECIMAL));
            IONCHECKORFREE(ion_reader_read_ion_decimal(hreader, decimal_value), decimal_value);
            IONCHECKORFREE(ion_decimal_claim(decimal_value), decimal_value);
            event->value = decimal_value;
            break;
        }
        case TID_TIMESTAMP:
        {
            ION_TIMESTAMP *timestamp_value = (ION_TIMESTAMP *)malloc(sizeof(ION_TIMESTAMP));
            IONCHECKORFREE(ion_reader_read_timestamp(hreader, timestamp_value), timestamp_value);
            event->value = timestamp_value;
            break;
        }
        case TID_SYMBOL:
        {
            ION_SYMBOL tmp, *symbol_value;
            IONCHECKORFREE2(ion_reader_read_ion_symbol(hreader, &tmp));
            copy_ion_symbol(&symbol_value, &tmp);
            event->value = symbol_value;
            break;
        }
        case TID_STRING:
        {
            ION_STRING tmp;
            IONCHECKORFREE2(ion_reader_read_string(hreader, &tmp));
            ION_STRING *string_value = copy_ion_string(&tmp);
            event->value = string_value;
            break;
        }
        case TID_CLOB: // intentional fall-through
        case TID_BLOB:
        {
            SIZE length, bytes_read;
            IONCHECKORFREE2(ion_reader_get_lob_size(hreader, &length));
            BYTE *buf = (BYTE*)malloc((size_t)length * sizeof(BYTE));
            if (length) {
                IONCHECKORFREE(ion_reader_read_lob_bytes(hreader, buf, length, &bytes_read), buf);
                if (length != bytes_read) {
                    free(buf);
                    buf = NULL;
                    FAILWITH(IERR_EOF);
                }
            }
            ION_LOB *lob_value = (ION_LOB *)malloc(sizeof(ION_LOB));
            lob_value->value = buf;
            lob_value->length = length;
            event->value = lob_value;
            break;
        }
        case TID_STRUCT: // intentional fall-through
        case TID_SEXP: // intentional fall-through
        case TID_LIST:
            IONCHECKORFREE2(ion_reader_step_in(hreader));
            IONCHECKORFREE2(read_all(hreader, stream, t == tid_STRUCT, depth + 1));
            IONCHECKORFREE2(ion_reader_step_out(hreader));
            stream->append_new(CONTAINER_END, t, /*field_name=*/NULL, /*annotations=*/NULL, /*annotation_count=*/0,
                               depth);
            break;
        case TID_DATAGRAM:
            default: IONCHECKORFREE2(IERR_INVALID_STATE);
    }
    iRETURN;
}

iERR ion_event_stream_read_all(hREADER hreader, IonEventStream *stream) {
    iENTER;
    IONCHECK(read_all(hreader, stream, /*in_struct=*/FALSE, /*depth=*/0));
    stream->append_new(STREAM_END, tid_none, NULL, NULL, 0, 0);
    iRETURN;
}

iERR read_value_stream_from_string(const char *ion_string, IonEventStream *stream) {
    iENTER;
    hREADER      reader;
    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    ion_event_register_symbol_table_callback(&options, stream);

    IONCHECK(ion_reader_open_buffer(&reader, (BYTE *)ion_string, (SIZE)strlen(ion_string), &options));
    IONCHECK(ion_event_stream_read_all(reader, stream));
    IONCHECK(ion_reader_close(reader));
    iRETURN;
}

iERR read_value_stream_from_bytes(const BYTE *ion_string, SIZE len, IonEventStream *stream, ION_CATALOG *catalog) {
    iENTER;
    hREADER      reader;
    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    if (catalog) {
        options.pcatalog = catalog;
    }
    ion_event_register_symbol_table_callback(&options, stream);

    IONCHECK(ion_reader_open_buffer(&reader, (BYTE *)ion_string, len, &options));
    IONCHECK(ion_event_stream_read_all(reader, stream));
    IONCHECK(ion_reader_close(reader));
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

std::string ion_event_type_to_string(ION_EVENT_TYPE type) {
    switch (type) {
        case SCALAR: return "SCALAR";
        case CONTAINER_START: return "CONTAINER_START";
        case CONTAINER_END: return "CONTAINER_END";
        case SYMBOL_TABLE: return "SYMBOL_TABLE";
        case STREAM_END: return "STREAM_END";
    }
}

std::string ion_event_ion_type_to_string(ION_TYPE type) {
    switch (ION_TID_INT(type)) {
        case TID_NULL: return "NULL";
        case TID_BOOL: return "BOOL";
        case TID_POS_INT:
        case TID_NEG_INT: return "INT";
        case TID_FLOAT: return "FLOAT";
        case TID_DECIMAL: return "DECIMAL";
        case TID_TIMESTAMP: return "TIMESTAMP";
        case TID_SYMBOL: return "SYMBOL";
        case TID_STRING: return "STRING";
        case TID_BLOB: return "BLOB";
        case TID_CLOB: return "CLOB";
        case TID_LIST: return "LIST";
        case TID_SEXP: return "SEXP";
        case TID_STRUCT: return "STRUCT";
        default: return NULL;
    }
}

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

    IONCHECK(ion_event_in_memory_writer_open(&text_context, FALSE, catalog, (event->ion_type == tid_SYMBOL) ? imports : NULL));
    IONCHECK(ion_event_in_memory_writer_open(&binary_context, TRUE, catalog, (event->ion_type == tid_SYMBOL) ? imports : NULL));
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

    free(text_value);
    free(binary_value);
    iRETURN;
}

iERR ion_event_stream_write_all_events(hWRITER writer, IonEventStream *stream, ION_CATALOG *catalog) {
    iENTER;
    ION_COLLECTION *imports = NULL;
    for (size_t i = 0; i < stream->size(); i++) {
        IonEvent *event = stream->at(i);
        std::string event_type = ion_event_type_to_string(event->event_type);
        std::string ion_type;
        ION_STRING event_type_str, ion_type_str;
        IONCHECK(ion_writer_start_container(writer, tid_STRUCT));
        ion_string_assign_cstr(&event_type_str, (char *)event_type.c_str(), (SIZE)event_type.size());
        IONCHECK(ion_writer_write_field_name(writer, &ion_event_event_type_field));
        IONCHECK(ion_writer_write_symbol(writer, &event_type_str));
        if (event->ion_type != tid_none) {
            ion_type = ion_event_ion_type_to_string(event->ion_type);
            ion_string_assign_cstr(&ion_type_str, (char *)ion_type.c_str(), (SIZE)ion_type.size());
            IONCHECK(ion_writer_write_field_name(writer, &ion_event_ion_type_field));
            IONCHECK(ion_writer_write_symbol(writer, &ion_type_str));
        }
        if (event->field_name) {
            IONCHECK(ion_writer_write_field_name(writer, &ion_event_field_name_field));
            IONCHECK(ion_event_stream_write_symbol_token(writer, event->field_name));
        }
        if (event->num_annotations > 0) {
            IONCHECK(ion_writer_write_field_name(writer, &ion_event_annotations_field));
            IONCHECK(ion_writer_start_container(writer, tid_LIST));
            for (int j = 0; j < event->num_annotations; j++) {
                IONCHECK(ion_event_stream_write_symbol_token(writer, event->annotations[j]));
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
    }
    iRETURN;
}
