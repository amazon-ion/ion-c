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
#include "ion_test_util.h"
#include <iostream>
#include <ion_helpers.h>

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
                switch (ION_TYPE_INT(event->ion_type)) {
                    case tid_INT_INT:
                        ion_int_free((ION_INT *)event->value);
                        break;
                    case tid_DECIMAL_INT:
                        ion_decimal_free((ION_DECIMAL *) event->value);
                        break;
                    case tid_SYMBOL_INT:
                        free_ion_symbol((ION_SYMBOL *)event->value);
                        break;
                    case tid_STRING_INT:
                    case tid_CLOB_INT:
                    case tid_BLOB_INT:
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

iERR read_next_value(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth) {
    iENTER;

    BOOL        is_null;
    ION_SYMBOL *field_name = NULL;
    SIZE        annotation_count = 0;
    ION_SYMBOL *annotations_tmp = NULL;
    ION_SYMBOL **annotations = NULL;
    IonEvent *event = NULL;
    ION_EVENT_TYPE event_type = SCALAR;
    int ion_type = ION_TYPE_INT(t);

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

    if (ion_type == tid_STRUCT_INT || ion_type == tid_LIST_INT || ion_type == tid_SEXP_INT) {
        event_type = CONTAINER_START;
    }
    event = stream->append_new(event_type, t, field_name, annotations, annotation_count, depth);

    switch (ion_type) {
        case tid_EOF_INT:
            SUCCEED();
        case tid_BOOL_INT:
        {
            BOOL *bool_value = (BOOL *)malloc(sizeof(BOOL));
            IONCHECKORFREE(ion_reader_read_bool(hreader, bool_value), bool_value);
            event->value = bool_value;
            break;
        }
        case tid_INT_INT:
        {
            ION_INT *ion_int_value = NULL;
            IONCHECKORFREE2(ion_int_alloc(NULL, &ion_int_value)); // NOTE: owner must be NULL; otherwise, this may be unexpectedly freed.
            IONCHECKORFREE(ion_reader_read_ion_int(hreader, ion_int_value), ion_int_value);
            event->value = ion_int_value;
            break;
        }
        case tid_FLOAT_INT:
        {
            double *double_value = (double *)malloc(sizeof(double));
            IONCHECKORFREE(ion_reader_read_double(hreader, double_value), double_value);
            event->value = double_value;
            break;
        }
        case tid_DECIMAL_INT:
        {
            ION_DECIMAL *decimal_value = (ION_DECIMAL *)malloc(sizeof(ION_DECIMAL));
            IONCHECKORFREE(ion_reader_read_ion_decimal(hreader, decimal_value), decimal_value);
            IONCHECKORFREE(ion_decimal_claim(decimal_value), decimal_value);
            event->value = decimal_value;
            break;
        }
        case tid_TIMESTAMP_INT:
        {
            ION_TIMESTAMP *timestamp_value = (ION_TIMESTAMP *)malloc(sizeof(ION_TIMESTAMP));
            IONCHECKORFREE(ion_reader_read_timestamp(hreader, timestamp_value), timestamp_value);
            event->value = timestamp_value;
            break;
        }
        case tid_SYMBOL_INT:
        {
            ION_SYMBOL tmp, *symbol_value;
            IONCHECKORFREE2(ion_reader_read_ion_symbol(hreader, &tmp));
            copy_ion_symbol(&symbol_value, &tmp);
            event->value = symbol_value;
            break;
        }
        case tid_STRING_INT:
        {
            ION_STRING tmp;
            IONCHECKORFREE2(ion_reader_read_string(hreader, &tmp));
            ION_STRING *string_value = copy_ion_string(&tmp);
            event->value = string_value;
            break;
        }
        case tid_CLOB_INT: // intentional fall-through
        case tid_BLOB_INT:
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
        case tid_STRUCT_INT: // intentional fall-through
        case tid_SEXP_INT: // intentional fall-through
        case tid_LIST_INT:
            IONCHECKORFREE2(ion_reader_step_in(hreader));
            IONCHECKORFREE2(read_all(hreader, stream, t == tid_STRUCT, depth + 1));
            IONCHECKORFREE2(ion_reader_step_out(hreader));
            stream->append_new(CONTAINER_END, t, /*field_name=*/NULL, /*annotations=*/NULL, /*annotation_count=*/0,
                               depth);
            break;

        case tid_DATAGRAM_INT:
            default: IONCHECKORFREE2(IERR_INVALID_STATE);
    }
    iRETURN;
}

iERR read_all(hREADER hreader, IonEventStream *stream) {
    iENTER;
    IONCHECK(read_all(hreader, stream, /*in_struct=*/FALSE, /*depth=*/0));
    stream->append_new(STREAM_END, tid_none, NULL, NULL, 0, 0);
    iRETURN;
}

iERR read_value_stream_from_string(const char *ion_string, IonEventStream *stream) {
    iENTER;
    hREADER      reader;
    ION_READER_OPTIONS options;
    ion_test_initialize_reader_options(&options);
    options.context_change_notifier.notify = &record_symbol_table_context_change;
    options.context_change_notifier.context = stream;

    IONCHECK(ion_reader_open_buffer(&reader, (BYTE *)ion_string, (SIZE)strlen(ion_string), &options));
    IONCHECK(read_all(reader, stream));
    IONCHECK(ion_reader_close(reader));
    iRETURN;
}

iERR read_value_stream_from_bytes(const BYTE *ion_string, SIZE len, IonEventStream *stream, ION_CATALOG *catalog) {
    iENTER;
    hREADER      reader;
    ION_READER_OPTIONS options;
    ion_test_initialize_reader_options(&options);
    if (catalog) {
        options.pcatalog = catalog;
    }
    options.context_change_notifier.notify = &record_symbol_table_context_change;
    options.context_change_notifier.context = stream;

    IONCHECK(ion_reader_open_buffer(&reader, (BYTE *)ion_string, len, &options));
    IONCHECK(read_all(reader, stream));
    IONCHECK(ion_reader_close(reader));
    iRETURN;
}


iERR read_value_stream(IonEventStream *stream, READER_INPUT_TYPE input_type, std::string pathname, ION_CATALOG *catalog)
{
    iENTER;
    FILE        *fstream = NULL;
    ION_STREAM  *f_ion_stream = NULL;
    hREADER      reader;
    long         size;
    char        *buffer = NULL;
    long         result;

    ION_READER_OPTIONS options;
    ion_test_initialize_reader_options(&options);
    options.pcatalog = catalog;
    options.context_change_notifier.notify = &record_symbol_table_context_change;
    options.context_change_notifier.context = stream;

    const char *pathname_c_str = pathname.c_str();

    switch (input_type) {
        case STREAM:
            // ------------ testing ion_reader_open_stream ----------------
            fstream = fopen(pathname_c_str, "rb");
            if (!fstream) {
                FAILWITHMSG(IERR_CANT_FIND_FILE, pathname_c_str);
            }

            IONCHECK(ion_stream_open_file_in(fstream, &f_ion_stream));
            IONCHECK(ion_reader_open(&reader, f_ion_stream, &options));
            IONCHECK(read_all(reader, stream));
            IONCHECK(ion_reader_close(reader));
            IONCHECK(ion_stream_close(f_ion_stream));
            fclose(fstream);
            break;
        case BUFFER:
            // ------------ testing ion_reader_open_buffer ----------------
            fstream = fopen(pathname_c_str, "rb");
            if (!fstream) {
                FAILWITHMSG(IERR_CANT_FIND_FILE, pathname_c_str);
            }

            fseek(fstream, 0, SEEK_END);
            size = ftell(fstream);
            rewind(fstream);                // Set position indicator to the beginning
            buffer = (char *) malloc(size);
            result = fread(buffer, 1, size, fstream);  // copy the file into the buffer:
            fclose(fstream);

            IONCHECK(ion_reader_open_buffer(&reader, (BYTE *) buffer, result, &options));
            IONCHECK(read_all(reader, stream));
            IONCHECK(ion_reader_close(reader));
            free(buffer);
            buffer = NULL;
            break;
        default:
            FAILWITH(IERR_INVALID_ARG);
    }
fail:
    if (buffer) {
        free(buffer);
    }
    RETURN(__location_name__, __line__, __count__++, err);
}

iERR write_scalar(hWRITER writer, IonEvent *event) {
    iENTER;
    if (!event->value) {
        IONCHECK(ion_writer_write_typed_null(writer, event->ion_type));
        SUCCEED();
    }
    switch (ION_TYPE_INT(event->ion_type)) {
        case tid_BOOL_INT:
            IONCHECK(ion_writer_write_bool(writer, *(BOOL *)event->value));
            break;
        case tid_INT_INT:
            IONCHECK(ion_writer_write_ion_int(writer, (ION_INT *)event->value));
            break;
        case tid_FLOAT_INT:
            IONCHECK(ion_writer_write_double(writer, *(double *)event->value));
            break;
        case tid_DECIMAL_INT:
            IONCHECK(ion_writer_write_ion_decimal(writer, (ION_DECIMAL *)event->value));
            break;
        case tid_TIMESTAMP_INT:
            IONCHECK(ion_writer_write_timestamp(writer, (ION_TIMESTAMP *)event->value));
            break;
        case tid_SYMBOL_INT:
            IONCHECK(ion_writer_write_ion_symbol(writer, (ION_SYMBOL *)event->value));
            break;
        case tid_STRING_INT:
            IONCHECK(ion_writer_write_string(writer, (ION_STRING *)event->value));
            break;
        case tid_CLOB_INT:
            IONCHECK(ion_writer_write_clob(writer, ((ION_STRING *)event->value)->value, ((ION_STRING *)event->value)->length));
            break;
        case tid_BLOB_INT:
            IONCHECK(ion_writer_write_blob(writer, ((ION_STRING *)event->value)->value, ((ION_STRING *)event->value)->length));
            break;
        case tid_NULL_INT: // NOTE: null events can only have NULL values; this is handled before the switch.
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

iERR write_value_stream(IonEventStream *stream, VECTOR_TEST_TYPE test_type, ION_CATALOG *catalog, BYTE **out, SIZE *len) {
    // TODO should also have versions of this that don't even need the event stream, and just use ion_writer_write_all_values().
    iENTER;
    ION_STREAM *ion_stream = NULL;
    POSITION pos;
    IONCHECK(ion_stream_open_memory_only(&ion_stream)); // TODO more types of output streams?
    hWRITER writer;
    ION_WRITER_OPTIONS options;
    ion_test_initialize_writer_options(&options);
    options.output_as_binary = (test_type == ROUNDTRIP_BINARY);
    options.pcatalog = catalog;

    IONCHECK(ion_writer_open(&writer, ion_stream, &options));

    for (size_t i = 0; i < stream->size(); i++) {
        IONCHECK(write_event(writer, stream->at(i)));
    }

    IONCHECK(ion_writer_close(writer));
    pos = ion_stream_get_position(ion_stream);
    IONCHECK(ion_stream_seek(ion_stream, 0));
    *out = (BYTE *)(malloc((size_t)pos));
    SIZE bytes_read;
    IONCHECK(ion_stream_read(ion_stream, *out, (SIZE)pos, &bytes_read));

    IONCHECK(ion_stream_close(ion_stream));
    if (bytes_read != (SIZE)pos) {
        FAILWITH(IERR_EOF);
    }
    *len = bytes_read;
    iRETURN;
}
