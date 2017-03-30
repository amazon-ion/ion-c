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
#include <iostream>
#include <ion_helpers.h>

#define IONCHECKORFREE(x, y) { \
    err = x; \
    if (err) { \
        free_ion_strings(annotations, annotation_count); annotations = NULL; \
        free_ion_string(field_name); field_name = NULL; \
        if (y) free(y); y = NULL; \
        goto fail; \
    } \
} \

#define IONCHECKORFREE2(x) { \
    err = x; \
    if (err) { \
        free_ion_strings(annotations, annotation_count); annotations = NULL; \
        free_ion_string(field_name); field_name = NULL; \
        goto fail; \
    } \
} \


void free_ion_string(ION_STRING *str) {
    if (str) {
        if (str->value) {
            free(str->value);
        }
    }
    free(str);
}

void free_ion_strings(ION_STRING **strs, SIZE len) {
    if (strs) {
        for (int q = 0; q < len; q++) {
            free_ion_string(strs[q]);
        }
        free(strs);
    }
}

IonEventStream::IonEventStream() {
    event_stream = new std::vector<IonEvent *>();
}

IonEventStream::~IonEventStream() {
    for (int i = 0; i < event_stream->size(); i++) {
        IonEvent *event = event_stream->at(i);
        if (event) {
            free_ion_strings(event->annotations, event->num_annotations);
            free_ion_string(event->field_name);
            if (event->value) {
                switch (ION_TYPE_INT(event->ion_type)) {
                    case tid_INT_INT:
                        ion_int_free((ION_INT *)event->value);
                        break;
                    case tid_SYMBOL_INT:
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

IonEvent * IonEventStream::append_new(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_STRING *field_name,
                                      ION_STRING **annotations, SIZE num_annotations, int depth) {
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
    string_value->value = (BYTE *)malloc(len);
    memcpy(string_value->value, src->value, len);
    string_value->length = (int32_t)len;
    return string_value;
}

iERR read_next_value(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth) {
    iENTER;

    BOOL        is_null;
    ION_STRING *field_name = NULL;
    SIZE        annotation_count = 0;
    ION_STRING *annotations_tmp = NULL;
    ION_STRING **annotations = NULL;
    IonEvent *event = NULL;
    ION_EVENT_TYPE event_type = SCALAR;
    int ion_type = ION_TYPE_INT(t);

    if (in_struct) {
        ION_STRING field_name_tmp;
        IONCHECKORFREE2(ion_reader_get_field_name(hreader, &field_name_tmp));
        field_name = copy_ion_string(&field_name_tmp);
    }

    IONCHECKORFREE2(ion_reader_get_annotation_count(hreader, &annotation_count));
    if (annotation_count > 0) {
        annotations_tmp = (ION_STRING *)malloc(annotation_count * sizeof(ION_STRING));
        annotations = (ION_STRING **)malloc(annotation_count * sizeof(ION_STRING *));
        IONCHECKORFREE(ion_reader_get_annotations(hreader, annotations_tmp, annotation_count, &annotation_count),
                       annotations_tmp);
        for (int i = 0; i < annotation_count; i++) {
            annotations[i] = copy_ion_string(&annotations_tmp[i]);
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
            decQuad *decimal_value = (decQuad *)malloc(sizeof(decQuad));
            IONCHECKORFREE(ion_reader_read_decimal(hreader, decimal_value), decimal_value);
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
        case tid_SYMBOL_INT: // intentional fall-through
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
        case tid_LIST_INT: IONCHECKORFREE2(ion_reader_step_in(hreader));
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
    return read_all(hreader, stream, /*in_struct=*/FALSE, /*depth=*/0);
}

iERR read_value_stream_from_string(const char *ion_string, IonEventStream *stream) {
    iENTER;
    hREADER      reader;
    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(ION_READER_OPTIONS));
    options.max_container_depth = 100; // Arbitrarily high; if any test vector exceeds this depth, raise this threshold.
    options.max_annotation_count = 100; // "

    IONCHECK(ion_reader_open_buffer(&reader, (BYTE *)ion_string, strlen(ion_string), &options));
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
    memset(&options, 0, sizeof(ION_READER_OPTIONS));
    options.max_container_depth = 100; // Arbitrarily high; if any test vector exceeds this depth, raise this threshold.
    options.max_annotation_count = 100; // "
    options.pcatalog = catalog;

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

    stream->append_new(STREAM_END, tid_none, NULL, NULL, 0, 0);
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
            IONCHECK(ion_writer_write_decimal(writer, (decQuad *)event->value));
            break;
        case tid_TIMESTAMP_INT:
            IONCHECK(ion_writer_write_timestamp(writer, (ION_TIMESTAMP *)event->value));
            break;
        case tid_SYMBOL_INT:
            IONCHECK(ion_writer_write_symbol(writer, (ION_STRING *)event->value));
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
        IONCHECK(ion_writer_write_field_name(writer, event->field_name));
    }
    if (event->num_annotations) {
        IONCHECK(ion_writer_write_annotations(writer, event->annotations, event->num_annotations));
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
        case STREAM_END:
            break;
        default:
            FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR write_value_stream(IonEventStream *stream, VECTOR_TEST_TYPE test_type, ION_CATALOG *catalog, BYTE **out) {
    // TODO should also have versions of this that don't even need the event stream, and just use ion_writer_write_all_values().
    iENTER;
    ION_STREAM *ion_stream = NULL;
    POSITION len;
    IONCHECK(ion_stream_open_memory_only(&ion_stream)); // TODO more types of output streams?
    hWRITER writer;
    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = (test_type == ROUNDTRIP_BINARY);
    options.pcatalog = catalog;

    IONCHECK(ion_writer_open(&writer, ion_stream, &options));

    for (size_t i = 0; i < stream->size(); i++) {
        IONCHECK(write_event(writer, stream->at(i)));
    }

    IONCHECK(ion_writer_close(writer));
    len = ion_stream_get_position(ion_stream);
    IONCHECK(ion_stream_seek(ion_stream, 0));
    *out = (BYTE *)(malloc((size_t)len));
    SIZE bytes_read;
    IONCHECK(ion_stream_read(ion_stream, *out, (SIZE)len, &bytes_read));

    IONCHECK(ion_stream_close(ion_stream));
    if (bytes_read != (SIZE)len) {
        FAILWITH(IERR_EOF);
    }

    iRETURN;
}
