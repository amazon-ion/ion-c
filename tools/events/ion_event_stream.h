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

#ifndef IONC_VALUE_STREAM_H
#define IONC_VALUE_STREAM_H

#include <vector>
#include <string>
#include "ion.h"

typedef ION_STRING ION_LOB;

typedef enum _ion_event_type {
    SCALAR = 0,
    CONTAINER_START,
    CONTAINER_END,
    SYMBOL_TABLE,
    STREAM_END,
    UNKNOWN
} ION_EVENT_TYPE;

class IonEvent {
public:
    ION_EVENT_TYPE event_type;
    ION_TYPE ion_type;
    ION_SYMBOL *field_name;
    ION_SYMBOL **annotations;
    SIZE num_annotations;
    int depth;
    void *value;

    IonEvent(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name, ION_SYMBOL **annotations, SIZE num_annotations, int depth) {
        this->event_type = event_type;
        this->ion_type = ion_type;
        this->field_name = field_name;
        this->annotations = annotations;
        this->num_annotations = num_annotations;
        this->depth = depth;
        value = NULL;
    }
};

class IonEventStream {
    std::vector<IonEvent*> *event_stream;
public:
    IonEventStream();
    ~IonEventStream();

    /**
     * Creates a new IonEvent from the given parameters, appends it to the IonEventStream, and returns it.
     * It is up to the caller to set the returned IonEvent's value.
     */
    IonEvent *append_new(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name,
                         ION_SYMBOL **annotations, SIZE num_annotations, int depth);

    size_t size() {
        return event_stream->size();
    }

    IonEvent *at(size_t index) {
        return event_stream->at(index);
    }

    void remove(size_t index) {
        event_stream->erase(event_stream->begin() + index);
    }
};

/**
 * Configure the given reader options to add a SYMBOL_TABLE event to the given IonEventStream whenever the symbol
 * table context changes.
 */
void ion_event_register_symbol_table_callback(ION_READER_OPTIONS *options, IonEventStream *stream);

/**
 * Returns the length of the value starting at start_index, in number of events. Scalars will always return 1.
 */
size_t valueEventLength(IonEventStream *stream, size_t start_index);

/**
 * Reads IonEvents from the given string of Ion data into the given IonEventStream.
 */
iERR read_value_stream_from_string(const char *ion_string, IonEventStream *stream, ION_CATALOG *catalog);
iERR read_value_stream_from_bytes(const BYTE *ion_string, SIZE len, IonEventStream *stream, ION_CATALOG *catalog);

iERR ion_event_stream_read(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth);

/**
 * Reads an IonEventStream from the given reader's data.
 */
iERR ion_event_stream_read_all(hREADER hreader, IonEventStream *stream);

iERR ion_event_stream_read_all_events(hREADER reader, IonEventStream *stream, ION_CATALOG *catalog);

/**
 * Writes an IonEventStream as an Ion stream using the given writer.
 */
iERR ion_event_stream_write_all(hWRITER writer, IonEventStream *stream);

/**
 * Writes an IonEventStream as a serialized event stream using the given writer.
 */
iERR ion_event_stream_write_all_events(hWRITER writer, IonEventStream *stream, ION_CATALOG *catalog);

#endif //IONC_VALUE_STREAM_H
