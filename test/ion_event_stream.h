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

typedef enum _reader_input_type {
    /**
     * Creates an ION_STREAM for the input file using ion_stream_open_file_in, then a reader using ion_reader_open.
     */
    STREAM = 0,
    /**
     * Buffers the contents of the input file, then creates a reader over that buffer using ion_reader_open_buffer.
     */
    BUFFER
} READER_INPUT_TYPE;

typedef enum _ion_event_type {
    SCALAR = 0,
    CONTAINER_START,
    CONTAINER_END,
    STREAM_END
} ION_EVENT_TYPE;

typedef enum _vector_test_type {
    /**
     * Simply read the file.
     */
    READ = 0,
    /**
     * Read the file, then write the file in the text format (regardless of the input format), then read the file.
     * Compare the event streams from the first and second reads for equivalence.
     */
    ROUNDTRIP_TEXT,
    /**
     * Read the file, then write the file in the binary format (regardless of the input format), then read the file.
     * Compare the event streams from the first and second reads for equivalence.
     */
    ROUNDTRIP_BINARY
} VECTOR_TEST_TYPE;

class IonEvent {
public:
    ION_EVENT_TYPE event_type;
    ION_TYPE ion_type;
    ION_STRING *field_name;
    ION_STRING **annotations;
    SIZE num_annotations;
    int depth;
    void *value;

    IonEvent(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_STRING *field_name, ION_STRING **annotations, SIZE num_annotations, int depth) {
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
    IonEvent *append_new(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_STRING *field_name,
                         ION_STRING **annotations, SIZE num_annotations, int depth);

    size_t size() {
        return event_stream->size();
    }

    IonEvent *at(size_t index) {
        return event_stream->at(index);
    }
};

/**
 * Returns the length of the value starting at start_index, in number of events. Scalars will always return 1.
 */
size_t valueEventLength(IonEventStream *stream, size_t start_index);

/**
 * Reads IonEvents from the given string of Ion data into the given IonEventStream.
 */
iERR read_value_stream_from_string(const char *ion_string, IonEventStream *stream);

/**
 * Constructs a reader using the given input type and catalog, then reads IonEvents from the Ion data contained
 * within the file at the given pathname, into the given IonEventStream.
 */
iERR read_value_stream(IonEventStream *stream, READER_INPUT_TYPE input_type, std::string pathname, ION_CATALOG *catalog);

/**
 * Constructs a writer using the given test type and catalog and uses it to write the given IonEventStream to BYTEs.
 */
iERR write_value_stream(IonEventStream *stream, VECTOR_TEST_TYPE test_type, ION_CATALOG *catalog, BYTE **out);

#endif //IONC_VALUE_STREAM_H
