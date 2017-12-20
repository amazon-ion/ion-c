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
    std::string location;
    IonEventStream(std::string location="UNKNOWN");
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

typedef enum _ion_writer_output_type {
    ION_WRITER_OUTPUT_TYPE_TEXT_PRETTY = 0,
    ION_WRITER_OUTPUT_TYPE_TEXT_UGLY,
    ION_WRITER_OUTPUT_TYPE_BINARY,
} ION_WRITER_OUTPUT_TYPE;

typedef enum _ion_event_error_type {
    ERROR_TYPE_READ = 0,
    ERROR_TYPE_WRITE,
    ERROR_TYPE_STATE,
} ION_EVENT_ERROR_TYPE;

typedef struct _ion_event_report_context {
    std::string location;
    IonEvent *event;
    size_t event_index;
} ION_EVENT_REPORT_CONTEXT;

typedef struct _ion_event_error_description {
    ION_EVENT_ERROR_TYPE error_type;
    std::string message;
    ION_EVENT_REPORT_CONTEXT context;
    bool has_context;
} ION_EVENT_ERROR_DESCRIPTION;

typedef enum _ion_event_comparison_result_type {
    COMPARISON_RESULT_EQUAL = 0,
    COMPARISON_RESULT_NOT_EQUAL,
    COMPARISON_RESULT_ERROR
} ION_EVENT_COMPARISON_RESULT_TYPE;

typedef struct _ion_event_comparison_result {
    ION_EVENT_COMPARISON_RESULT_TYPE result;
    ION_EVENT_REPORT_CONTEXT lhs;
    ION_EVENT_REPORT_CONTEXT rhs;
    std::string message;
} ION_EVENT_COMPARISON_RESULT;

class IonEventResult {
public:
    ION_EVENT_ERROR_DESCRIPTION error_description;
    ION_EVENT_COMPARISON_RESULT comparison_result;
    bool has_error_description;
    bool has_comparison_result;

    IonEventResult() {
        memset(&error_description, 0, sizeof(ION_EVENT_ERROR_DESCRIPTION));
        memset(&comparison_result, 0, sizeof(ION_EVENT_COMPARISON_RESULT));
        has_error_description = false;
        has_comparison_result = false;
    }
};

class IonEventReport {
    std::vector<ION_EVENT_ERROR_DESCRIPTION> error_report;
    std::vector<ION_EVENT_COMPARISON_RESULT> comparison_report;
public:
    IonEventReport() {}
    void addResult(IonEventResult *result);
    iERR writeErrorsTo(hWRITER writer);
    iERR writeComparisonResultsTo(hWRITER writer);
    bool hasErrors() {
        return !error_report.empty();
    }
    bool hasComparisonFailures() {
        return !comparison_report.empty();
    }
};

iERR ion_event_stream_write_error_report(hWRITER writer, IonEventReport *report);
iERR ion_event_stream_write_comparison_report(hWRITER writer, IonEventReport *report);


#define IONCLEANEXIT goto cleanup

/**
 * Conveys any errors that occurred down-stack.
 */
#define IONREPORT(x) \
    err = (x); \
    if (err) { \
        IONCLEANEXIT; \
    }

/**
 * Conveys an error upward at the point it occurs.
 */
#define IONERROR(type, code, msg, ctx, res) \
    _ion_cli_set_error(res, type, code, msg, ctx); \
    IONREPORT(code)

/**
 * Conveys an illegal state error upward at the point it occurs.
 */
#define IONFAILSTATE(code, msg, res) IONERROR(ERROR_TYPE_STATE, code, msg, NULL, res)

#define IONCCALL(type, x, ctx) \
    err = (x); \
    if (err) { \
        IONERROR(type, err, "", ctx, result); \
    }

/**
 * Conveys an error upward from a call to an ion-c read API.
 */
#define IONCREAD(x) IONCCALL(ERROR_TYPE_READ, x, context)
#define IONCWRITE(x) IONCCALL(ERROR_TYPE_WRITE, x, context)
#define IONCSTATE(x) IONCCALL(ERROR_TYPE_STATE, x, context)


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

iERR ion_event_stream_write_error(hWRITER writer, ION_EVENT_ERROR_DESCRIPTION *error_description);

iERR ion_event_stream_write_comparison_result(hWRITER writer, ION_EVENT_COMPARISON_RESULT *comparison_result);

#endif //IONC_VALUE_STREAM_H
