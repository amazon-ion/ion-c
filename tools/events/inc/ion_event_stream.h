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

typedef enum _ion_event_type {
    SCALAR = 0,
    CONTAINER_START,
    CONTAINER_END,
    SYMBOL_TABLE,
    STREAM_END,
    UNKNOWN
} ION_EVENT_TYPE;

/**
 * Describes a single Ion parsing event.
 */
class IonEvent {
public:
    ION_EVENT_TYPE event_type;
    ION_TYPE ion_type;
    ION_SYMBOL *field_name;
    ION_SYMBOL *annotations;
    SIZE num_annotations;
    int depth;
    void *value;

    IonEvent(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name, ION_SYMBOL *annotations, SIZE num_annotations, int depth);
    ~IonEvent();
};

/**
 * Describes the sequence of Ion parsing events that make up a given Ion stream.
 */
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
    IonEvent *appendNew(ION_EVENT_TYPE event_type, ION_TYPE ion_type, ION_SYMBOL *field_name,
                        ION_SYMBOL *annotations, SIZE num_annotations, int depth);

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

typedef enum _ion_event_output_type {
    OUTPUT_TYPE_TEXT_PRETTY = 0,
    OUTPUT_TYPE_TEXT_UGLY,
    OUTPUT_TYPE_BINARY,
    OUTPUT_TYPE_EVENTS,
    OUTPUT_TYPE_NONE,
} ION_EVENT_OUTPUT_TYPE;

typedef enum _ion_event_error_type {
    ERROR_TYPE_UNKNOWN = 0,
    ERROR_TYPE_READ,
    ERROR_TYPE_WRITE,
    ERROR_TYPE_STATE,
} ION_EVENT_ERROR_TYPE;

/**
 * Describes a single error.
 */
class IonEventErrorDescription {
public:
    ION_EVENT_ERROR_TYPE error_type;
    std::string message;
    std::string location;
    size_t event_index;
    bool has_location;
    bool has_event_index;

    IonEventErrorDescription() {
        memset(this, 0, sizeof(IonEventErrorDescription));
    }

    /**
     * Writes an Ion representation of this error description using the given writer.
     */
    iERR writeTo(hWRITER writer);
};

typedef enum _ion_event_comparison_result_type {
    COMPARISON_RESULT_EQUAL = 0,
    COMPARISON_RESULT_NOT_EQUAL,
    COMPARISON_RESULT_ERROR
} ION_EVENT_COMPARISON_RESULT_TYPE;

class IonEventResult;

/**
 * Describes a single side of a comparison operation.
 */
class IonEventComparisonContext {
public:
    std::string location;
    IonEvent *event;
    size_t event_index;

    IonEventComparisonContext() {
        memset(this, 0, sizeof(IonEventComparisonContext));
    }

    /**
     * Writes an Ion representation of this comparison context using the given writer.
     */
    iERR writeTo(hWRITER writer, ION_CATALOG *catalog, std::string *location, IonEventResult *result);
};

/**
 * Describes a single comparison failure.
 */
class IonEventComparisonResult {
public:
    ION_EVENT_COMPARISON_RESULT_TYPE result;
    IonEventComparisonContext lhs;
    IonEventComparisonContext rhs;
    std::string message;

    IonEventComparisonResult() {
        memset(this, 0, sizeof(IonEventComparisonResult));
    }

    /**
     * Writes an Ion representation of this comparison result using the given writer.
     */
    iERR writeTo(hWRITER writer, ION_CATALOG *catalog, std::string *location, IonEventResult *result);
};

/**
 * Describes the result of a single compare or process operation.
 */
class IonEventResult {
public:
    IonEventErrorDescription error_description;
    IonEventComparisonResult comparison_result;
    bool has_error_description;
    bool has_comparison_result;

    IonEventResult() {
        memset(this, 0, sizeof(IonEventResult));
    }

    ~IonEventResult() {
        if (has_comparison_result) {
            delete comparison_result.lhs.event;
            delete comparison_result.rhs.event;
        }
    }
};

/**
 * Describes the results of a set of compare or process operations.
 */
class IonEventReport {
    std::vector<IonEventErrorDescription> error_report;
    std::vector<IonEventComparisonResult> comparison_report;
public:
    IonEventReport() {}
    ~IonEventReport() {
        for (size_t i = 0; i < comparison_report.size(); i++) {
            IonEventComparisonResult *comparison_result = &comparison_report.at(i);
            delete comparison_result->lhs.event;
            delete comparison_result->rhs.event;
        }
    }

    /**
     * Adds the given IonEventResult's results to the report and claims ownership of their resources.
     */
    void addResult(IonEventResult *result);

    /**
     * Writes an Ion representation of the report's errors using the given writer. Any errors that occur during writing
     * are conveyed in a non-zero return value.
     */
    iERR writeErrorsTo(hWRITER writer);

    /**
     * Writes an Ion representation of the report's comparison results using the given writer. If either of the events
     * contain symbols with unknown text, the optional catalog is used to resolve their imports. Any errors that occur
     * during writing are conveyed in `result` and in a non-zero return value.
     */
    iERR writeComparisonResultsTo(hWRITER writer, ION_CATALOG *catalog, std::string *location, IonEventResult *result);

    bool hasErrors() {
        return !error_report.empty();
    }
    bool hasComparisonFailures() {
        return !comparison_report.empty();
    }
    std::vector<IonEventErrorDescription> *getErrors() { return &error_report; }
    std::vector<IonEventComparisonResult> *getComparisonResults() { return &comparison_report; }
};

/**
 * Reads an IonEventStream from the given reader's data, which may contain a regular Ion stream or an event stream.
 */
iERR ion_event_stream_read_all(hREADER hreader, ION_CATALOG *catalog, IonEventStream *stream, IonEventResult *result);

/**
 * Writes an IonEventStream as an Ion stream using the given writer.
 */
iERR ion_event_stream_write_all(hWRITER writer, IonEventStream *stream, IonEventResult *result);

/**
 * Writes an IonEventStream as a serialized event stream using the given writer. The given catalog, which may be NULL,
 * is used by temporary scalar value writers to resolve the shared symbol tables for symbol values with unknown text.
 */
iERR ion_event_stream_write_all_events(hWRITER writer, IonEventStream *stream, ION_CATALOG *catalog, IonEventResult *result);

#endif //IONC_VALUE_STREAM_H
