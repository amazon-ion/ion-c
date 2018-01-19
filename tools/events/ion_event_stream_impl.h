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

#ifndef IONC_ION_EVENT_STREAM_IMPL_H
#define IONC_ION_EVENT_STREAM_IMPL_H

#define ION_RESULT_ARG result
#define ION_LOCATION_ARG location
#define ION_STREAM_ARG stream
#define ION_CATALOG_ARG catalog
#define ION_INDEX_ARG index

#define ION_EVENT_READ_PARAMS ION_CATALOG *ION_CATALOG_ARG, IonEventStream *ION_STREAM_ARG, IonEventResult *ION_RESULT_ARG
#define ION_EVENT_READ_ARGS ION_CATALOG_ARG, ION_STREAM_ARG, ION_RESULT_ARG

#define ION_EVENT_COMMON_PARAMS std::string *ION_LOCATION_ARG, IonEventResult *ION_RESULT_ARG
#define ION_EVENT_COMMON_ARGS ION_LOCATION_ARG, ION_RESULT_ARG

#define ION_EVENT_WRITER_PARAMS ION_CATALOG *ION_CATALOG_ARG, ION_EVENT_COMMON_PARAMS
#define ION_EVENT_WRITER_ARGS ION_CATALOG_ARG, ION_EVENT_COMMON_ARGS

#define ION_EVENT_WRITER_INDEX_PARAMS size_t *ION_INDEX_ARG, ION_EVENT_WRITER_PARAMS
#define ION_EVENT_WRITER_INDEX_ARGS ION_INDEX_ARG, ION_EVENT_WRITER_ARGS

#define ION_EVENT_INDEX_PARAMS size_t *ION_INDEX_ARG, ION_EVENT_COMMON_PARAMS
#define ION_EVENT_INDEX_ARGS ION_INDEX_ARG, ION_EVENT_COMMON_ARGS

#define ION_ERROR_LOCATION_VAR _error_location
#define ION_ERROR_EVENT_INDEX_VAR _error_event_index

#define IONCLEANEXIT goto cleanup

/**
 * Conveys any errors that occurred down-stack.
 */
#define IONREPORT(x) \
    err = (x); \
    if (err) { \
        IONCLEANEXIT; \
    }

#define _IONERROR(type, code, msg, loc, idx, res) \
    _ion_event_set_error(res, type, code, msg, loc, idx, __FILE__, __LINE__); \
    IONREPORT(code)

/**
 * Conveys an illegal state error upward at the point it occurs.
 */
#define IONFAILSTATE(code, msg) _IONERROR(ERROR_TYPE_STATE, code, msg, ION_ERROR_LOCATION_VAR, ION_ERROR_EVENT_INDEX_VAR, ION_RESULT_ARG)

#define _IONCCALL(type, x, loc, idx, m) \
    err = (x); \
    if (err) { \
        _IONERROR(type, err, m, loc, idx, ION_RESULT_ARG); \
    }

/**
 * Sets the context required for conveying errors using IONFAILSTATE, IONCREAD, IONCWRITE, IONCSTATE, and ION_NON_FATAL.
 */
#define ION_SET_ERROR_CONTEXT(location, event_index) \
    std::string *ION_ERROR_LOCATION_VAR = location; \
    size_t *ION_ERROR_EVENT_INDEX_VAR = event_index;

/**
 * Conveys an error upward from a call to an ion-c read API.
 */
#define IONCREAD(x) _IONCCALL(ERROR_TYPE_READ, x, ION_ERROR_LOCATION_VAR, NULL, "")

/**
 * Conveys an error upward from a call to an ion-c write API.
 */
#define IONCWRITE(x) _IONCCALL(ERROR_TYPE_WRITE, x, ION_ERROR_LOCATION_VAR, ION_ERROR_EVENT_INDEX_VAR, "")

/**
 * Conveys an error upward from a call to an ion-c state API (e.g. an initialization or cleanup function).
 */
#define IONCSTATE(x, m) _IONCCALL(ERROR_TYPE_STATE, x, ION_ERROR_LOCATION_VAR, NULL, m)

/**
 * Conveys the error only if an error had not previously occurred. Does not short-circuit on failure. This is useful
 * for conveying errors that occur in cleanup routines.
 */
#define ION_NON_FATAL(x, m) { \
    iERR err_backup = (x); \
    if (err == IERR_OK && err_backup != IERR_OK) { \
        err = err_backup; \
        _ion_event_set_error(ION_RESULT_ARG, ERROR_TYPE_STATE, err, m, ION_ERROR_LOCATION_VAR, ION_ERROR_EVENT_INDEX_VAR, __FILE__, __LINE__); \
    } \
}

#define cRETURN cleanup: RETURN(__location_name__, __line__, __count__++, err)

typedef ION_STRING ION_LOB;

/**
 * Configure the given reader options to add a SYMBOL_TABLE event to the given IonEventStream whenever the symbol
 * table context changes.
 */
void ion_event_register_symbol_table_callback(ION_READER_OPTIONS *options, IonEventStream *stream);

/**
 * Returns the length of the value starting at start_index, in number of events. Scalars will always return 1.
 */
size_t ion_event_value_length(IonEventStream *stream, size_t start_index);

/**
 * Returns the length of the stream starting at index and ending in a STREAM_END event, in number of events. Empty
 * streams will always return 1.
 */
size_t ion_event_stream_length(IonEventStream *stream, size_t index);

iERR ion_event_stream_read(hREADER hreader, IonEventStream *stream, ION_TYPE t, BOOL in_struct, int depth, BOOL is_embedded_stream_set, IonEventResult *result);

/**
 * Reads IonEvents from the given BYTE* of Ion data into the given IonEventStream.
 */
iERR ion_event_stream_read_all_from_bytes(const BYTE *ion_string, SIZE len, ION_CATALOG *catalog,
                                          IonEventStream *stream, IonEventResult *result = NULL);

iERR ion_event_stream_read_imports(hREADER reader, ION_COLLECTION *imports, std::string *location, IonEventResult *result);


/**
 * Writes an IonEventStream as an Ion stream in the given format. The caller is responsible for freeing the output
 * bytes.
 */
iERR ion_event_stream_write_all_to_bytes(IonEventStream *stream, ION_EVENT_OUTPUT_TYPE output_type,
                                         ION_CATALOG *catalog, BYTE **out, SIZE *len, IonEventResult *result);

iERR ion_event_stream_write_error_report(hWRITER writer, IonEventReport *report, ION_CATALOG *catalog, std::string *location, IonEventResult *result);
iERR ion_event_stream_write_comparison_report(hWRITER writer, IonEventReport *report, ION_CATALOG *catalog, std::string *location, IonEventResult *result);
iERR ion_event_stream_write_error(hWRITER writer, IonEventErrorDescription *error_description);
iERR ion_event_stream_write_comparison_result(hWRITER writer, IonEventComparisonResult *comparison_result, ION_CATALOG *catalog, std::string *location, IonEventResult *result);

iERR ion_event_copy(IonEvent **dst, IonEvent *src, std::string *location, IonEventResult *result);

#endif //IONC_ION_EVENT_STREAM_IMPL_H
