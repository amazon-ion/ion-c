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

#include <string>
#include <sstream>
#include <iostream>
#include <inc/ion_errors.h>
#include <inc/ion.h>
#include <ion_helpers.h>
#include <ion_event_util.h>
#include <ion_event_stream.h>
#include <ion_event_equivalence.h>
#include "cli.h"

iERR ion_cli_command_process_filter(ION_EVENT_WRITER_CONTEXT *writer_context, ION_CLI_COMMON_ARGS *common_args, ION_CLI_PROCESS_ARGS *process_args, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    // TODO implement jq-style filtering.
    IONFAILSTATE(IERR_NOT_IMPL, "Filtering not yet implemented.", result);
    cRETURN;
}

iERR ion_cli_command_process_traverse(ION_EVENT_WRITER_CONTEXT *writer_context, ION_CLI_COMMON_ARGS *common_args, ION_CLI_PROCESS_ARGS *process_args, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    // TODO implement custom read traversal.
    IONFAILSTATE(IERR_NOT_IMPL, "Custom traversals not yet implemented.", result);
    cRETURN;
}

iERR ion_cli_close_reader(ION_CLI_READER_CONTEXT *context, IonEventResult *result) {
    iENTER;
    ASSERT(context);
    ION_SET_ERROR_CONTEXT(&context->input_location, NULL);
    if (context->reader) {
        ION_NON_FATAL(ion_reader_close(context->reader), "Failed to close reader.");
        context->reader = NULL;
    }
    if (context->ion_stream) {
        ION_NON_FATAL(ion_stream_close(context->ion_stream), "Failed to close ION_STREAM.");
        context->ion_stream = NULL;
    }
    if (context->file_stream) {
        fclose(context->file_stream);
        context->file_stream = NULL;
    }
    cRETURN;
}

iERR ion_cli_open_reader_file(ION_CLI_READER_CONTEXT *reader_context, std::string *file_path, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(file_path, NULL);
    if (file_path == NULL) {
        IONFAILSTATE(IERR_INVALID_ARG, "Filepath not provided to reader.", result);
    }
    reader_context->input_location = *file_path;
    reader_context->file_stream = fopen(file_path->c_str(), "rb");
    if (!reader_context->file_stream) {
        IONFAILSTATE(IERR_CANT_FIND_FILE, reader_context->input_location, result);
    }
    IONCREAD(ion_stream_open_file_in(reader_context->file_stream, &reader_context->ion_stream));
    IONCREAD(ion_reader_open(&reader_context->reader, reader_context->ion_stream, &reader_context->options));
    cRETURN;
}

iERR ion_cli_open_reader_stdin(ION_CLI_READER_CONTEXT *reader_context, IonEventResult *result) {
    iENTER;
    reader_context->input_location = "stdin";
    ION_SET_ERROR_CONTEXT(&reader_context->input_location, NULL);
    IONCREAD(ion_stream_open_stdin(&reader_context->ion_stream));
    IONCREAD(ion_reader_open(&reader_context->reader, reader_context->ion_stream, &reader_context->options));
    cRETURN;
}

iERR ion_cli_open_reader_memory(ION_CLI_READER_CONTEXT *reader_context, std::string *data, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(data, NULL);
    if (data == NULL) {
        IONFAILSTATE(IERR_INVALID_ARG, "Data not provided to reader.", result);
    }
    reader_context->input_location = *data;
    IONCREAD(ion_reader_open_buffer(&reader_context->reader, (BYTE *)data->c_str(), data->length(), &reader_context->options));
    cRETURN;
}

iERR ion_cli_open_reader_basic(ION_CLI_READER_CONTEXT *reader_context, ION_CLI_IO_TYPE input_format, std::string *file_path, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(file_path, NULL);

    switch (input_format) {
        case IO_TYPE_FILE:
            IONREPORT(ion_cli_open_reader_file(reader_context, file_path, result));
            break;
        case IO_TYPE_CONSOLE:
            IONREPORT(ion_cli_open_reader_stdin(reader_context, result));
            break;
        case IO_TYPE_MEMORY:
            IONREPORT(ion_cli_open_reader_memory(reader_context, file_path, result));
            break;
        default:
            IONFAILSTATE(IERR_INVALID_STATE, "Invalid input format.", result);
    }
    cRETURN;
}

iERR ion_cli_open_reader(ION_CLI_IO_TYPE input_format, ION_CATALOG *catalog, std::string *file_path,
                         ION_CLI_READER_CONTEXT *reader_context, IonEventStream *event_stream, IonEventResult *result) {
    iENTER;
    memset(reader_context, 0, sizeof(ION_CLI_READER_CONTEXT));
    ion_event_initialize_reader_options(&reader_context->options);
    reader_context->options.pcatalog = catalog;
    ion_event_register_symbol_table_callback(&reader_context->options, event_stream);

    IONREPORT(ion_cli_open_reader_basic(reader_context, input_format, file_path, result));

    cRETURN;
}

iERR ion_cli_add_shared_tables_to_catalog(std::string file_path, ION_CLI_IO_TYPE input_format, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&file_path, NULL);
    ION_TYPE type;
    ION_CLI_READER_CONTEXT reader_context;
    memset(&reader_context, 0, sizeof(ION_CLI_READER_CONTEXT));

    IONREPORT(ion_cli_open_reader_basic(&reader_context, input_format, &file_path, result));
    IONCREAD(ion_reader_next(reader_context.reader, &type));
    while (type != tid_EOF) {
        ION_SYMBOL_TABLE *symbol_table;
        IONCREAD(ion_symbol_table_load(reader_context.reader, catalog, &symbol_table));
        IONCREAD(ion_catalog_add_symbol_table(catalog, symbol_table));
        IONCREAD(ion_reader_next(reader_context.reader, &type));
    }
cleanup:
    UPDATEERROR(ion_cli_close_reader(&reader_context, result));
    iRETURN;
}

iERR ion_cli_create_catalog(std::vector<std::string> *catalog_paths, ION_CLI_IO_TYPE input_format, ION_CATALOG **catalog, IonEventResult *result) {
    iENTER;
    if (!catalog_paths->empty()) {
        ION_SET_ERROR_CONTEXT(&catalog_paths->at(0), NULL);
        IONCSTATE(ion_catalog_open(catalog), "Failed to open catalog.");
        for (size_t i = 0; i < catalog_paths->size(); i++) {
            IONREPORT(ion_cli_add_shared_tables_to_catalog(catalog_paths->at(i), input_format, *catalog, result));
        }
    }
    cRETURN;
}

ION_WRITER_OUTPUT_FORMAT ion_cli_output_type_from_arg(std::string output_format_arg) {
    if (output_format_arg == "binary") return ION_WRITER_OUTPUT_TYPE_BINARY;
    if (output_format_arg == "pretty" || output_format_arg == "events") return ION_WRITER_OUTPUT_TYPE_TEXT_PRETTY;
    return ION_WRITER_OUTPUT_TYPE_TEXT_UGLY;
}

iERR ion_cli_open_writer_basic(ION_CLI_IO_TYPE output_type, ION_WRITER_OUTPUT_FORMAT output_format, std::string output_destination, ION_EVENT_WRITER_CONTEXT *writer_context, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&output_destination, NULL);
    writer_context->options.output_as_binary = output_format == ION_WRITER_OUTPUT_TYPE_BINARY;
    writer_context->options.pretty_print = output_format == ION_WRITER_OUTPUT_TYPE_TEXT_PRETTY;
    writer_context->output_location = output_destination;

    switch (output_type) {
        case IO_TYPE_FILE:
            writer_context->file_stream = fopen(output_destination.c_str(), "wb");
            if (!writer_context->file_stream) {
                IONFAILSTATE(IERR_CANT_FIND_FILE, output_destination, result);
            }
            IONCWRITE(ion_stream_open_file_out(writer_context->file_stream, &writer_context->ion_stream));
            break;
        case IO_TYPE_CONSOLE:
            if (output_destination == "stdout") {
                IONCWRITE(ion_stream_open_stdout(&writer_context->ion_stream));
            }
            else if (output_destination == "stderr") {
                IONCWRITE(ion_stream_open_stderr(&writer_context->ion_stream));
            }
            else {
                IONFAILSTATE(IERR_INVALID_STATE, "Unknown console output destination.", result);
            }
            break;
        case IO_TYPE_MEMORY:
            IONCWRITE(ion_stream_open_memory_only(&writer_context->ion_stream));
            break;
        default:
            IONFAILSTATE(IERR_INVALID_STATE, "Unknown output type.", result);
    }

    IONCWRITE(ion_writer_open(&writer_context->writer, writer_context->ion_stream, &writer_context->options));
    cRETURN;
}

iERR ion_cli_open_writer(ION_CLI_COMMON_ARGS *common_args, ION_CATALOG *catalog, ION_COLLECTION *imports, ION_EVENT_WRITER_CONTEXT *writer_context, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    memset(writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    ion_event_initialize_writer_options(&writer_context->options);
    writer_context->options.pcatalog = catalog;
    if (imports && !ION_COLLECTION_IS_EMPTY(imports)) {
        IONCWRITE(ion_writer_options_initialize_shared_imports(&writer_context->options));
        IONCWRITE(ion_writer_options_add_shared_imports(&writer_context->options, imports));
    }

    IONCWRITE(ion_cli_open_writer_basic(common_args->output_type, ion_cli_output_type_from_arg(common_args->output_format), common_args->output, writer_context, result));
    cRETURN;
}

iERR ion_cli_close_writer(ION_EVENT_WRITER_CONTEXT *context, ION_CLI_IO_TYPE output_type, ION_STRING *output, IonEventResult *result) {
    iENTER;
    ASSERT(context);
    ION_SET_ERROR_CONTEXT(&context->output_location, NULL);
    if (output_type == IO_TYPE_MEMORY) {
        ASSERT(output);
        IONREPORT(ion_event_in_memory_writer_close(context, &output->value, &output->length, result));
        IONCLEANEXIT;
    }
    if (context->writer) {
        ION_NON_FATAL(ion_writer_close(context->writer), "Failed to close writer.");
        context->writer = NULL;
    }
    if (context->has_imports) {
        ION_NON_FATAL(ion_writer_options_close_shared_imports(&context->options), "Failed to close writer imports.");
        context->has_imports = FALSE;
    }
    if (context->ion_stream) {
        ION_NON_FATAL(ion_stream_close(context->ion_stream), "Failed to close ION_STREAM.");
        context->ion_stream = NULL;
    }
    if (context->file_stream) {
        fclose(context->file_stream);
        context->file_stream = NULL;
    }
    cRETURN;
}

iERR ion_cli_add_imports_to_collection(ION_COLLECTION *imports, ION_CLI_IO_TYPE input_format, std::string filepath, IonEventResult *result) {
    iENTER;
    ION_CLI_READER_CONTEXT reader_context;
    memset(&reader_context, 0, sizeof(ION_CLI_READER_CONTEXT));
    IONREPORT(ion_cli_open_reader_basic(&reader_context, input_format, &filepath, result));
    IONREPORT(ion_event_stream_read_imports(reader_context.reader, imports, &filepath, result));
cleanup:
    UPDATEERROR(ion_cli_close_reader(&reader_context, result));
    iRETURN;
}

iERR ion_cli_create_imports(std::vector<std::string> *import_files, ION_CLI_IO_TYPE input_format, ION_COLLECTION **imports, IonEventResult *result) {
    iENTER;
    ION_COLLECTION *p_imports;
    ASSERT(imports);

    if (!import_files->empty()) {
        *imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
        p_imports = *imports;
        _ion_collection_initialize(p_imports, p_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
        for (size_t i = 0; i < import_files->size(); i++) {
            IONREPORT(ion_cli_add_imports_to_collection(p_imports, input_format, import_files->at(i), result));
        }
    }
    cRETURN;
}

iERR ion_cli_open_event_writer(ION_CLI_COMMON_ARGS *args, ION_EVENT_WRITER_CONTEXT *writer_context, IonEventResult *result) {
    iENTER;
    memset(writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    IONREPORT(ion_cli_open_writer_basic(args->output_type, ion_cli_output_type_from_arg(args->output_format), args->output, writer_context, result));
    IONREPORT(ion_writer_write_symbol(writer_context->writer, &ion_event_stream_marker));
    cRETURN;
}

iERR ion_cli_write_value(ION_CLI_COMMON_ARGS *args, ION_EVENT_WRITER_CONTEXT *writer_context, ION_CATALOG *catalog, std::string file_path, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&file_path, NULL);
    ION_CLI_READER_CONTEXT reader_context;
    IonEventStream stream(file_path);
    IONREPORT(ion_cli_open_reader(args->inputs_format, catalog, &file_path, &reader_context, &stream, result));
    if (args->output_format == "events") {
        // NOTE: don't short-circuit on read failure. Write as many events as possible.
        err = ion_event_stream_read_all(reader_context.reader, catalog, &stream, result);
        UPDATEERROR(ion_event_stream_write_all_events(writer_context->writer, &stream, catalog, result));
        IONREPORT(err);
    }
    else if (args->output_format != "none"){
        IONREPORT(ion_event_stream_read_all(reader_context.reader, catalog, &stream, result));
        IONREPORT(ion_event_stream_write_all(writer_context->writer, &stream, result));
        // Would be nice to use this (especially for performance testing), but having to peek at the first value in the
        // stream (to identify $ion_event_stream) rules it out.
        //IONCHECK(ion_writer_write_all_values(writer_context->writer, reader_context.reader));
    }
cleanup:
    UPDATEERROR(ion_cli_close_reader(&reader_context, result));
    iRETURN;
}

iERR ion_cli_command_process_standard(ION_EVENT_WRITER_CONTEXT *writer_context, ION_CLI_COMMON_ARGS *common_args, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    if (common_args->inputs_format == IO_TYPE_CONSOLE) {
        IONREPORT(ion_cli_write_value(common_args, writer_context, catalog, "stdin", result));
    }
    else {
        for (size_t i = 0; i < common_args->input_files.size(); i++) {
            IONREPORT(ion_cli_write_value(common_args, writer_context, catalog, common_args->input_files.at(i), result));
        }
    }
    cRETURN;
}

iERR ion_cli_command_process(ION_CLI_COMMON_ARGS *common_args, ION_CLI_PROCESS_ARGS *process_args, ION_STRING *output, IonEventReport *report) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    IonEventResult _result, *result = &_result;
    ION_EVENT_WRITER_CONTEXT writer_context;
    memset(&writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    ION_CATALOG *catalog = NULL;
    ION_COLLECTION *imports = NULL;

    IONREPORT(ion_cli_create_catalog(&common_args->catalogs, common_args->catalogs_format, &catalog, result));
    IONREPORT(ion_cli_create_imports(&process_args->imports, process_args->imports_format, &imports, result));
    if (common_args->output_format == "events") {
        IONREPORT(ion_cli_open_event_writer(common_args, &writer_context, result));
    }
    else if (common_args->output_format != "none") {
        IONREPORT(ion_cli_open_writer(common_args, catalog, imports, &writer_context, result));
    }
    else {
        // TODO how should "no output" be conveyed?
        IONFAILSTATE(IERR_NOT_IMPL, "output-format = none not yet implemented.", result);
    }
    if (!process_args->perf_report.empty()) {
        // TODO add perf report support
        IONFAILSTATE(IERR_NOT_IMPL, "Performance reporting not yet implemented.", result);
    }

    if (!process_args->filter.empty()) {
        IONREPORT(ion_cli_command_process_filter(&writer_context, common_args, process_args, catalog, result));
        IONCLEANEXIT;
    }
    if (!process_args->traverse.empty()) {
        IONREPORT(ion_cli_command_process_traverse(&writer_context, common_args, process_args, catalog, result));
        IONCLEANEXIT;
    }

    // Full traversal, no filtering.
    IONREPORT(ion_cli_command_process_standard(&writer_context, common_args, catalog, result));

cleanup:
    UPDATEERROR(ion_cli_close_writer(&writer_context, common_args->output_type, output, result));
    if (catalog) {
        ION_NON_FATAL(ion_catalog_close(catalog), "Failed to close catalog.");
    }
    if (imports) {
        ion_free_owner(imports);
    }
    report->addResult(result);
    iRETURN;
}

typedef iERR (*ION_CLI_WRITE_FUNC)(hWRITER writer, IonEventReport *report, std::string *location, ION_CATALOG *catalog, IonEventResult *result);

iERR ion_cli_write_report(IonEventReport *report, std::string report_destination, ION_CLI_IO_TYPE report_type, ION_CLI_WRITE_FUNC write_func, ION_STRING *output, ION_CATALOG *catalog=NULL, IonEventResult *result=NULL) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&report_destination, NULL);
    ION_EVENT_WRITER_CONTEXT writer_context;
    memset(&writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    IONREPORT(ion_cli_open_writer_basic(report_type, ION_WRITER_OUTPUT_TYPE_TEXT_PRETTY, report_destination, &writer_context, result));
    IONREPORT(write_func(writer_context.writer, report, &report_destination, catalog, result));
cleanup:
    UPDATEERROR(ion_cli_close_writer(&writer_context, report_type, output, result));
    iRETURN;
}

iERR ion_cli_write_error_report(IonEventReport *report, ION_CLI_COMMON_ARGS *common_args) {
    iENTER;
    IONREPORT(ion_cli_write_report(report, common_args->error_report, common_args->error_report_type, ion_event_stream_write_error_report, NULL));
    cRETURN;
}

inline BOOL ion_cli_command_compare_streams(ION_EVENT_COMPARISON_TYPE comparison_type, IonEventStream *lhs, IonEventStream *rhs, IonEventResult *result) {
    return (comparison_type == COMPARISON_TYPE_BASIC)
           ? assertIonEventStreamEq(lhs, rhs, result) : testComparisonSets(lhs, rhs, comparison_type, result);
}

iERR ion_cli_command_compare_standard(ION_CLI_COMMON_ARGS *common_args, ION_EVENT_COMPARISON_TYPE comparison_type, ION_CATALOG *catalog, IonEventReport *report, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    IonEventStream **streams = NULL;
    ION_CLI_READER_CONTEXT *reader_contexts = NULL;
    size_t num_inputs = common_args->input_files.size();

    streams = (IonEventStream**)calloc(num_inputs, sizeof(IonEventStream*));
    reader_contexts = (ION_CLI_READER_CONTEXT *)calloc(num_inputs, sizeof(ION_CLI_READER_CONTEXT));

    for (size_t i = 0; i < num_inputs; i++) {
        streams[i] = new IonEventStream(common_args->input_files.at(i));
        IONREPORT(ion_cli_open_reader(common_args->inputs_format, catalog, &common_args->input_files.at(i), &reader_contexts[i], streams[i], result));
        IONREPORT(ion_event_stream_read_all(reader_contexts[i].reader, catalog, streams[i], result));
    }

    for (size_t i = 0; i < num_inputs; i++) {
        for (size_t j = 0; j < num_inputs; j++) {
            IonEventResult compare_result;
            if (!ion_cli_command_compare_streams(comparison_type, streams[i], streams[j], &compare_result)) {
                report->addResult(&compare_result);
            }
        }
    }
cleanup:
    if (streams) {
        for (size_t i = 0; i < num_inputs; i++) {
            delete streams[i];
        }
        free(streams);
    }
    if (reader_contexts) {
        free(reader_contexts);
    }
    iRETURN;
}

iERR ion_cli_command_compare(ION_CLI_COMMON_ARGS *common_args, ION_EVENT_COMPARISON_TYPE comparison_type, ION_STRING *output, IonEventReport *report) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    IonEventResult _result, *result = &_result;
    ION_CATALOG *catalog = NULL;

    if (comparison_type == COMPARISON_TYPE_UNKNOWN) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid argument: comparison-type must be in (basic, equivs, nonequivs).", result);
    }

    IONREPORT(ion_cli_create_catalog(&common_args->catalogs, common_args->catalogs_format, &catalog, result));
    IONREPORT(ion_cli_command_compare_standard(common_args, comparison_type, catalog, report, result));

cleanup:
    if (report->hasComparisonFailures()
        && !report->hasErrors()
        && !result->has_error_description) { // If there are errors, comparison failures are just noise.
        err = ion_cli_write_report(report, common_args->output, common_args->output_type, ion_event_stream_write_comparison_report, output, catalog, result);
    }
    if (catalog) {
        ION_NON_FATAL(ion_catalog_close(catalog), "Failed to close catalog.");
    }
    report->addResult(result);
    iRETURN;
}

