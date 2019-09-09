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
#include <ionc/ion_errors.h>
#include <ionc/ion.h>
#include "ion_helpers.h"
#include "ion_event_util.h"
#include "ion_event_stream_impl.h"
#include "ion_event_equivalence.h"
#include "cli.h"

/**
 * Stores the resources required by an ION_READER.
 */
class IonCliReaderContext {
public:
    ION_READER_OPTIONS options;
    hREADER reader;
    FILE *file_stream;
    std::string input_location;
    ION_STREAM *ion_stream;

    IonCliReaderContext() {
        ion_event_initialize_reader_options(&options);
        reader = NULL;
        file_stream = NULL;
        ion_stream = NULL;
    }
};

iERR ion_cli_command_process_filter(IonEventWriterContext *writer_context, IonCliCommonArgs *common_args,
                                    IonCliProcessArgs *process_args, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    // TODO implement jq-style filtering.
    IONFAILSTATE(IERR_NOT_IMPL, "Filtering not yet implemented.");
    cRETURN;
}

iERR ion_cli_command_process_traverse(IonEventWriterContext *writer_context, IonCliCommonArgs *common_args,
                                      IonCliProcessArgs *process_args, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    // TODO implement custom read traversal.
    IONFAILSTATE(IERR_NOT_IMPL, "Custom traversals not yet implemented.");
    cRETURN;
}

iERR ion_cli_close_reader(IonCliReaderContext *context, iERR err, IonEventResult *result) {
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

iERR ion_cli_open_reader_file(IonCliReaderContext *reader_context, std::string *location, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(location, NULL);
    if (location == NULL) {
        IONFAILSTATE(IERR_INVALID_ARG, "Filepath not provided to reader.");
    }
    reader_context->input_location = *location;
    reader_context->file_stream = fopen(location->c_str(), "rb");
    if (!reader_context->file_stream) {
        IONFAILSTATE(IERR_CANT_FIND_FILE, reader_context->input_location);
    }
    IONCREAD(ion_stream_open_file_in(reader_context->file_stream, &reader_context->ion_stream));
    IONCREAD(ion_reader_open(&reader_context->reader, reader_context->ion_stream, &reader_context->options));
    cRETURN;
}

iERR ion_cli_open_reader_stdin(IonCliReaderContext *reader_context, IonEventResult *result) {
    iENTER;
    reader_context->input_location = "stdin";
    ION_SET_ERROR_CONTEXT(&reader_context->input_location, NULL);
    IONCREAD(ion_stream_open_stdin(&reader_context->ion_stream));
    IONCREAD(ion_reader_open(&reader_context->reader, reader_context->ion_stream, &reader_context->options));
    cRETURN;
}

iERR ion_cli_open_reader_memory(IonCliReaderContext *reader_context, std::string *data, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(data, NULL);
    if (data == NULL) {
        IONFAILSTATE(IERR_INVALID_ARG, "Data not provided to reader.");
    }
    reader_context->input_location = *data;
    IONCREAD(ion_reader_open_buffer(&reader_context->reader, (BYTE *)data->c_str(), data->length(),
                                    &reader_context->options));
    cRETURN;
}

iERR ion_cli_open_reader_basic(IonCliReaderContext *reader_context, IonCliIO *input, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&input->contents, NULL);

    switch (input->type) {
        case IO_TYPE_FILE:
            IONREPORT(ion_cli_open_reader_file(reader_context, &input->contents, result));
            break;
        case IO_TYPE_CONSOLE:
            IONREPORT(ion_cli_open_reader_stdin(reader_context, result));
            break;
        case IO_TYPE_MEMORY:
            IONREPORT(ion_cli_open_reader_memory(reader_context, &input->contents, result));
            break;
        default:
            IONFAILSTATE(IERR_INVALID_STATE, "Invalid input format.");
    }
    cRETURN;
}

iERR ion_cli_open_reader(IonCliIO *input, ION_CATALOG *catalog,
                         IonCliReaderContext *reader_context, IonEventStream *event_stream, IonEventResult *result) {
    iENTER;
    reader_context->options.pcatalog = catalog;
    ion_event_register_symbol_table_callback(&reader_context->options, event_stream);

    IONREPORT(ion_cli_open_reader_basic(reader_context, input, result));

    cRETURN;
}

iERR ion_cli_add_shared_tables_to_catalog(IonCliIO *catalog_input, ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&catalog_input->contents, NULL);
    ION_TYPE type;
    IonCliReaderContext reader_context;

    IONREPORT(ion_cli_open_reader_basic(&reader_context, catalog_input, result));
    IONCREAD(ion_reader_next(reader_context.reader, &type));
    while (type != tid_EOF) {
        ION_SYMBOL_TABLE *symbol_table;
        IONCREAD(ion_symbol_table_load(reader_context.reader, catalog, &symbol_table));
        IONCREAD(ion_catalog_add_symbol_table(catalog, symbol_table));
        IONCREAD(ion_reader_next(reader_context.reader, &type));
    }
cleanup:
    UPDATEERROR(ion_cli_close_reader(&reader_context, err, result));
    iRETURN;
}

iERR ion_cli_create_catalog(std::vector<IonCliIO> *catalogs, ION_CATALOG **catalog, IonEventResult *result) {
    iENTER;
    if (!catalogs->empty()) {
        ION_SET_ERROR_CONTEXT(&catalogs->at(0).contents, NULL);
        IONCSTATE(ion_catalog_open(catalog), "Failed to open catalog.");
        for (size_t i = 0; i < catalogs->size(); i++) {
            IONREPORT(ion_cli_add_shared_tables_to_catalog(&catalogs->at(i), *catalog, result));
        }
    }
    cRETURN;
}

iERR ion_cli_open_writer_basic(IonCliIO *destination, ION_EVENT_OUTPUT_TYPE output_format,
                               IonEventWriterContext *writer_context, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&destination->contents, NULL);
    writer_context->options.output_as_binary = output_format == OUTPUT_TYPE_BINARY;
    writer_context->options.pretty_print = output_format == OUTPUT_TYPE_TEXT_PRETTY
                                           || output_format == OUTPUT_TYPE_EVENTS;
    writer_context->output_location = destination->contents;

    switch (destination->type) {
        case IO_TYPE_FILE:
            writer_context->file_stream = fopen(destination->contents.c_str(), "wb");
            if (!writer_context->file_stream) {
                IONFAILSTATE(IERR_CANT_FIND_FILE, destination->contents);
            }
            IONCWRITE(ion_stream_open_file_out(writer_context->file_stream, &writer_context->ion_stream));
            break;
        case IO_TYPE_CONSOLE:
            if (destination->contents == "stdout") {
                IONCWRITE(ion_stream_open_stdout(&writer_context->ion_stream));
            }
            else if (destination->contents == "stderr") {
                IONCWRITE(ion_stream_open_stderr(&writer_context->ion_stream));
            }
            else {
                IONFAILSTATE(IERR_INVALID_STATE, "Unknown console output destination.");
            }
            break;
        case IO_TYPE_MEMORY:
            IONCWRITE(ion_stream_open_memory_only(&writer_context->ion_stream));
            break;
        default:
            IONFAILSTATE(IERR_INVALID_STATE, "Unknown output type.");
    }

    IONCWRITE(ion_writer_open(&writer_context->writer, writer_context->ion_stream, &writer_context->options));
    cRETURN;
}

iERR ion_cli_open_writer(IonCliCommonArgs *common_args, ION_CATALOG *catalog, ION_COLLECTION *imports,
                         IonEventWriterContext *writer_context, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    writer_context->options.pcatalog = catalog;
    if (imports && !ION_COLLECTION_IS_EMPTY(imports)) {
        IONCWRITE(ion_writer_options_initialize_shared_imports(&writer_context->options));
        IONCWRITE(ion_writer_options_add_shared_imports(&writer_context->options, imports));
    }

    IONCWRITE(ion_cli_open_writer_basic(&common_args->output, common_args->output_format, writer_context, result));
    cRETURN;
}

iERR ion_cli_close_writer(IonEventWriterContext *context, ION_CLI_IO_TYPE output_type, ION_STRING *output, iERR err,
                          IonEventResult *result) {
    UPDATEERROR(ion_event_writer_close(context, result, err, output_type == IO_TYPE_MEMORY, &output->value,
                                       &output->length));
    cRETURN;
}

iERR ion_cli_add_imports_to_collection(ION_COLLECTION *imports, IonCliIO *import, IonEventResult *result) {
    iENTER;
    IonCliReaderContext reader_context;
    IONREPORT(ion_cli_open_reader_basic(&reader_context, import, result));
    IONREPORT(ion_event_stream_read_imports(reader_context.reader, imports, &import->contents, result));
cleanup:
    UPDATEERROR(ion_cli_close_reader(&reader_context, err, result));
    iRETURN;
}

iERR ion_cli_create_imports(std::vector<IonCliIO> *import_inputs, ION_COLLECTION **imports, IonEventResult *result) {
    iENTER;
    ION_COLLECTION *p_imports;
    ASSERT(imports);

    if (!import_inputs->empty()) {
        *imports = (ION_COLLECTION *)ion_alloc_owner(sizeof(ION_COLLECTION));
        p_imports = *imports;
        _ion_collection_initialize(p_imports, p_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
        for (size_t i = 0; i < import_inputs->size(); i++) {
            IONREPORT(ion_cli_add_imports_to_collection(p_imports, &import_inputs->at(i), result));
        }
    }
    cRETURN;
}

iERR ion_cli_open_event_writer(IonCliCommonArgs *args, IonEventWriterContext *writer_context, IonEventResult *result) {
    iENTER;
    IONREPORT(ion_cli_open_writer_basic(&args->output, args->output_format, writer_context, result));
    IONREPORT(ion_writer_write_symbol(writer_context->writer, &ion_event_stream_marker));
    cRETURN;
}

iERR ion_cli_write_input(IonCliCommonArgs *args, IonEventWriterContext *writer_context, IonCliIO *input,
                         ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&input->contents, NULL);
    IonCliReaderContext reader_context;
    IonEventStream stream(input->contents);
    IONREPORT(ion_cli_open_reader(input, catalog, &reader_context, &stream, result));
    if (args->output_format == OUTPUT_TYPE_EVENTS) {
        // NOTE: don't short-circuit on read failure. Write as many events as possible.
        err = ion_event_stream_read_all(reader_context.reader, catalog, &stream, result);
        UPDATEERROR(ion_event_stream_write_all_events(writer_context->writer, &stream, catalog, result));
        IONREPORT(err);
    }
    else if (args->output_format != OUTPUT_TYPE_NONE){
        IONREPORT(ion_event_stream_read_all(reader_context.reader, catalog, &stream, result));
        IONREPORT(ion_event_stream_write_all(writer_context->writer, &stream, result));
        // Would be nice to use this (especially for performance testing), but having to peek at the first value in the
        // stream (to identify $ion_event_stream) rules it out.
        //IONCHECK(ion_writer_write_all_values(writer_context->writer, reader_context.reader));
    }
cleanup:
    UPDATEERROR(ion_cli_close_reader(&reader_context, err, result));
    iRETURN;
}

iERR ion_cli_command_process_standard(IonEventWriterContext *writer_context, IonCliCommonArgs *common_args,
                                      ION_CATALOG *catalog, IonEventResult *result) {
    iENTER;
    for (size_t i = 0; i < common_args->input_files.size(); i++) {
        IONREPORT(ion_cli_write_input(common_args, writer_context, &common_args->input_files.at(i), catalog, result));
    }
    cRETURN;
}

iERR ion_cli_command_process(IonCliCommonArgs *common_args, IonCliProcessArgs *process_args, ION_STRING *output,
                             IonEventReport *report) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    IonEventResult _result, *result = &_result;
    IonEventWriterContext writer_context;
    ION_CATALOG *catalog = NULL;
    ION_COLLECTION *imports = NULL;

    IONREPORT(ion_cli_create_catalog(&common_args->catalogs, &catalog, result));
    IONREPORT(ion_cli_create_imports(&process_args->imports, &imports, result));
    if (common_args->output_format == OUTPUT_TYPE_EVENTS) {
        // Event writers do not need to write with shared symbol tables, and they do not need catalogs. Their temporary
        // scalar writers, however, do need catalogs so that they can attempt to resolve symbol values with unknown
        // text. These catalogs should be passed to ion_event_stream_write_all_events.
        IONREPORT(ion_cli_open_event_writer(common_args, &writer_context, result));
    }
    else if (common_args->output_format != OUTPUT_TYPE_NONE) {
        // Ion stream writers receive their catalogs and shared symbol table imports lists at construction.
        IONREPORT(ion_cli_open_writer(common_args, catalog, imports, &writer_context, result));
    }
    else {
        // TODO add "silent" support.
        IONFAILSTATE(IERR_NOT_IMPL, "output-format = none not yet implemented.");
    }
    if (!process_args->perf_report.contents.empty()) {
        // TODO add perf report support
        IONFAILSTATE(IERR_NOT_IMPL, "Performance reporting not yet implemented.");
    }

    if (!process_args->filter.empty()) {
        IONREPORT(ion_cli_command_process_filter(&writer_context, common_args, process_args, catalog, result));
        IONCLEANEXIT;
    }
    if (!process_args->traverse.contents.empty()) {
        IONREPORT(ion_cli_command_process_traverse(&writer_context, common_args, process_args, catalog, result));
        IONCLEANEXIT;
    }

    // Full traversal, no filtering.
    IONREPORT(ion_cli_command_process_standard(&writer_context, common_args, catalog, result));

cleanup:
    UPDATEERROR(ion_cli_close_writer(&writer_context, common_args->output.type, output, err, result));
    if (catalog) {
        ION_NON_FATAL(ion_catalog_close(catalog), "Failed to close catalog.");
    }
    if (imports) {
        ion_free_owner(imports);
    }
    report->addResult(result);
    iRETURN;
}

typedef iERR (*ION_CLI_WRITE_FUNC)(hWRITER writer, IonEventReport *report, ION_CATALOG *catalog, std::string *location,
                                   IonEventResult *result);

iERR ion_event_stream_write_error_report(hWRITER writer, IonEventReport *report, ION_EVENT_WRITER_PARAMS) {
    iENTER;
    ASSERT(report);
    IONREPORT(report->writeErrorsTo(writer));
    cRETURN;
}

iERR ion_event_stream_write_comparison_report(hWRITER writer, IonEventReport *report, ION_EVENT_WRITER_PARAMS) {
    iENTER;
    ASSERT(report);
    IONREPORT(report->writeComparisonResultsTo(writer, ION_EVENT_WRITER_ARGS));
    cRETURN;
}

iERR ion_cli_write_report(IonEventReport *report, IonCliIO *report_destination, ION_CLI_WRITE_FUNC write_func,
                          ION_STRING *output, ION_CATALOG *catalog=NULL, IonEventResult *ION_RESULT_ARG=NULL) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&report_destination->contents, NULL);
    IonEventWriterContext writer_context;
    IONREPORT(ion_cli_open_writer_basic(report_destination, OUTPUT_TYPE_TEXT_PRETTY, &writer_context, ION_RESULT_ARG));
    IONREPORT(write_func(writer_context.writer, report, catalog,  &report_destination->contents, ION_RESULT_ARG));
cleanup:
    UPDATEERROR(ion_cli_close_writer(&writer_context, report_destination->type, output, err, ION_RESULT_ARG));
    iRETURN;
}

iERR ion_cli_write_error_report(IonEventReport *report, IonCliCommonArgs *common_args) {
    iENTER;
    IONREPORT(ion_cli_write_report(report, &common_args->error_report, ion_event_stream_write_error_report, NULL));
    cRETURN;
}

inline BOOL ion_cli_command_compare_streams(ION_EVENT_COMPARISON_TYPE comparison_type, IonEventStream *lhs,
                                            IonEventStream *rhs, IonEventResult *result) {
    return (comparison_type == COMPARISON_TYPE_BASIC)
           ? ion_compare_streams(lhs, rhs, result) : ion_compare_sets(lhs, rhs, comparison_type, result);
}

iERR ion_cli_command_compare_standard(IonCliCommonArgs *common_args, ION_EVENT_COMPARISON_TYPE comparison_type,
                                      ION_CATALOG *catalog, IonEventReport *report, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    IonEventStream **streams = NULL;
    IonCliReaderContext **reader_contexts = NULL;
    size_t num_inputs = common_args->input_files.size();

    streams = (IonEventStream**)calloc(num_inputs, sizeof(IonEventStream*));
    reader_contexts = (IonCliReaderContext **)calloc(num_inputs, sizeof(IonCliReaderContext *));

    for (size_t i = 0; i < num_inputs; i++) {
        streams[i] = new IonEventStream(common_args->input_files.at(i).contents);
        reader_contexts[i] = new IonCliReaderContext();
        IONREPORT(ion_cli_open_reader(&common_args->input_files.at(i), catalog, reader_contexts[i], streams[i],
                                      result));
        IONREPORT(ion_event_stream_read_all(reader_contexts[i]->reader, catalog, streams[i], result));
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
        for (size_t i = 0; i < num_inputs; i++) {
            delete reader_contexts[i];
        }
        free(reader_contexts);
    }
    iRETURN;
}

iERR ion_cli_command_compare(IonCliCommonArgs *common_args, ION_EVENT_COMPARISON_TYPE comparison_type,
                             ION_STRING *output, IonEventReport *report) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    IonEventResult _result, *result = &_result;
    ION_CATALOG *catalog = NULL;

    if (comparison_type == COMPARISON_TYPE_UNKNOWN) {
        IONFAILSTATE(IERR_INVALID_ARG, "Invalid argument: comparison-type must be in (basic, equivs, nonequivs).");
    }

    IONREPORT(ion_cli_create_catalog(&common_args->catalogs, &catalog, result));
    IONREPORT(ion_cli_command_compare_standard(common_args, comparison_type, catalog, report, result));

cleanup:
    if (report->hasComparisonFailures()
        && !report->hasErrors()
        && !result->has_error_description) { // If there are errors, comparison failures are just noise.
        err = ion_cli_write_report(report, &common_args->output, ion_event_stream_write_comparison_report, output,
                                   catalog, result);
    }
    if (catalog) {
        ION_NON_FATAL(ion_catalog_close(catalog), "Failed to close catalog.");
    }
    report->addResult(result);
    iRETURN;
}

void ion_cli_free_command_output(ION_STRING *output) {
    if (!output) return;
    if (output->value) {
        free(output->value);
    }
    output->value = NULL;
}

