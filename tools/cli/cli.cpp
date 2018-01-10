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
#include "docopt/docopt.h"
#include "cli.h"

static const char USAGE[] =
R"(
ion-c CLI

Usage:
    ion process [--output <file>] [--error-report <file>] [--output-format <type>] [--catalog <file>]... [--imports <file>]... [--perf-report <file>] [--filter <filter> | --traverse <file>] (- | <input_file>...)
    ion compare [--output <file>] [--error-report <file>] [--output-format <type>] [--catalog <file>]... [--comparison-type <type>] (- | <input_file>...)
    ion extract [--output <file>] [--error-report <file>] [--output-format <type>] (--symtab-name <name>) (--symtab-version <version>) (- | <input_file>...)
    ion help [extract | compare | process]
    ion --help
    ion version
    ion --version

)";

static const char COMMANDS[] =
R"(
Commands:
    extract     Extract the symbols from the given input(s) into a shared symbol table with the given name and
                version.

    compare     Compare all inputs (which may contain Ion streams and/or EventStreams) against all other inputs
                using the Ion data model's definition of equality. Write a ComparisonReport to the output.

    process     Read the input file(s) (optionally, specifying ReadInstructions or a filter) and re-write in the
                format specified by --output.

    help        Prints this general help. If provided a command, prints help specific to that command.

    version     Prints version information about this tool.

)";

static const char OPTIONS[] =
R"(
Options:
    -o, --output <file>             Output location. [default: stdout]
    -f, --output-format <type>      Output format, from the set (text | pretty | binary | events | none). 'events' is only available with the 'process' command, and outputs a serialized EventStream representing the input Ion stream(s). [default: pretty]
    -e, --error-report <file>       ErrorReport location. [default: stderr]
    -p, --perf-report <file>        PerformanceReport location. If left unspecified, a performance report is not generated.
    -c, --catalog <file>            Location(s) of files containing Ion streams of shared symbol tables from which to populate a catalog. This catalog will be used by all readers and writers when encountering shared symbol table import descriptors.
    -i, --imports <file>            Location(s) of files containing list(s) of shared symbol table import descriptors. These imports will be used by writers during serialization. If a catalog is available (see: --catalog), the writer will attempt to match those import descriptors to actual shared symbol tables using the catalog.
    -F, --filter <filter>           JQ-style filter to perform on the input stream(s) before writing the result.
    -t, --traverse <file>           Location of a file containing a stream of ReadInstructions to use when reading the input stream(s) instead of performing a full traversal.
    -n, --symtab-name <name>        Name of the shared symbol table to be extracted.
    -v, --symtab-version <version>  Version of the shared symbol table to be extracted.
    -y, --comparison-type <type>    Comparison semantics to be used with the compare command, from the set (basic | equivs | nonequivs). Any embedded streams in the inputs are compared for EventStream equality. 'basic' performs a standard data-model comparison between the corresponding events (or embedded streams) in the inputs. 'equivs' verifies that each value (or embedded stream) in a top-level sequence is equivalent to every other value (or embedded stream) in that sequence. 'nonequivs' does the same, but verifies that the values (or embedded streams) are not equivalent. 'equiv-timeline' is the same as 'equivs', except that when top-level sequences contain timestamp values, they are considered equivalent if they represent the same instant regardless of whether they are considered equivalent by the Ion data model. [default: basic]
    -h, --help                      Synonym for the help command.
    --version                       Synonym for the version command.

)";

static const char EXAMPLES[] =
R"(
Examples:
    Read input.10n and pretty-print it to stdout.
        $ ion process input.10n

    Read input.ion (using a catalog comprised of the shared symbol tables contained in catalog.10n) without
    re-writing, and write a performance report to stdout.
        $ ion process --output-format none --catalog catalog.10n --perf-report -- input.10n

    Read input.10n according to the ReadInstructions specified by instructions.ion and write the resulting Events
    to output.ion.
        $ ion process -o output.ion -f events -t instructions.ion input.10n

    Extract a shared symbol table with name "foo_table" and version 1 from the piped Ion stream and write it in
    binary format to foo_table.10n.
        $ echo 'foo' | ion extract -n 'foo_table' -V 1 -o foo_table.10n -f binary -

    Read input1.ion and input2.10n and output to stdout any values in the streams that match the filter .foo.
        $ ion process --filter .foo input1.ion input2.10n

    Compare each stream in read_events.ion, input1.ion, and input2.10n against all other streams in the set and
    output a ComparisonReport to comparison_report.ion.
        $ ion compare -o comparison_report.ion read_events.ion input1.ion input2.10n
)";

static bool ion_cli_has_flag(std::map<std::string, docopt::value> *args, std::string arg) {
    std::map<std::string, docopt::value>::iterator it = args->find(arg);
    return it != args->end() && it->second.asBool();
}

static bool ion_cli_has_value(std::map<std::string, docopt::value> *args, std::string arg) {
    std::map<std::string, docopt::value>::iterator it = args->find(arg);
    if (it == args->end()) return false;
    docopt::value val = it->second;
    return val.isString() || val.isBool() || val.isLong() || val.isStringList(); // Otherwise, the value is Empty.
}

inline void ion_cli_print_help() {
    // TODO specific help based on command (if any)
    std::cout << USAGE << COMMANDS << OPTIONS << EXAMPLES << std::endl;
}

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
    IONREPORT(ion_writer_write_symbol(writer_context->writer, &ion_cli_event_stream_symbol));
    cRETURN;
}

iERR ion_cli_write_value(ION_CLI_COMMON_ARGS *args, ION_EVENT_WRITER_CONTEXT *writer_context, ION_CATALOG *catalog, std::string *file_path, IonEventResult *result) {
    iENTER;
    ION_SET_ERROR_CONTEXT(file_path, NULL);
    ION_CLI_READER_CONTEXT reader_context;
    IonEventStream stream(*file_path);
    IONREPORT(ion_cli_open_reader(args->inputs_format, catalog, file_path, &reader_context, &stream, result));
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
        IONREPORT(ion_cli_write_value(common_args, writer_context, catalog, NULL, result));
    }
    else {
        for (size_t i = 0; i < common_args->input_files.size(); i++) {
            IONREPORT(ion_cli_write_value(common_args, writer_context, catalog, &common_args->input_files.at(i), result));
        }
    }
    cRETURN;
}

iERR ion_cli_args_common(std::map<std::string, docopt::value> *args, ION_CLI_COMMON_ARGS *common_args, IonEventReport *report) {
    iENTER;
    common_args->output = args->find("--output")->second.asString();
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    IonEventResult result;
    if (common_args->output == "stdout" || common_args->output == "stderr") {
        common_args->output_type = IO_TYPE_CONSOLE;
    }
    common_args->output_format = args->find("--output-format")->second.asString();
    common_args->error_report = args->find("--error-report")->second.asString();
    if (common_args->error_report == "stdout" || common_args->error_report == "stderr") {
        common_args->error_report_type = IO_TYPE_CONSOLE;
    }
    if (ion_cli_has_value(args, "--catalog")) {
        common_args->catalogs = args->find("--catalog")->second.asStringList();
    }
    if (ion_cli_has_flag(args, "-")) {
        common_args->inputs_format = IO_TYPE_CONSOLE;
    }
    else {
        common_args->input_files = args->find("<input_file>")->second.asStringList();
    }
    if (!common_args->inputs_format == IO_TYPE_CONSOLE && common_args->input_files.empty()) {
        IONFAILSTATE(IERR_INVALID_ARG, "Input not specified.", &result);
    }
cleanup:
    if (result.has_error_description) {
        report->addResult(&result);
    }
    iRETURN;
}

iERR ion_cli_args_process(std::map<std::string, docopt::value> *args, ION_CLI_PROCESS_ARGS *process_args) {
    iENTER;
    memset(process_args, 0, sizeof(ION_CLI_PROCESS_ARGS));
    if (ion_cli_has_value(args, "--perf-report")) {
        process_args->perf_report = args->find("--perf-report")->second.asString();
    }
    if (ion_cli_has_value(args, "--filter")) {
        process_args->filter = args->find("--filter")->second.asString();
    }
    if (ion_cli_has_value(args, "--traverse")) {
        process_args->traverse = args->find("--traverse")->second.asString();
    }
    if (ion_cli_has_value(args, "--imports")) {
        process_args->imports = args->find("--imports")->second.asStringList();
    }
    cRETURN;
}

iERR ion_cli_command_process(ION_CLI_COMMON_ARGS *common_args, ION_CLI_PROCESS_ARGS *process_args, ION_STRING *output, IonEventReport *report) {
    iENTER;
    ION_SET_ERROR_CONTEXT(&common_args->output, NULL);
    //bool has_filter = ion_cli_has_value(args, "--filter");
    //bool has_traverse = ion_cli_has_value(args, "--traverse");
    IonEventResult _result, *result = &_result;
    ION_EVENT_WRITER_CONTEXT writer_context;
    memset(&writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    ION_CATALOG *catalog = NULL;
    ION_COLLECTION *imports = NULL;

    /* TODO verify that these verifications are performed by docopt.
    if (ion_cli_has_value(args, "--symtab-version") || ion_cli_has_value(args, "--symtab-name")) {
        FAILWITHMSG(IERR_INVALID_ARG, "--symtab-version and --symtab-name are incompatible with the process command.")
    }
    if (has_filter && has_traverse) FAILWITHMSG(IERR_INVALID_ARG, "--filter and --traverse are mutually exclusive.");
    */

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

COMPARISON_TYPE ion_cli_comparison_type_from_string(std::string type_str) {
    if ("basic" == type_str) {
        return COMPARISON_TYPE_BASIC;
    }
    if ("equivs" == type_str) {
        return COMPARISON_TYPE_EQUIVS;
    }
    if ("nonequivs" == type_str) {
        return COMPARISON_TYPE_NONEQUIVS;
    }
    return COMPARISON_TYPE_UNKNOWN;
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

inline BOOL ion_cli_command_compare_streams(COMPARISON_TYPE comparison_type, IonEventStream *lhs, IonEventStream *rhs, IonEventResult *result) {
    return (comparison_type == COMPARISON_TYPE_BASIC)
           ? assertIonEventStreamEq(lhs, rhs, result) : testComparisonSets(lhs, rhs, comparison_type, result);
}

iERR ion_cli_command_compare_standard(ION_CLI_COMMON_ARGS *common_args, COMPARISON_TYPE comparison_type, ION_CATALOG *catalog, IonEventReport *report, IonEventResult *result) {
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

iERR ion_cli_command_compare(ION_CLI_COMMON_ARGS *common_args, COMPARISON_TYPE comparison_type, ION_STRING *output, IonEventReport *report) {
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

iERR ion_cli_parse(std::vector<std::string> const &argv) {
    iENTER;
    std::string docopt_input = std::string(USAGE) + std::string(OPTIONS);
    std::map<std::string, docopt::value> args = docopt::docopt(docopt_input, argv, false, ION_CLI_VERSION);

    IonEventReport report;
    ION_CLI_COMMON_ARGS common_args;

    if (ion_cli_has_flag(&args, "help") || ion_cli_has_flag(&args, "--help")) {
        ion_cli_print_help();
        IONCLEANEXIT;
    }
    if (ion_cli_has_flag(&args, "version") || ion_cli_has_flag(&args, "--version")) {
        std::cout << ION_CLI_VERSION << std::endl;
        IONCLEANEXIT;
    }

    IONREPORT(ion_cli_args_common(&args, &common_args, &report));
    if (ion_cli_has_flag(&args, "process")) {
        ION_CLI_PROCESS_ARGS process_args;
        IONREPORT(ion_cli_args_process(&args, &process_args));
        IONREPORT(ion_cli_command_process(&common_args, &process_args, NULL, &report));
    }
    else if (ion_cli_has_flag(&args, "compare")) {
        COMPARISON_TYPE comparison_type = COMPARISON_TYPE_BASIC;
        if (ion_cli_has_value(&args, "--comparison-type")) {
            comparison_type = ion_cli_comparison_type_from_string(args.find("--comparison-type")->second.asString());
        }
        IONREPORT(ion_cli_command_compare(&common_args, comparison_type, NULL, &report));
    }
    else if (ion_cli_has_flag(&args, "extract")) {
        // TODO implement extract
        std::cerr << "extract command not yet implemented" << std::endl;
    }
cleanup:
    if (report.hasErrors()) {
        if (IERR_OK != ion_cli_write_report(&report, common_args.error_report, common_args.error_report_type, ion_event_stream_write_error_report, NULL)) {
            std::cerr << "Error writing error report to " << common_args.error_report << "." << std::endl;
        }
    }
    iRETURN;
}

