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
#include "docopt/docopt.h"
#include "cli.h"

static const char USAGE[] =
R"(
ion-c CLI

Usage:
    ion process [--output <file>] [--error-report <file>] [--output-format <type>] [--catalog <file>]... [--imports <file>]... [--perf-report <file>] [--filter <filter> | --traverse <file>] (- | <input_file>...)
    ion compare [--output <file>] [--error-report <file>] [--output-format <type>] [--catalog <file>]... [--imports <file>]... [--comparison-type <type>] (- | <input_file>...)
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

/**
 * Arguments shared by the process, compare, and extract commands.
 */
typedef struct _ion_cli_common_args {
    std::string output;
    std::string error_report;
    std::string output_format;
    bool input_is_stdin;
    std::vector<std::string> input_files;
} ION_CLI_COMMON_ARGS;

/**
 * Arguments shared by the process and compare commands only.
 */
typedef struct _ion_cli_process_compare_args {
    std::vector<std::string> catalog;
    std::vector<std::string> imports;
} ION_CLI_PROCESS_COMPARE_ARGS;

typedef struct _ion_cli_process_args {
    ION_CLI_COMMON_ARGS common_args;
    ION_CLI_PROCESS_COMPARE_ARGS process_compare_args;
    std::string perf_report;
    std::string filter;
    std::string traverse;
} ION_CLI_PROCESS_ARGS;

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

iERR ion_cli_command_process_filter(ION_CLI_PROCESS_ARGS *args) {
    iENTER;
    // TODO implement jq-style filtering.
    iRETURN;
}

iERR ion_cli_command_process_traverse(ION_CLI_PROCESS_ARGS *args) {
    iENTER;
    // TODO implement custom read traversal.
    iRETURN;
}

typedef struct _ion_cli_reader_context {
    ION_READER_OPTIONS options;
    hREADER reader;
    FILE *file_stream;
    ION_STREAM *ion_stream;
    IonEventStream *event_stream;
} ION_CLI_READER_CONTEXT;

iERR ion_cli_close_reader(ION_CLI_READER_CONTEXT *context) {
    iENTER;
    ASSERT(context);
    IONCHECK(ion_reader_close(context->reader));
    IONCHECK(ion_stream_close(context->ion_stream));
    if (context->file_stream) {
        fclose(context->file_stream);
    }
    iRETURN;
}

iERR ion_cli_add_shared_tables_to_catalog(std::string file_path, ION_CATALOG *catalog) {
    iENTER;
    hREADER symbol_table_reader;
    ION_TYPE type;
    FILE *file_stream = fopen(file_path.c_str(), "rb");
    ION_STREAM *ion_stream;
    if (!file_stream) {
        FAILWITHMSG(IERR_CANT_FIND_FILE, file_path);
    }
    IONCHECK(ion_stream_open_file_in(file_stream, &ion_stream));
    // A basic reader will do.
    IONCHECK(ion_reader_open(&symbol_table_reader, ion_stream, NULL));
    IONCHECK(ion_reader_next(symbol_table_reader, &type));
    while (type != tid_none) {
        ION_SYMBOL_TABLE *symbol_table;
        IONCHECK(ion_symbol_table_load(symbol_table_reader, catalog, &symbol_table));
        IONCHECK(ion_catalog_add_symbol_table(catalog, symbol_table));
        IONCHECK(ion_reader_next(symbol_table_reader, &type));
    }
    IONCHECK(ion_reader_close(symbol_table_reader));

    iRETURN;
}

iERR ion_cli_open_reader(ION_CLI_COMMON_ARGS *args, ION_CATALOG *catalog, std::string *file_path,
                         ION_CLI_READER_CONTEXT *reader_context, IonEventStream *event_stream) {
    iENTER;
    memset(reader_context, 0, sizeof(ION_CLI_READER_CONTEXT));
    ion_event_initialize_reader_options(&reader_context->options);
    reader_context->options.pcatalog = catalog;
    reader_context->event_stream = event_stream;
    ion_event_register_symbol_table_callback(&reader_context->options, event_stream);

    if (!file_path) {
        ASSERT(args->input_is_stdin);
        IONCHECK(ion_stream_open_stdin(&reader_context->ion_stream));
    }
    else {
        reader_context->file_stream = fopen(file_path->c_str(), "rb");
        if (!reader_context->file_stream) {
            FAILWITHMSG(IERR_CANT_FIND_FILE, file_path->c_str());
        }
        IONCHECK(ion_stream_open_file_in(reader_context->file_stream, &reader_context->ion_stream));
    }

    IONCHECK(ion_reader_open(&reader_context->reader, reader_context->ion_stream, &reader_context->options));

    iRETURN;
}

iERR ion_cli_create_catalog(std::vector<std::string> *catalog_paths, ION_CATALOG **catalog) {
    iENTER;
    ION_CATALOG *_catalog = NULL;
    ASSERT(catalog);

    if (catalog_paths && !catalog_paths->empty()) {
        IONCHECK(ion_catalog_open(&_catalog));
        for (size_t i = 0; i < catalog_paths->size(); i++) {
            IONCHECK(ion_cli_add_shared_tables_to_catalog(catalog_paths->at(i), _catalog));
        }
    }
    *catalog = _catalog;
    iRETURN;
}

iERR ion_cli_open_writer_basic(ION_CLI_COMMON_ARGS *args, ION_EVENT_WRITER_CONTEXT *writer_context) {
    iENTER;
    writer_context->options.output_as_binary = args->output_format == "binary";
    writer_context->options.pretty_print = args->output_format == "pretty" || args->output_format == "events";

    if (args->output == "stdout") {
        IONCHECK(ion_stream_open_stdout(&writer_context->ion_stream));
    }
    else {
        writer_context->file_stream = fopen(args->output.c_str(), "wb");
        if (!writer_context->file_stream) {
            FAILWITHMSG(IERR_CANT_FIND_FILE, args->output.c_str());
        }
        IONCHECK(ion_stream_open_file_out(writer_context->file_stream, &writer_context->ion_stream));
    }

    IONCHECK(ion_writer_open(&writer_context->writer, writer_context->ion_stream, &writer_context->options));
    iRETURN;
}

iERR ion_cli_open_writer(ION_CLI_COMMON_ARGS *args, ION_CATALOG *catalog, ION_COLLECTION *imports, ION_EVENT_WRITER_CONTEXT *writer_context) {
    iENTER;

    memset(writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    ion_event_initialize_writer_options(&writer_context->options);
    writer_context->options.pcatalog = catalog;
    if (imports && !ION_COLLECTION_IS_EMPTY(imports)) {
        IONCHECK(ion_writer_options_initialize_shared_imports(&writer_context->options));
        IONCHECK(ion_writer_options_add_shared_imports(&writer_context->options, imports));
    }

    IONCHECK(ion_cli_open_writer_basic(args, writer_context));
    iRETURN;
}

iERR ion_cli_close_writer(ION_EVENT_WRITER_CONTEXT *context) {
    iENTER;
    ASSERT(context);
    IONCHECK(ion_writer_close(context->writer));
    if (context->has_imports) {
        IONCHECK(ion_writer_options_close_shared_imports(&context->options));
    }
    IONCHECK(ion_stream_close(context->ion_stream));
    if (context->file_stream) {
        fclose(context->file_stream);
    }
    iRETURN;
}

iERR ion_cli_create_imports(std::vector<std::string> *import_files, hOWNER *imports_owner, ION_COLLECTION *imports) {
    iENTER;
    hOWNER owner = NULL;
    ASSERT(imports_owner);
    ASSERT(imports);

    if (import_files && !import_files->empty()) {
        owner = _ion_alloc_owner(sizeof(int)); // This is a dummy owner; the size doesn't matter.
        _ion_collection_initialize(owner, imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    }
    *imports_owner = owner;
    iRETURN;
}

iERR ion_cli_open_event_writer(ION_CLI_COMMON_ARGS *args, ION_EVENT_WRITER_CONTEXT *writer_context) {
    iENTER;
    memset(writer_context, 0, sizeof(ION_EVENT_WRITER_CONTEXT));
    IONCHECK(ion_cli_open_writer_basic(args, writer_context));
    IONCHECK(ion_writer_write_symbol(writer_context->writer, &ion_cli_event_stream_symbol));
    iRETURN;
}

iERR ion_cli_is_event_stream(ION_CLI_READER_CONTEXT *reader_context, bool *is_event_stream, bool *has_more_events) {
    iENTER;
    ION_TYPE ion_type;
    ION_SYMBOL *symbol_value = NULL;
    IonEvent *event;
    size_t i = 0;
    IonEventStream *stream = reader_context->event_stream;
    ASSERT(is_event_stream);

    *is_event_stream = FALSE;
    *has_more_events = TRUE;
    for (;; i++) {
        IONCHECK(ion_reader_next(reader_context->reader, &ion_type));
        if (ion_type == tid_EOF) {
            SUCCEED();
        }
        IONCHECK(ion_event_stream_read(reader_context->reader, stream, ion_type, FALSE, 0));
        ASSERT(stream->size() > 0);
        event = stream->at(i);
        if (event->event_type == SYMBOL_TABLE) {
            // It's unlikely, but event streams could be serialized with imports. If this is true, skip to the next
            // event.
            continue;
        }
        if (event->event_type == SCALAR && event->ion_type == tid_SYMBOL
            && event->num_annotations == 0 && event->depth == 0) {
            symbol_value = (ION_SYMBOL *) event->value;
            if (ION_STRING_EQUALS(&ion_cli_event_stream_symbol, &symbol_value->value)) {
                *is_event_stream = TRUE;
                stream->remove(i); // Toss this event -- it's not part of the user data.
            }
        }
        else if (event->event_type == STREAM_END) {
            *has_more_events = FALSE;
        }
        break;
    }
    iRETURN;
}

iERR ion_cli_write_value(ION_CLI_COMMON_ARGS *args, ION_EVENT_WRITER_CONTEXT *writer_context, ION_CATALOG *catalog, std::string *file_path) {
    iENTER;
    ION_CLI_READER_CONTEXT reader_context;
    IonEventStream stream;
    bool is_event_stream, has_more_events;
    IONCHECK(ion_cli_open_reader(args, catalog, file_path, &reader_context, &stream));

    IONCHECK(ion_cli_is_event_stream(&reader_context, &is_event_stream, &has_more_events));
    if (has_more_events) {
        if (is_event_stream) {
            IONCHECK(ion_event_stream_read_all_events(reader_context.reader, &stream, catalog));
        }
        else {
            IONCHECK(ion_event_stream_read_all(reader_context.reader, &stream));
        }
    }
    if (args->output_format == "events") {
        IONCHECK(ion_event_stream_write_all_events(writer_context->writer, &stream, catalog));
    }
    else if (args->output_format != "none"){
        IONCHECK(ion_event_stream_write_all(writer_context->writer, &stream));
        // Would be nice to use this (especially for performance testing), but having to peek at the first value in the
        // stream (to identify $ion_event_stream) rules it out.
        //IONCHECK(ion_writer_write_all_values(writer_context->writer, reader_context.reader));
    }
    IONCHECK(ion_cli_close_reader(&reader_context));
    iRETURN;
}

iERR ion_cli_write_all_values(ION_CLI_COMMON_ARGS *args, ION_CATALOG *catalog, ION_COLLECTION *imports, std::vector<std::string> *file_paths) {
    iENTER;
    ION_EVENT_WRITER_CONTEXT writer_context;
    if (args->output_format == "events") {
        IONCHECK(ion_cli_open_event_writer(args, &writer_context));
    }
    else if (args->output_format != "none") {
        IONCHECK(ion_cli_open_writer(args, catalog, imports, &writer_context));
    }
    if (args->input_is_stdin) {
        IONCHECK(ion_cli_write_value(args, &writer_context, catalog, NULL));
    }
    else {
        for (size_t i = 0; i < file_paths->size(); i++) {
            IONCHECK(ion_cli_write_value(args, &writer_context, catalog, &file_paths->at(i)));
        }
    }
    IONCHECK(ion_cli_close_writer(&writer_context));
    iRETURN;
}

iERR ion_cli_command_process_standard(ION_CLI_PROCESS_ARGS *args) {
    iENTER;
    ION_CATALOG *catalog = NULL;
    hOWNER imports_owner = NULL;
    ION_COLLECTION imports;

    IONCHECK(ion_cli_create_catalog(&args->process_compare_args.catalog, &catalog));
    IONCHECK(ion_cli_create_imports(&args->process_compare_args.imports, &imports_owner, &imports));
    IONCHECK(ion_cli_write_all_values(&args->common_args, catalog, imports_owner ? &imports : NULL, &args->common_args.input_files));

    if (catalog) {
        IONCHECK(ion_catalog_close(catalog));
    }
    if (imports_owner) {
        ion_free_owner(imports_owner);
    }

    iRETURN;
}

iERR ion_cli_args_common(std::map<std::string, docopt::value> *args, ION_CLI_COMMON_ARGS *result) {
    iENTER;
    result->output_format = args->find("--output-format")->second.asString();
    result->output = args->find("--output")->second.asString();
    result->error_report = args->find("--error-report")->second.asString();
    result->input_is_stdin = false;
    if (ion_cli_has_flag(args, "-")) {
        result->input_is_stdin = true;
    }
    else {
        result->input_files = args->find("<input_file>")->second.asStringList();
    }
    if (!result->input_is_stdin && result->input_files.empty()) FAILWITHMSG(IERR_INVALID_ARG, "Input not specified.");
    iRETURN;
}

iERR ion_cli_args_process_compare(std::map<std::string, docopt::value> *args, ION_CLI_PROCESS_COMPARE_ARGS *result) {
    iENTER;
    if (ion_cli_has_value(args, "--imports")) {
        result->imports = args->find("--imports")->second.asStringList();
    }
    if (ion_cli_has_value(args, "--catalog")) {
        result->catalog = args->find("--catalog")->second.asStringList();
    }
    iRETURN;
}

iERR ion_cli_command_process(std::map<std::string, docopt::value> *args) {
    iENTER;
    bool has_filter = ion_cli_has_value(args, "--filter");
    bool has_traverse = ion_cli_has_value(args, "--traverse");
    ION_CLI_PROCESS_ARGS process_args;

    if (ion_cli_has_value(args, "--symtab-version") || ion_cli_has_value(args, "--symtab-name")) {
        FAILWITHMSG(IERR_INVALID_ARG, "--symtab-version and --symtab-name are incompatible with the process command.")
    }
    if (has_filter && has_traverse) FAILWITHMSG(IERR_INVALID_ARG, "--filter and --traverse are mutually exclusive.");

    IONCHECK(ion_cli_args_common(args, &process_args.common_args));
    IONCHECK(ion_cli_args_process_compare(args, &process_args.process_compare_args));

    if (ion_cli_has_value(args, "--perf-report")) {
        process_args.perf_report = args->find("--perf-report")->second.asString();
        // TODO add perf report support
    }

    // TODO add error report support

    if (has_filter) {
        process_args.filter = args->find("--filter")->second.asString();
        IONCHECK(ion_cli_command_process_filter(&process_args));
        SUCCEED();
    }
    if (has_traverse) {
        process_args.traverse = args->find("--traverse")->second.asString();
        IONCHECK(ion_cli_command_process_traverse(&process_args));
        SUCCEED();
    }

    // Full traversal, no filtering.
    IONCHECK(ion_cli_command_process_standard(&process_args));

    iRETURN;
}

iERR ion_cli_parse(std::vector<std::string> const &argv) {
    iENTER;
    std::string docopt_input = std::string(USAGE) + std::string(OPTIONS);
    std::map<std::string, docopt::value> args
            = docopt::docopt(docopt_input, argv, false, ION_CLI_VERSION);

    if (ion_cli_has_flag(&args, "help") || ion_cli_has_flag(&args, "--help")) {
        ion_cli_print_help();
        SUCCEED();
    }
    if (ion_cli_has_flag(&args, "version") || ion_cli_has_flag(&args, "--version")) {
        std::cout << ION_CLI_VERSION << std::endl;
        SUCCEED();
    }
    if (ion_cli_has_flag(&args, "process")) {
        IONCHECK(ion_cli_command_process(&args));
        SUCCEED();
    }
    if (ion_cli_has_flag(&args, "compare")) {
        // TODO implement compare
        std::cerr << "compare command not yet implemented" << std::endl;
        SUCCEED();
    }
    if (ion_cli_has_flag(&args, "extract")) {
        // TODO implement extract
        std::cerr << "extract command not yet implemented" << std::endl;
        SUCCEED();
    }

    iRETURN;
}

