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

#include "cli.h"
#include <docopt/docopt.h>
#include <docopt_util.h>

static const char USAGE[] =
R"(
ion-c CLI

Usage:
    ion process [--output <file>] [--error-report <file>] [--output-format <type>] [--catalog <file>]... [--imports <file>]... [--perf-report <file>] [--filter <filter> | --traverse <file>] [-] [<input_file>]...
    ion compare [--output <file>] [--error-report <file>] [--output-format <type>] [--catalog <file>]... [--comparison-type <type>] [-] [<input_file>]...
    ion extract [--output <file>] [--error-report <file>] [--output-format <type>] (--symtab-name <name>) (--symtab-version <version>) [-] [<input_file>]...
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
    -y, --comparison-type <type>    Comparison semantics to be used with the compare command, from the set (basic | equivs | non-equivs | equiv-timeline). Any embedded streams in the inputs are compared for EventStream equality. 'basic' performs a standard data-model comparison between the corresponding events (or embedded streams) in the inputs. 'equivs' verifies that each value (or embedded stream) in a top-level sequence is equivalent to every other value (or embedded stream) in that sequence. 'non-equivs' does the same, but verifies that the values (or embedded streams) are not equivalent. 'equiv-timeline' is the same as 'equivs', except that when top-level sequences contain timestamp values, they are considered equivalent if they represent the same instant regardless of whether they are considered equivalent by the Ion data model. [default: basic]
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

static bool ion_cli_has_flag(std::map<std::string, docopt::value> *args, const std::string arg) {
    auto it = args->find(arg);
    return it != args->end() && it->second.asBool();
}

static bool ion_cli_has_value(std::map<std::string, docopt::value> *args, const std::string arg) {
    auto it = args->find(arg);
    if (it == args->end()) return false;
    docopt::value val = it->second;
    return val.isString() || val.isBool() || val.isLong() || val.isStringList(); // Otherwise, the value is Empty.
}

inline void ion_cli_print_help() {
    // TODO specific help based on command (if any)
    std::cout << USAGE << COMMANDS << OPTIONS << EXAMPLES << std::endl;
}

std::vector<IonCliIO> ion_cli_files_list_to_IO(const std::vector<std::string> *files_list) {
    std::vector<IonCliIO> io;
    for (size_t i = 0; i < files_list->size(); i++) {
        io.emplace_back(files_list->at(i)); // Implicitly converts to a file-type IonCliIO
    }
    return io;
}

ION_EVENT_OUTPUT_TYPE ion_cli_output_type_from_arg(const std::string *output_format_arg) {
    if (!output_format_arg || *output_format_arg == "pretty") return OUTPUT_TYPE_TEXT_PRETTY;
    if (*output_format_arg == "binary") return OUTPUT_TYPE_BINARY;
    if (*output_format_arg == "events") return OUTPUT_TYPE_EVENTS;
    if (*output_format_arg == "none") return OUTPUT_TYPE_NONE;
    return OUTPUT_TYPE_TEXT_UGLY;
}

iERR ion_cli_args_common(std::map<std::string, docopt::value> *args, IonCliCommonArgs *common_args, IonEventReport *report) {
    iENTER;
    ION_CLI_IO_TYPE output_type = IO_TYPE_FILE;
    std::string output_destination = args->find("--output")->second.asString();
    if (output_destination == "stdout" || output_destination == "stderr") {
        output_type = IO_TYPE_CONSOLE;
    }
    common_args->output = IonCliIO(output_destination, output_type);
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    IonEventResult _result, *result = &_result;
    common_args->output_format = ion_cli_output_type_from_arg(&args->find("--output-format")->second.asString());
    ION_CLI_IO_TYPE error_report_type = IO_TYPE_FILE;
    std::string error_report_destination = args->find("--error-report")->second.asString();
    if (error_report_destination == "stdout" || error_report_destination == "stderr") {
        error_report_type = IO_TYPE_CONSOLE;
    }
    common_args->error_report = IonCliIO(error_report_destination, error_report_type);
    if (ion_cli_has_value(args, "--catalog")) {
        common_args->catalogs = ion_cli_files_list_to_IO(&args->find("--catalog")->second.asStringList());
    }
    if (ion_cli_has_value(args, "<input_file>")) {
        common_args->input_files = ion_cli_files_list_to_IO(&args->find("<input_file>")->second.asStringList());
    }
    if (ion_cli_has_flag(args, "-")) {
        common_args->input_files.emplace_back(IonCliIO("stdin", IO_TYPE_CONSOLE));
    }
    if (common_args->input_files.empty()) {
        IONFAILSTATE(IERR_INVALID_ARG, "Input not specified.");
    }
cleanup:
    if (result->has_error_description) {
        report->addResult(result);
    }
    iRETURN;
}

iERR ion_cli_args_process(std::map<std::string, docopt::value> *args, IonCliProcessArgs *process_args) {
    iENTER;
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
        process_args->imports = ion_cli_files_list_to_IO(&args->find("--imports")->second.asStringList());
    }
    cRETURN;
}

ION_EVENT_COMPARISON_TYPE ion_cli_comparison_type_from_string(const std::string type_str) {
    if ("basic" == type_str) {
        return COMPARISON_TYPE_BASIC;
    }
    if ("equivs" == type_str) {
        return COMPARISON_TYPE_EQUIVS;
    }
    if ("non-equivs" == type_str) {
        return COMPARISON_TYPE_NONEQUIVS;
    }
    if ("equiv-timeline" == type_str) {
        return COMPARISON_TYPE_EQUIVTIMELINE;
    }
    return COMPARISON_TYPE_UNKNOWN;
}

iERR ion_cli_parse(std::vector<std::string> const &argv) {
    iENTER;
    std::string docopt_input = std::string(USAGE) + std::string(OPTIONS);
    std::map<std::string, docopt::value> args = docopt::docopt(docopt_input, argv, false, ION_CLI_VERSION);

    IonEventReport report;
    IonCliCommonArgs common_args;

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
        IonCliProcessArgs process_args;
        IONREPORT(ion_cli_args_process(&args, &process_args));
        IONREPORT(ion_cli_command_process(&common_args, &process_args, NULL, &report));
    }
    else if (ion_cli_has_flag(&args, "compare")) {
        ION_EVENT_COMPARISON_TYPE comparison_type = COMPARISON_TYPE_BASIC;
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
        if (IERR_OK != ion_cli_write_error_report(&report, &common_args)) {
            std::cerr << "Error writing error report to " << common_args.error_report.contents << "." << std::endl;
        }
    }
    iRETURN;
}

int main(int argc, char **argv)
{
    if (argc == 1) {
        // Interactive mode
        std::string input_line;
        std::cout << "> ";
        while (std::getline(std::cin, input_line)) {
            ion_cli_parse(split(input_line));
            std::cout << std::endl << "> ";
        }
        std::cout << "Exit" << std::endl;
        return 0;
    }
    // Non-interactive mode.
    return ion_cli_parse({ argv + 1, argv + argc });
}