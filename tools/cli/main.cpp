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

//
// Consult https://github.com/ts-amazon/ion-test-driver for the set of command line options required for
// consensus testing across Ion library implementations.
//
// For argtable3 see https://www.argtable.org/tutorial/
//

#include "cli.h"
#include "argtable/argtable3.h"
#include <iomanip>


#define iREPORT_EXIT(x)\
    err = (x); \
    if (err) { \
        goto cleanup; \
    } \

#define iREPORT_EXIT_MSG(x, msg)\
    err = (x); \
    if (err) { \
        std::cerr << msg << std::endl; \
        goto cleanup; \
    } \

#define iFAIL(code)\
    err = (code); \
    goto fail; \


#define EXIT(STATE) \
    err = (STATE); \
    goto exit; \

#define iFAIL_MSG(code, msg) \
    std::cerr << msg << std::endl; \
    iREPORT_EXIT(code) \

#define PRINT_ION_COMMAND printf("\t%s", ION_CLI_PNAME)
#define PRINT_HEADER_USAGE std::cout << "Usage: " << std::endl << std::endl
#define PRINT_HEADER_OPTIONS std::cout << std::endl << "Options: " << std::endl << std::endl
#define PRINT_HEADER_EXAMPLES std::cout << "Examples: " << std::endl << std::endl
#define PRINT_HEADER_COMMANDS std::cout << "Commands: " << std::endl << std::endl

#define PRINT_SUBCOMMAND_HELP_ON_ERROR(tablename) \
    PRINT_HEADER_USAGE; \
    PRINT_ION_COMMAND; \
    arg_print_syntaxv(stdout, tablename, "\n"); \
    PRINT_HEADER_OPTIONS; \
    arg_print_glossary_gnu(stdout, tablename); \
    PRINT_HEADER_EXAMPLES;\


//
// Constants
//

// help and version
static const char *const HELP_SHORT_OPT = "h";
static const char *const HELP_LONG_OPT = "help";
static const char *const HELP_COMMAND_NAME = HELP_LONG_OPT;
static const char *const HELP_GLOSSARY = "print the help message and exit";
static const char *const VERSION_SHORT_OPT = "v";
static const char *const SYM_TABLE_VERSION_SHORT_OPT = VERSION_SHORT_OPT;
static const char *const VERSION_LONG_OPT = "version";
static const char *const VERSION_COMMAND_NAME = VERSION_LONG_OPT;
static const char *const VERSION_GLOSSARY = "print the command's version number and exit";

// Max values for
//   - no errors
//   - no input for multiple input options
static const int ARG_MAX_ERRORS = 20;
static const int MAX_NO_INPUTS = 48;

// Common args shared by subcommands
static const char *const OUTPUT_SHORT_OPT = "o";
static const char *const OUTPUT_LONG_OPT = "output";
static const char *const FILE_DATATYPE = "<file>";
static const char *const OUTPUT_FILE_GLOSSARY = "Output file [default: stdout].";

static const char *const ERR_SHORT_OPT = "e";
static const char *const ERR_LONG_OPT = "error-report";
static const char *const ERR_GLOSSARY = "Error report file [default: stderr].";

static const char *const OUT_TYPE_SHORT_OPT = "f";
static const char *const OUT_TYPE_LONG_OPT = "output-format";
static const char *const TYPE_DATATYPE = "<type>";
static const char *const OUT_TYPE_GLOSSARY = "Output format, from the set (text | pretty | binary | events | none). 'events' is only available with the 'process' command, and outputs a serialized EventStream representing the input Ion stream(s). [default: pretty]";

static const char *const CATALOG_SHORT_OPT = "c";
static const char *const CATALOG_LONG_OPT = "catalog";
static const char *const CATALOG_GLOSSARY = "Location(s) of files containing Ion streams of shared symbol tables from which to populate a catalog. This catalog will be used by all readers and writers when encountering shared symbol table import descriptors.";

static const char *const INPUT_GLOSSARY = "Input file(s).";

// subcommand: process
static const char *const PROCESS_COMMAND_NAME = "process";

static const char *const IMPORTS_SHORT_OPT = "i";
static const char *const IMPORTS_LONG_OPT = "imports";
static const char *const IMPORT_GLOSSARY = "Location(s) of files containing list(s) of shared symbol table import descriptors. These imports will be used by writers during serialization. If a catalog is available (see: --catalog), the writer will attempt to match those import descriptors to actual shared symbol tables using the catalog.";

static const char *const PERF_SHORT_OPT = "p";
static const char *const PERF_LONG_OPT = "perf-report";
static const char *const PERF_GLOSSARY = "PerformanceReport location. If left unspecified, a performance report is not generated.";

static const char *const FILTER_SHORT_OPT = "F";
static const char *const FILTER_LONG_OPT = "filter";
static const char *const FILTER_DATATYPE = "<filter>";
static const char *const FILTER_GLOSSARY = "JQ-style filter to perform on the input stream(s) before writing the result.";

static const char *const TRAVERSE_SHORT_OPT = "t";
static const char *const TRAVERSE_LONG_OPT = "traverse";
static const char *const TRAVERSE_GLOSSARY = "Location of a file containing a stream of ReadInstructions to use when reading the input stream(s) instead of performing a full traversal.";

// subcommand: compare
static const char *const COMPARE_COMMAND_NAME = "compare";

static const char *const COMPARE_TYPE_SHORT_OPT = "y";
static const char *const COMPARE_TYPE_LONG_OPT = "comparison-type";
static const char *const COMPARE_TYPE_GLOSSARY = "Comparison semantics to be used with the compare command, from the set (basic | equivs | non-equivs | equiv-timeline). Any embedded streams in the inputs are compared for EventStream equality. 'basic' performs a standard data-model comparison between the corresponding events (or embedded streams) in the inputs. 'equivs' verifies that each value (or embedded stream) in a top-level sequence is equivalent to every other value (or embedded stream) in that sequence. 'non-equivs' does the same, but verifies that the values (or embedded streams) are not equivalent. 'equiv-timeline' is the same as 'equivs', except that when top-level sequences contain timestamp values, they are considered equivalent if they represent the same instant regardless of whether they are considered equivalent by the Ion data model. [default: basic]";

// subcommand: extract
static const char *const EXTRACT_COMMAND_NAME = "extract";

static const char *const SYM_TABLE_NAME_SHORT_OPT = "n";
static const char *const SYM_TABLE_NAME_LONG_OPT = "symtable-name";
static const char *const NAME_DATATYPE = "<name>";
static const char *const SYM_TABLE_NAME_GLOSSARY = "Name of the shared symbol table to be extracted.";

static const char *const SYM_TABLE_VERSION_LONG_OPT = "symtable-version";
static const char *const VERSION_DATATYPE = "<version>";
static const char *const SYM_TABLE_VERSION_GLOSSARY = "Version of the shared symbol table to be extracted.";

// subcommand: version
static const char *const VERSION_SUB_COMMAND_GLOSSARY = "Print the command's version.";

// subcommand: help
static const char *const HELP_SUB_COMMAND_GLOSSARY = "Print help for ion and its subcommands.";
static const char *const SUB_COMMAND_GLOSSARY = "Print help for a subcommand process, compare or extract.";


//
// arg table datastructs
//

// ion --help | --version
static struct arg_lit *help = arg_lit0(HELP_SHORT_OPT, HELP_LONG_OPT, HELP_GLOSSARY);
static struct arg_lit *version = arg_lit0(VERSION_SHORT_OPT, VERSION_LONG_OPT, VERSION_GLOSSARY);
static struct arg_end *end = arg_end(ARG_MAX_ERRORS);
static void *argtable[] = {help, version, end};
static int nerrors;

#define REG_EXTENDED 1
#define REG_ICASE (REG_EXTENDED << 1)

// subcommand: version


static struct arg_rex *version_cmd = arg_rex1(NULL,
                                              NULL,
                                              VERSION_COMMAND_NAME,
                                              NULL,
                                              REG_ICASE,
                                              VERSION_SUB_COMMAND_GLOSSARY);
static struct arg_end *version_end = arg_end(ARG_MAX_ERRORS);
static void *version_argtable[] =
        {version_cmd, version_end};
static int version_nerrors;

// subcommand: help [process | compare | extract]
static struct arg_rex *help_cmd = arg_rex1(NULL, NULL, HELP_COMMAND_NAME, NULL, REG_ICASE, HELP_SUB_COMMAND_GLOSSARY);
static struct arg_rex *sub_cmd = arg_rex0(NULL,
                                          NULL,
                                          "process|compare|extract",
                                          NULL,
                                          REG_ICASE,
                                          SUB_COMMAND_GLOSSARY);
static struct arg_end *help_end = arg_end(ARG_MAX_ERRORS);


static void *help_argtable[] =
        {help_cmd, sub_cmd, version_end};
static int help_nerrors;

//  subcommand: process
//    ion process [--output <file>]
//    [--error-report <file>]
//    [--output-format <type>]
//    [--catalog <file>]...
//    [--imports <file>]...
//    [--perf-report <file>]
//    [--filter <filter> | --traverse <file>]
//    (- | <input_file>... )



static struct arg_rex *proc = arg_rex1(NULL, NULL, PROCESS_COMMAND_NAME, NULL, REG_ICASE, NULL);

static struct arg_file *proc_out_f = arg_file0(OUTPUT_SHORT_OPT, OUTPUT_LONG_OPT, FILE_DATATYPE, OUTPUT_FILE_GLOSSARY);

static struct arg_file *proc_err_f = arg_file0(ERR_SHORT_OPT, ERR_LONG_OPT, FILE_DATATYPE, ERR_GLOSSARY);

static struct arg_str *proc_output_type = arg_str0(OUT_TYPE_SHORT_OPT,
                                                   OUT_TYPE_LONG_OPT,
                                                   TYPE_DATATYPE,
                                                   OUT_TYPE_GLOSSARY);

static struct arg_file *proc_catalog_fs = arg_filen(CATALOG_SHORT_OPT,
                                                    CATALOG_LONG_OPT,
                                                    FILE_DATATYPE,
                                                    0,
                                                    MAX_NO_INPUTS,
                                                    CATALOG_GLOSSARY);


static struct arg_file *proc_import_fs = arg_filen(IMPORTS_SHORT_OPT,
                                                   IMPORTS_LONG_OPT,
                                                   FILE_DATATYPE,
                                                   0,
                                                   MAX_NO_INPUTS,
                                                   IMPORT_GLOSSARY);


static struct arg_file *proc_perf_f = arg_file0(PERF_SHORT_OPT,
                                                PERF_LONG_OPT,
                                                FILE_DATATYPE,
                                                PERF_GLOSSARY);
static struct arg_str *proc_filter = arg_str0(FILTER_SHORT_OPT,
                                              FILTER_LONG_OPT,
                                              FILTER_DATATYPE,
                                              FILTER_GLOSSARY);

static struct arg_file *proc_traverse = arg_file0(TRAVERSE_SHORT_OPT,
                                                  TRAVERSE_LONG_OPT,
                                                  FILE_DATATYPE,
                                                  TRAVERSE_GLOSSARY);
static struct arg_file *proc_input_fs = arg_filen(NULL, NULL, FILE_DATATYPE, 1, MAX_NO_INPUTS, INPUT_GLOSSARY);
static struct arg_end *proc_end = arg_end(ARG_MAX_ERRORS);
static void *proc_argtable[] =
        {proc, proc_out_f, proc_err_f, proc_output_type, proc_catalog_fs, proc_import_fs, proc_perf_f, proc_filter,
         proc_traverse, proc_input_fs, proc_end};
static int proc_nerrors;


//  subcommand: compare
//    ion compare [--output <file>]
//    [--error-report <file>]
//    [--output-format <type>]
//    [--catalog <file>]...
//    [--comparison-type <type>]
//    ( - | <input_file>...)

static struct arg_rex *comp = arg_rex1(NULL, NULL, COMPARE_COMMAND_NAME, NULL, REG_ICASE, NULL);

static struct arg_file *comp_out_f = arg_file0(OUTPUT_SHORT_OPT,
                                               OUTPUT_LONG_OPT, FILE_DATATYPE, OUTPUT_FILE_GLOSSARY);

static struct arg_file *comp_err_f = arg_file0(ERR_SHORT_OPT, ERR_LONG_OPT,
                                               FILE_DATATYPE, ERR_GLOSSARY);

static struct arg_str *comp_output_type = arg_str0(OUT_TYPE_SHORT_OPT,
                                                   OUT_TYPE_LONG_OPT,
                                                   TYPE_DATATYPE,
                                                   OUT_TYPE_GLOSSARY);

static struct arg_file *comp_catalog_fs = arg_filen(CATALOG_SHORT_OPT,
                                                    CATALOG_LONG_OPT,
                                                    FILE_DATATYPE,
                                                    0,
                                                    MAX_NO_INPUTS,
                                                    CATALOG_GLOSSARY);

static struct arg_str *comp_comparison_type = arg_str0(COMPARE_TYPE_SHORT_OPT,
                                                       COMPARE_TYPE_LONG_OPT,
                                                       TYPE_DATATYPE,
                                                       COMPARE_TYPE_GLOSSARY);


static struct arg_file *comp_input_fs = arg_filen(NULL, NULL, FILE_DATATYPE, 0, MAX_NO_INPUTS, INPUT_GLOSSARY);

static struct arg_end *comp_end = arg_end(ARG_MAX_ERRORS);

static void *comp_argtable[] =
        {comp, comp_out_f, comp_err_f, comp_output_type, comp_catalog_fs, comp_comparison_type, comp_input_fs,
         comp_end};

static int comp_nerrors;

//    subcommand: extract
//    ion extract [--output <file>]
//    [--error-report <file>]
//    [--output-format <type>]
//    (--symtab-name <name>)
//    (--symtab-version <version>)
//    ( - | <input_file>...)

static struct arg_rex *extract = arg_rex1(NULL, NULL, EXTRACT_COMMAND_NAME, NULL, REG_ICASE, NULL);

static struct arg_file *extract_out_f = arg_file0(OUTPUT_SHORT_OPT,
                                                  OUTPUT_LONG_OPT, FILE_DATATYPE, OUTPUT_FILE_GLOSSARY);

static struct arg_file *extract_err_f = arg_file0(ERR_SHORT_OPT, ERR_LONG_OPT,
                                                  FILE_DATATYPE, ERR_GLOSSARY);

static struct arg_str *extract_output_type = arg_str0(OUT_TYPE_SHORT_OPT,
                                                      OUT_TYPE_LONG_OPT,
                                                      TYPE_DATATYPE,
                                                      OUT_TYPE_GLOSSARY);


static struct arg_file *extract_catalog_fs = arg_filen(CATALOG_SHORT_OPT,
                                                       CATALOG_LONG_OPT,
                                                       FILE_DATATYPE,
                                                       0,
                                                       MAX_NO_INPUTS,
                                                       CATALOG_GLOSSARY);

static struct arg_str *extract_symtable_name = arg_str1(SYM_TABLE_NAME_SHORT_OPT,
                                                        SYM_TABLE_NAME_LONG_OPT,
                                                        NAME_DATATYPE,
                                                        SYM_TABLE_NAME_GLOSSARY);

static struct arg_str *extract_symtable_version = arg_str1(SYM_TABLE_VERSION_SHORT_OPT,
                                                           SYM_TABLE_VERSION_LONG_OPT,
                                                           VERSION_DATATYPE,
                                                           SYM_TABLE_VERSION_GLOSSARY);

static struct arg_file *extract_input_fs = arg_filen(NULL, NULL, FILE_DATATYPE, 0, MAX_NO_INPUTS, INPUT_GLOSSARY);

static struct arg_end *extract_end = arg_end(20);

static void *extract_argtable[] =
        {extract, extract_out_f, extract_err_f, extract_output_type, extract_catalog_fs, extract_symtable_name,
         extract_symtable_version, extract_input_fs, extract_end};

static int extract_nerrors;

static void **all_arg_tables[] = {argtable, proc_argtable, comp_argtable, extract_argtable, help_argtable,
                                  version_argtable};
static int all_arg_tables_size = sizeof(all_arg_tables) / sizeof(all_arg_tables[0]);

//
// Printing functions
//

void ion_print_process_examples() {
    std::string s =
            //--------------------------------------------------------------------------------
            //<-                                80 chars                                    ->
            //--------------------------------------------------------------------------------
            "Read input.10n and pretty-print it to stdout:\n"
            "\t$ ion process input.10n\n\n"

            "Read input.10n (using a catalog comprised of the shared symbol tables contained \n"
            "in catalog.10n) without re-writing, and write a performance report:\n"
            "\t$ ion process --output-format none --catalog catalog.10n --perf-report report input.10n\n\n"

            "Read input.10n according to the ReadInstructions specified by instructions.ion \n"
            "and write the resulting Events to output.ion:\n"
            "\t$ ion process -o output.ion -f events -t instructions.ion input.10n\n\n"

            "Read input1.ion and input2.10n and output to stdout any values in the streams \n"
            "that match the filter .foo:\n"
            "\t$ ion process --filter .foo input1.ion input2.10n\n\n";

    std::cout << s << std::endl;

}

void ion_print_compare_examples() {
    std::string s =
            //--------------------------------------------------------------------------------
            //<-                                80 chars                                    ->
            //--------------------------------------------------------------------------------
            "Compare each stream in read_events.ion, input1.ion, and input2.10n against all \n"
            "other streams in the set and output a ComparisonReport to comparison_report.ion:\n"
            "\t$ ion compare -o comparison_report.ion read_events.ion input1.ion input2.10n\n\n";
    std::cout << s << std::endl;
}

void ion_print_extract_examples() {
    std::string s =
            //--------------------------------------------------------------------------------
            //<-                                80 chars                                    ->
            //--------------------------------------------------------------------------------

            "Extract a shared symbol table with name \"foo_table\" and version 1 from the \n"
            "piped Ion stream and write it in binary format to foo_table.10n:\n"
            "\t$ echo 'foo' | ion extract -n 'foo_table' -V 1 -o foo_table.10n -f binary -\n\n";
    std::cout << s << std::endl;
}


void ion_print_subcommand_help() {
    PRINT_HEADER_COMMANDS;

    std::string command_help =
            //--------------------------------------------------------------------------------
            //<-                                80 chars                                    ->
            //--------------------------------------------------------------------------------
            "extract    Extract the symbols from the given input(s) into a shared symbol \n"
            "           table with the given name and version.\n\n"

            "compare    Compare all inputs (which may contain Ion streams and/or EventStreams) \n"
            "           against all other inputs using the Ion data model's definition of \n"
            "           equality. Write a ComparisonReport to the output.\n\n"

            "process    Read the input file(s) (optionally specifying ReadInstructions or a \n"
            "           filter) and re-write in the format specified by --output.\n\n"

            "help       Prints this general help. If provided a command, prints help specific\n"
            "           to that command.\n\n"

            "version    Prints version information about this tool.\n\n";

    std::cout << command_help << std::endl << std::endl;
}

void ion_print_command_options() {
    for (int i = 0; i < all_arg_tables_size; i++) {
        PRINT_ION_COMMAND;
        arg_print_syntaxv(stdout, all_arg_tables[i], "\n");
    }
}

void ion_print_command_options_glossary() {
    PRINT_HEADER_USAGE;
    for (int i = 0; i < all_arg_tables_size; i++) {
        arg_print_glossary_gnu(stdout, all_arg_tables[i]);
    }
}

void ion_print_examples() {
    PRINT_HEADER_EXAMPLES;
    ion_print_process_examples();
    ion_print_compare_examples();
    ion_print_extract_examples();
}

void ion_print_full_help_message() {
    PRINT_HEADER_USAGE;
    PRINT_ION_COMMAND;
    arg_print_syntaxv(stdout, help_argtable, "\n\n");
    ion_print_subcommand_help();
}

iERR ion_print_help_or_version(int help_count, int version_count) {
    iENTER;
    if (version_count) {
        std::cout << ION_CLI_VERSION << std::endl;
        SUCCEED()
    } else if (help_count) {
        ion_print_full_help_message();
        SUCCEED()
    } else {
        iFAIL(IERR_INVALID_ARG)
    }
    iRETURN;
}

//
// helpers
//

/**
 * Maps the command line argument provided for `--output-format` to the appropriate internal enum value.
 * The string given as argument to `--output-format` should be one of
 *
 *   - `input`
 *   - `pretty`
 *   - `binary`
 *   - `events`
 *   - `none`
 *
 * If no `--output-format` was given, the default is `pretty`.
 *
 * @param common_args common argument object whose output_format field gets mutated accordingly
 * @param count number of occurrences --output-format was used. This can be 0 or 1
 * @param type string passed on CLI for output type. Should be one of text, pretty, binary, events, none
 * @return invalid argument error or success
 */
iERR ion_cli_arg_to_output_type(IonCliCommonArgs *common_args, int count, const char *type) {
    iENTER;
    std::string input(type);
    if (count == 0) {
        common_args->output_format = OUTPUT_TYPE_TEXT_PRETTY;
        SUCCEED()
    }
    if (input == "text") {
        common_args->output_format = OUTPUT_TYPE_TEXT_UGLY;
        SUCCEED()
    }
    if (input == "pretty") {
        common_args->output_format = OUTPUT_TYPE_TEXT_PRETTY;
        SUCCEED()
    }
    if (input == "binary") {
        common_args->output_format = OUTPUT_TYPE_BINARY;
        SUCCEED()
    }
    if (input == "events") {
        common_args->output_format = OUTPUT_TYPE_EVENTS;
        SUCCEED()
    }
    if (input == "none") {
        common_args->output_format = OUTPUT_TYPE_NONE;
        SUCCEED()
    }
    iFAIL(IERR_INVALID_ARG)

    iRETURN;
}

/**
 * Given the number of file names and the file names as string return a vector with IonCliIo objects,
 * one for each filename given.
 *
 * @param count number of file names given
 * @param fnames file names as strings
 * @return vector of IonCliIo objects, one for each file name given
 */
std::vector<IonCliIO> ion_cli_arg_names_to_io(int count, const char **fnames) {
    std::vector<IonCliIO> acc; // accumulator
    for (int i = 0; i < count; i++) {
        acc.emplace_back(std::string(fnames[i]));
    }
    return acc;
}

bool is_hyphen(const std::string &input) {
    return std::string("-") == input;
}


/**
 * Given the number of file name and the file names as strings check if they contain the special
 * string "-" that denotes STDIN. If so return false, else true.
 *
 * @param count number of file names
 * @param in_fnames file names as strings
 * @return false if "-" is in `in_fnames`, true otherwise
 */
bool ion_cli_input_names_clean(int count, const char **in_fnames) {
    for (int i = 0; i < count; i++) {
        const char *tmp = in_fnames[i];
        if (is_hyphen(std::string(tmp))) return false;
    }
    return true;
}

/**
 * Populates `proc_args` from the rest of the function arguments.
 *
 * @pre any checks for mutually exclusive cli arguments must be done **before** calling this function
 */
iERR ion_cli_proc_args(IonCliProcessArgs *proc_args,
                       int perf_count,
                       const char *perf_fname,
                       int filter_count,
                       const char *filter,
                       int traverse_count,
                       const char *traverse_fname,
                       int import_count,
                       const char **import_fnames) {
    iENTER;

    if (perf_count == 1) {
        proc_args->perf_report = std::string(perf_fname);
    }

    if (filter_count == 1) {
        proc_args->filter = std::string(filter);
    }

    if (traverse_count == 1) {
        proc_args->traverse = std::string(traverse_fname);
    }

    if (import_count > 0) {
        proc_args->imports = ion_cli_arg_names_to_io(import_count, import_fnames);
    }

    iRETURN;

}


/**
 * Maps a string `input` to the corresponding variant from `ION_EVENT_COMPARISON_TYPE`
 */
ION_EVENT_COMPARISON_TYPE ion_cli_comparison_type_from_input(const std::string &input) {
    if ("basic" == input) {
        return COMPARISON_TYPE_BASIC;
    }
    if ("equivs" == input) {
        return COMPARISON_TYPE_EQUIVS;
    }
    if ("non-equivs" == input) {
        return COMPARISON_TYPE_NONEQUIVS;
    }
    if ("equiv-timeline" == input) {
        return COMPARISON_TYPE_EQUIVTIMELINE;
    }
    return COMPARISON_TYPE_UNKNOWN;
}


/**
 * Process command line arguments that are common for all subcommands and update `report` and `common_args`
 * accordingly.
 */
iERR ion_cli_common_args(IonEventReport *report,
                         IonCliCommonArgs *common_args,
                         int out_count,
                         const char *out_fname,
                         int err_count,
                         const char *err_fname,
                         int out_type_count,
                         const char *out_type,
                         int catalog_count,
                         const char **catalog_fnames,
                         int in_count,
                         const char **in_fnames) {
    iENTER;
    ION_CLI_IO_TYPE output_type = IO_TYPE_CONSOLE;
    std::string out_destination("stdout");  // default is STDOUT
    ION_CLI_IO_TYPE err_report_type = IO_TYPE_CONSOLE;
    std::string err_destination("stderr"); // default is STDERR

    if (out_count == 1) {
        out_destination = std::string(out_fname);
        output_type = IO_TYPE_FILE;
    }

    common_args->output = IonCliIO(out_destination, output_type);
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL)
    IonEventResult _result, *result = &_result;
    iREPORT_EXIT_MSG(ion_cli_arg_to_output_type(common_args, out_type_count, out_type),
                     "Invalid option for --output-format (-f)")

    if (err_count == 1) {
        err_destination = std::string(err_fname);
        err_report_type = IO_TYPE_FILE;
    }
    common_args->error_report = IonCliIO(err_destination, err_report_type);
    if (catalog_count > 0) {
        common_args->catalogs = ion_cli_arg_names_to_io(catalog_count, catalog_fnames);
    }

    if (in_count == 1 && is_hyphen(std::string(in_fnames[0]))) {
        common_args->input_files.emplace_back(IonCliIO("stdin", IO_TYPE_CONSOLE));
    } else if (!ion_cli_input_names_clean(in_count, in_fnames)) {
        iFAIL_MSG(IERR_INVALID_ARG, "Input can be '-' or file name(s) but not both.")
    } else {
        common_args->input_files = ion_cli_arg_names_to_io(in_count, in_fnames);
    }

    cleanup:
    if (result->has_error_description) {
        report->addResult(result);
    }
    iRETURN;
}

iERR ion_help_subcommand(int sub_command, const char **sub_cmd_name) {
    iENTER;

    if (sub_command > 1) {
        std::cerr << "Help subcommand accepts only one of process, compare, extract." << std::endl;
        iFAIL(IERR_INVALID_ARG)
    } else if (sub_command == 0) {
        ion_print_full_help_message();
        SUCCEED()
    } else {
        std::string sname = std::string(*sub_cmd_name);
        if (sname == "process") {
            PRINT_SUBCOMMAND_HELP_ON_ERROR(proc_argtable)
            ion_print_process_examples();
            SUCCEED()
        }
        if (sname == "compare") {
            PRINT_SUBCOMMAND_HELP_ON_ERROR(comp_argtable)
            ion_print_compare_examples();
            SUCCEED()
        }
        if (sname == "extract") {
            PRINT_SUBCOMMAND_HELP_ON_ERROR(extract_argtable)
            ion_print_extract_examples();
            SUCCEED()
        }
    }
    iRETURN;
}

/**
 * Used for when a subcommand is given some valid and some invalid inputs.
 * Shorter message (no examples) than the message printed with `help` subcommand.
 */
void print_usage_for_subcommand(struct arg_end *table_end, void **args_table) {
    arg_print_errors(stdout, table_end, ION_CLI_PNAME);
    std::cout << std::endl << "Usage : " << std::endl;
    PRINT_ION_COMMAND;
    arg_print_syntaxv(stdout, args_table, "\n");
    arg_print_glossary_gnu(stdout, args_table);
}

/**
 * Check that each argtable instance was created and initialized.
 */
bool arg_tables_not_initalized() {
    bool result = false;
    for (int i = 0; i < all_arg_tables_size; i++) {
        result = result || arg_nullcheck(all_arg_tables[i]) != 0;
    }
    return result;
}

extern "C" int ion_c_cli_main(int argc, char **argv);

int ion_c_cli_main(int argc, char **argv) {
    iENTER;

    IonEventReport report;
    IonCliCommonArgs common_args;

    if (arg_tables_not_initalized()) {
        std::cerr << "Unable to allocate memory for argument parsing." << std::endl;
        EXIT(IERR_NO_MEMORY)
    }

    version_nerrors = arg_parse(argc, argv, version_argtable);
    help_nerrors = arg_parse(argc, argv, help_argtable);
    proc_nerrors = arg_parse(argc, argv, proc_argtable);
    comp_nerrors = arg_parse(argc, argv, comp_argtable);
    extract_nerrors = arg_parse(argc, argv, extract_argtable);
    nerrors = arg_parse(argc, argv, argtable);

    if (nerrors == 0) {
        EXIT(ion_print_help_or_version(help->count, version->count))
    } else if (version_nerrors == 0) {
        EXIT(ion_print_help_or_version(0, 1))
    } else if (help_nerrors == 0) {
        EXIT(ion_help_subcommand(sub_cmd->count, sub_cmd->sval))
    } else if (proc_nerrors == 0) {

        iREPORT_EXIT(ion_cli_common_args(&report,
                                         &common_args,
                                         proc_out_f->count,
                                         proc_out_f->filename[0],
                                         proc_err_f->count,
                                         proc_err_f->filename[0],
                                         proc_output_type->count,
                                         proc_output_type->sval[0],
                                         proc_catalog_fs->count,
                                         proc_catalog_fs->filename,
                                         proc_input_fs->count,
                                         proc_input_fs->filename))

        IonCliProcessArgs proc_args;

        if (proc_filter->count > 0 && proc_traverse->count > 0) {
            std::cerr << "Cannot use both --filter and --traverse." << std::endl;
            proc_nerrors++;
        }
        iREPORT_EXIT(ion_cli_proc_args(&proc_args,
                                       proc_perf_f->count,
                                       proc_perf_f->filename[0],
                                       proc_filter->count,
                                       proc_filter->sval[0],
                                       proc_traverse->count,
                                       proc_traverse->filename[0],
                                       proc_import_fs->count,
                                       proc_import_fs->filename))

        iREPORT_EXIT(ion_cli_command_process(&common_args, &proc_args, NULL, &report))

    } else if (comp_nerrors == 0) {
        iREPORT_EXIT(ion_cli_common_args(&report,
                                         &common_args,
                                         comp_out_f->count,
                                         comp_out_f->filename[0],
                                         comp_err_f->count,
                                         comp_err_f->filename[0],
                                         comp_output_type->count,
                                         comp_output_type->sval[0],
                                         comp_catalog_fs->count,
                                         comp_catalog_fs->filename,
                                         comp_input_fs->count,
                                         comp_input_fs->filename))

        ION_EVENT_COMPARISON_TYPE comp_type = COMPARISON_TYPE_BASIC;
        if (comp_comparison_type->count == 1) {
            comp_type = ion_cli_comparison_type_from_input(std::string(comp_comparison_type->sval[0]));
        }
        iREPORT_EXIT(ion_cli_command_compare(&common_args, comp_type, NULL, &report))
    } else if (extract_nerrors == 0) {
        std::cout << "Extract command not implemented yet." << std::endl;
    } else {
        if (version_cmd->count > 0) {
            std::cout << "Invalid input provided to ion version" << std::endl;
            print_usage_for_subcommand(version_end, version_argtable);
            EXIT(IERR_INVALID_ARG)
        }
        if (help_cmd->count > 0) {
            std::cout << "Invalid input provided to ion help" << std::endl;
            print_usage_for_subcommand(help_end, help_argtable);
            EXIT(IERR_INVALID_ARG)
        }
        if (proc->count > 0) {
            std::cout << "Invalid input provided to ion process" << std::endl;
            print_usage_for_subcommand(proc_end, proc_argtable);
            EXIT(IERR_INVALID_ARG)
        }
        if (comp->count > 0) {
            std::cout << "Invalid input provided to ion compare" << std::endl;
            print_usage_for_subcommand(comp_end, comp_argtable);
            EXIT(IERR_INVALID_ARG)
        }
        if (extract->count > 0) {
            std::cout << "Invalid input provided to ion extract" << std::endl;
            print_usage_for_subcommand(extract_end, extract_argtable);
            EXIT(IERR_INVALID_ARG)
        } else {
            std::cout << "Invalid input provided to ion" << std::endl;
            arg_print_errors(stdout, end, ION_CLI_PNAME);
            std::cout << "Usage : " << std::endl;
            arg_print_syntaxv(stdout, argtable, "\n");
            arg_print_glossary_gnu(stdout, argtable);
            EXIT(IERR_INVALID_ARG)
        }
    }

    cleanup:
    if (report.hasErrors()) {
        if (IERR_OK != ion_cli_write_error_report(&report, &common_args)) {
            std::cerr << "Error writing error report to " << common_args.error_report.contents << "." << std::endl;
        }
    }

    exit:
    // deallocate each non-null entry in each argtable
    for (int i = 0; i < all_arg_tables_size; i++) {
        void **current_arg_table = all_arg_tables[i];
        arg_freetable(current_arg_table, sizeof(current_arg_table) / sizeof(current_arg_table[0]));
    }

    iRETURN;
}

/**
 * External programs may compile the CLI as a library and invoke
 * ion_c_cli_main() themselves.
 */ 
#ifndef EXTERNAL_DRIVER
int main(int argc, char **argv) {
    return ion_c_cli_main(argc, argv);
}
#endif

