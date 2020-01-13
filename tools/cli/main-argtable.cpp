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


/**

 Key (from DocOpt spec):
 [ ] - optional arg
 ( ) - required arg
  |  - mutually exclusive option

 Usage:
 ion process [--output <file>]
             [--error-report <file>]
             [--output-format <type>]
             [--catalog <file>]...
             [--imports <file>]...
             [--perf-report <file>]
             [--filter <filter> | --traverse <file>]
             [-]
             [<input_file>]...

 ion compare [--output <file>]
             [--error-report <file>]
             [--output-format <type>]
             [--catalog <file>]...
             [--comparison-type <type>]
             [-]
             [<input_file>]...

 ion extract [--output <file>]
             [--error-report <file>]
             [--output-format <type>]
             (--symtab-name <name>)
             (--symtab-version <version>)
             [-]
             [<input_file>]...

 ion help [extract | compare | process]

 ion --help

 ion version

 ion --version


*/
#include "cli.h"
#include "argtable/argtable3.h"

#define FAIL 1
#define SUCCESS 0

#define iREPORT_EXIT(x)\
    err = (x); \
    if (err) { \
        ion_report_errors(report, common_args); \
        goto cleanup; \
    } \


#define EXIT(STATE) \
 exit_code = (STATE); \
    goto exit; \


void ion_report_errors(IonEventReport &report, IonCliCommonArgs &common_args);
int help_or_version(int help, int version);
int subcommand_proc_main(int proc_count,
                         int out_count,
                         const char *out_fname,
                         int err_count,
                         const char *err_fname,
                         const char **catalog_fnames,
                         int catalog_count,
                         const char **import_fnames,
                         int import_count,
                         int perf_count,
                         const char *perf_fname,
                         int filter_count,
                         const char *filter_str,
                         int traverse_count,
                         const char *traverse_fname,
                         int input_count,
                         const char *input_fnames);

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
                        const char **in_fnames);
ION_EVENT_OUTPUT_TYPE ion_cli_arg_to_output_type(int count, const char *type);
std::vector<IonCliIO> ion_cli_arg_names_to_io(int count, const char **fnames);
bool ion_cli_input_names_clean(int count, const char **in_fnames);


// ion --help | -h | --version | -v
static struct arg_lit *help = arg_lit0("h", "help", "print the help message and exit");
static struct arg_lit *version = arg_lit1("v", "version", "print the commands version number and exit");
static struct arg_end *end = arg_end(19);
static void *argtable[] = {help, version, end};
static int nerrors;


//  subcommand: process
//    ion process [--output <file>]
//    [--error-report <file>]
//    [--output-format <type>]
//    [--catalog <file>]...
//    [--imports <file>]...
//    [--perf-report <file>]
//    [--filter <filter> | --traverse <file>]
//    (- | <input_file>... )

#define REG_EXTENDED 1
#define REG_ICASE (REG_EXTENDED << 1)

static struct arg_rex *proc = arg_rex1(NULL, NULL, "process", NULL, REG_ICASE, NULL);

static struct arg_file *proc_out_f = arg_file0("o", "output", "<file>", "Output file (default: stdout).");

static struct arg_file *proc_err_f = arg_file0("e", "error-report", "<file>", "Error report file (default: stderr).");

static struct arg_str *proc_output_type = arg_str0("f",
                                                   "output-format",
                                                   "<type>",
                                                   "Output format, from the set (text | pretty | binary | events | none). 'events' is only available with the 'process' command, and outputs a serialized EventStream representing the input Ion stream(s). [default: pretty]");
static struct arg_file *proc_catalog_fs = arg_filen("c",
                                                    "catalog",
                                                    "<file>",
                                                    0,
                                                    24,
                                                    "Location(s) of files containing Ion streams of shared symbol tables from which to populate a catalog. This catalog will be used by all readers and writers when encountering shared symbol table import descriptors.");

static struct arg_file *proc_import_fs = arg_filen("i",
                                                   "imports",
                                                   "<file>",
                                                   0,
                                                   24,
                                                   "Location(s) of files containing list(s) of shared symbol table import descriptors. These imports will be used by writers during serialization. If a catalog is available (see: --catalog), the writer will attempt to match those import descriptors to actual shared symbol tables using the catalog.");
static struct arg_file *proc_perf_f = arg_file0("p",
                                                "perf-report",
                                                "<file>",
                                                "PerformanceReport location. If left unspecified, a performance report is not generated.");
static struct arg_str *proc_filter = arg_str0("F",
                                              "filter",
                                              "<filter>",
                                              "JQ-style filter to perform on the input stream(s) before writing the result.");

static struct arg_file *proc_traverse = arg_file0("t",
                                                  "traverse",
                                                  "<file>",
                                                  "Location of a file containing a stream of ReadInstructions to use when reading the input stream(s) instead of performing a full traversal.");
static struct arg_file *proc_input_fs = arg_filen(NULL, NULL, "<file>", 1, 24, "Input file(s).");
static struct arg_end *proc_end = arg_end(20);
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
//    [-]
//    [<input_file>]...

static struct arg_rex *comp = arg_rex1(NULL, NULL, "compare", NULL, REG_ICASE, NULL);

static struct arg_file *comp_out_f = arg_file0("o", "output", "<file>", "Output file (default: stdout).");

static struct arg_file *comp_err_f = arg_file0("e", "error-report", "<file>", "Error report file (default: stderr).");

static struct arg_str *comp_output_type = arg_str0("f",
                                                   "output-format",
                                                   "<type>",
                                                   "Output format, from the set (text | pretty | binary | events | none). 'events' is only available with the 'compess' command, and outputs a serialized EventStream representing the input Ion stream(s). [default: pretty]");

static struct arg_file *comp_catalog_fs = arg_filen("c",
                                                    "catalog",
                                                    "<file>",
                                                    0,
                                                    24,
                                                    "Location(s) of files containing Ion streams of shared symbol tables from which to populate a catalog. This catalog will be used by all readers and writers when encountering shared symbol table import descriptors.");

static struct arg_str *comp_comparison_type = arg_str0("y",
                                                       "comparison-type",
                                                       "<type>",
                                                       "Comparison semantics to be used with the compare command, from the set (basic | equivs | non-equivs | equiv-timeline). Any embedded streams in the inputs are compared for EventStream equality. 'basic' performs a standard data-model comparison between the corresponding events (or embedded streams) in the inputs. 'equivs' verifies that each value (or embedded stream) in a top-level sequence is equivalent to every other value (or embedded stream) in that sequence. 'non-equivs' does the same, but verifies that the values (or embedded streams) are not equivalent. 'equiv-timeline' is the same as 'equivs', except that when top-level sequences contain timestamp values, they are considered equivalent if they represent the same instant regardless of whether they are considered equivalent by the Ion data model. [default: basic]");

static struct arg_file *comp_input_fs = arg_filen(NULL, NULL, "<file>", 0, 24, "Input file(s).");

static struct arg_end *comp_end = arg_end(20);

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
//    [-]
//    [<input_file>]...

static struct arg_rex *xtract = arg_rex1(NULL, NULL, "extract", NULL, REG_ICASE, NULL);

static struct arg_file *xtract_out_f = arg_file0("o", "output", "<file>", "Output file (default: stdout).");

static struct arg_file *xtract_err_f = arg_file0("e", "error-report", "<file>", "Error report file (default: stderr).");

static struct arg_str *xtract_output_type = arg_str0("f",
                                                     "output-format",
                                                     "<type>",
                                                     "Output format, from the set (text | pretty | binary | events | none). 'events' is only available with the 'xtractess' command, and outputs a serialized EventStream representing the input Ion stream(s). [default: pretty]");

static struct arg_file *xtract_catalog_fs = arg_filen("c",
                                                      "catalog",
                                                      "<file>",
                                                      0,
                                                      24,
                                                      "Location(s) of files containing Ion streams of shared symbol tables from which to populate a catalog. This catalog will be used by all readers and writers when encountering shared symbol table import descriptors.");

static struct arg_str *xtract_symtable_name = arg_str1("n",
                                                       "symtable-name",
                                                       "<name>",
                                                       "Name of the shared symbol table to be extracted.");

static struct arg_str *xtract_symtable_version = arg_str1("v",
                                                          "symtable-versoim",
                                                          "<version>",
                                                          "Version of the shared symbol table to be extracted.");

static struct arg_file *xtract_input_fs = arg_filen(NULL, NULL, "<file>", 0, 24, "Input file(s).");

static struct arg_end *xtract_end = arg_end(20);

static void *xtract_argtable[] =
        {xtract, xtract_out_f, xtract_err_f, xtract_output_type, xtract_catalog_fs, xtract_symtable_name,
         xtract_symtable_version, xtract_input_fs, xtract_end};

static int xtract_nerrors;

int main(int argc, char **argv) {
    iENTER;
    int exit_code = 0;

    // verify argtable entries were allocated successfully
    if (arg_nullcheck(argtable) != 0
            || arg_nullcheck(proc_argtable) != 0
            || arg_nullcheck(comp_argtable) != 0
            || arg_nullcheck(xtract_argtable) != 0) {
        std::cerr << "Unable to allocate memory for argument parsing" << std::endl;
        EXIT(FAIL);
    }

    nerrors = arg_parse(argc, argv, argtable);
    proc_nerrors = arg_parse(argc, argv, proc_argtable);
    comp_nerrors = arg_parse(argc, argv, comp_argtable);
    xtract_nerrors = arg_parse(argc, argv, xtract_argtable);

    if (nerrors == 0) {
        exit_code = help_or_version(help->count, version->count);
        EXIT(exit_code)
    }
    if (proc_nerrors == 0) {
        IonEventReport report;
        IonCliCommonArgs common_args;

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
                                   proc_input_fs->filename));

        IonCliProcessArgs proc_args;
//        IONREPORT(ion_cli_proc_args(&proc_args,
//                proc_perf_f->count,
//                proc_perf_f->filename[0],
//                proc_filter->count,
//                proc_filter->sval[0],
//                proc_traverse->count,
//                proc_traverse->filename[0],
//                proc_import_fs->count,
//                proc_import_fs->filename));

        exit_code = subcommand_proc_main(
                proc->count,
                proc_out_f->count,
                proc_out_f->filename[0],
                proc_err_f->count,
                proc_err_f->filename[0],
                proc_catalog_fs->filename,
                proc_catalog_fs->count,
                proc_import_fs->filename,
                proc_import_fs->count,
                proc_perf_f->count,
                proc_perf_f->filename[0],
                proc_filter->count,
                proc_filter->sval[0],
                proc_traverse->count,
                proc_traverse->filename[0],
                proc_input_fs->count,
                proc_input_fs->filename[0]);
        EXIT(exit_code);
    }
    if (comp_nerrors == 0) {
        std::cout << "Compare subcommand detected" << std::endl;
        exit_code = 0;
        EXIT(exit_code);
    }
    if (xtract_nerrors == 0) {
        std::cout << "Extract subcommand detected" << std::endl;
        exit_code = 0;
        EXIT(exit_code);
    } else {
        if (proc->count > 0) {
            std::cout << "Invalid input provided to ion process" << std::endl;
            arg_print_errors(stdout, proc_end, ION_CLI_PNAME);
            std::cout << "Usage : " << std::endl;
            arg_print_syntaxv(stdout, proc_argtable, "\n");
            EXIT(FAIL);
        }
        if (comp->count > 0) {
            std::cout << "Invalid input provided to ion process" << std::endl;
            arg_print_errors(stdout, proc_end, ION_CLI_PNAME);
            std::cout << "Usage : " << std::endl;
            arg_print_syntaxv(stdout, comp_argtable, "\n");
            EXIT(FAIL);
        }
        if (xtract->count > 0) {
            std::cout << "Invalid input provided to ion process" << std::endl;
            arg_print_errors(stdout, proc_end, ION_CLI_PNAME);
            std::cout << "Usage : " << std::endl;
            arg_print_syntaxv(stdout, xtract_argtable, "\n");
            EXIT(FAIL);
        } else {
            std::cout << "Invalid input provided to ion" << std::endl;
            arg_print_errors(stdout, end, ION_CLI_PNAME);
            std::cout << "Usage : " << std::endl;
            arg_print_syntaxv(stdout, argtable, "\n");
            EXIT(FAIL);
        }
    }

    cleanup:

    exit:
    /* deallocate each non-null entry in each argtable */
    arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
    arg_freetable(proc_argtable, sizeof(proc_argtable) / sizeof(proc_argtable[0]));
    arg_freetable(comp_argtable, sizeof(comp_argtable) / sizeof(comp_argtable[0]));
    arg_freetable(xtract_argtable, sizeof(xtract_argtable) / sizeof(xtract_argtable[0]));

    return exit_code;

}

void ion_report_errors(IonEventReport &report, IonCliCommonArgs &common_args) {
    if (report.hasErrors()) {
        if (IERR_OK != ion_cli_write_error_report(&report, &common_args)) {
            std::cerr << "Error writing error report to " << common_args.error_report.contents << "." << std::endl;
        }
    }
}

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
    if (out_count == 1) {
        out_destination = std::string(out_fname);
        output_type = IO_TYPE_FILE;
    }

    common_args->output = IonCliIO(out_destination, output_type);
    ION_SET_ERROR_CONTEXT(&common_args->output.contents, NULL);
    IonEventResult _result, *result = &_result;
    common_args->output_format = ion_cli_arg_to_output_type(out_type_count, out_type);

    ION_CLI_IO_TYPE err_report_type = IO_TYPE_CONSOLE;
    std::string err_destination("stderr"); // default is STDERR
    if (err_count == 1) {
        err_destination = std::string(err_fname);
        err_report_type = IO_TYPE_FILE;
    }
    common_args->error_report = IonCliIO(err_destination, err_report_type);
    if (catalog_count > 0) {
        common_args->catalogs = ion_cli_arg_names_to_io(catalog_count, catalog_fnames);
    }

    if (in_count == 1 && strcmp("-",in_fnames[0])) {
       in_fnames[0] = "stdin";
    }

    if (!ion_cli_input_names_clean(in_count, in_fnames)) {
        IONFAILSTATE(IERR_INVALID_ARG, "Input can be '-' or file name(s) but not both.")
    } else {
        common_args->input_files = ion_cli_arg_names_to_io(in_count, in_fnames);
    }

    cleanup:
    if (result->has_error_description) {
        report->addResult(result);
    }
    iRETURN;
}
bool ion_cli_input_names_clean(int count, const char **in_fnames) {
    bool acc;
    for (int i = 0; i < count; i++) {
        const char *tmp = in_fnames[i];
        if(strcmp("-",tmp) == 0) return false; // in_fnames[i]) == 0) return false;
    }
    return true;
}

std::vector<IonCliIO> ion_cli_arg_names_to_io(int count, const char **fnames) {
    std::vector<IonCliIO> acc; // accumulator
    for (int i = 0; i < count; i++) {
        acc.emplace_back(std::string(fnames[i]));
    }
    return acc;
}

/**
 * Maps the command line argument provided for `--output-format` to the appropriate internal enum value.
 * The string given as argument to `--output-format` should be one of
 *
 *   - `pretty`
 *   - `binary`
 *   - `events`
 *   - `none`
 *
 * If no `--output-format` was given, the default is `pretty`.
 *
 * @param count number of occurrences --output-format was used. This can be 0 or 1.
 * @param type string passed on CLI for output type. Should be one of pretty, binary, events, none
 * @return corresponding ION_EVENT_OUTPUT_TYPE.
 */
ION_EVENT_OUTPUT_TYPE ion_cli_arg_to_output_type(int count, const char *type) {
    if (count == 0) return OUTPUT_TYPE_TEXT_PRETTY;
    if (strcmp("pretty", type)) return OUTPUT_TYPE_TEXT_PRETTY;
    if (strcmp("binary", type)) return OUTPUT_TYPE_BINARY;
    if (strcmp("events", type)) return OUTPUT_TYPE_EVENTS;
    if (strcmp("none", type)) return OUTPUT_TYPE_NONE;
    return OUTPUT_TYPE_TEXT_UGLY;
}

int help_or_version(int help, int version) {
    if (version) {
        std::cout << ION_CLI_VERSION << std::endl;
        return 0;
    } else if (help) {
        std::cout << "Help ... I need somebody" << std::endl;
        return 0;
    } else {
        return 1;
    }
}

int subcommand_proc_main(int proc_count,
                         int out_count,
                         const char *out_fname,
                         int err_count,
                         const char *err_fname,
                         const char **catalog_fnames,
                         int catalog_count,
                         const char **import_fnames,
                         int import_count,
                         int perf_count,
                         const char *perf_fname,
                         int filter_count,
                         const char *filter_str,
                         int traverse_count,
                         const char *traverse_fname,
                         int input_count,
                         const char *input_fnames) {

    std::cout << "proc_count : " << proc_count << std::endl;

    std::cout << "out_count : " << out_count << std::endl;
    std::cout << "out_name : " << out_fname << std::endl;

    std::cout << "err_count : " << err_count << std::endl;
    std::cout << "err_name : " << err_fname << std::endl;

    std::cout << "catalog_count : " << catalog_count << std::endl;
    std::cout << "catalog_name : " << catalog_fnames << std::endl;

    for (int i = 0; i < catalog_count; i++) {
        printf("catalog[%d] = %s \n", i, catalog_fnames[i]);
    }

    std::cout << "import_count : " << import_count << std::endl;
    std::cout << "import_name : " << import_fnames << std::endl;

    for (int i = 0; i < import_count; i++) {
        printf("import[%d] = %s \n", i, import_fnames[i]);
    }

    std::cout << "perf_count : " << perf_count << std::endl;
    std::cout << "perf_name : " << perf_fname << std::endl;

    std::cout << "filter_count : " << filter_count << std::endl;
    std::cout << "filter_str : " << filter_str << std::endl;

    std::cout << "traverse_count : " << traverse_count << std::endl;
    std::cout << "traverse_name : " << traverse_fname << std::endl;

    std::cout << "input_count : " << input_count << std::endl;
    std::cout << "input_name : " << input_fnames << std::endl;

    return 0;
}

