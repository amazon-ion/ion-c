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

 Key:
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

#define EXIT(STATE) \
 exit_code = (STATE); \
    goto exit; \


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

int main(int argc, char **argv) {
  int exit_code = 0;

  // ion --help | -h | --version | -v
  struct arg_lit *help = arg_lit0("h", "help", "print the help message and exit");
  struct arg_lit *version = arg_lit0("v", "version", "print the commands version number and exit");
  struct arg_end *end = arg_end(20);
  void *argtable[] = {help, version, end};
  int nerrors;

//    ion process [--output <file>]
//    [--error-report <file>]
//    [--output-format <type>]
//    [--catalog <file>]...
//    [--imports <file>]...
//    [--perf-report <file>]
//    [--filter <filter> | --traverse <file>]
//    [-]
//    [<input_file>]...

#define REG_EXTENDED 1
#define REG_ICASE (REG_EXTENDED << 1)

  struct arg_rex *proc = arg_rex1(NULL, NULL, "process", NULL, REG_ICASE, NULL);

  struct arg_file *proc_out_f = arg_file0("o", "output", "<file>", "Output file (default: stdout).");

  struct arg_file *proc_err_f = arg_file0("e", "error-report", "<file>", "Error report file (default: stderr).");

  struct arg_str *proc_output_type = arg_str0("f",
                                              "output-format",
                                              "<type>",
                                              "Output format, from the set (text | pretty | binary | events | none). 'events' is only available with the 'process' command, and outputs a serialized EventStream representing the input Ion stream(s). [default: pretty]");

  struct arg_file *proc_catalog_fs = arg_filen("c",
                                               "catalog",
                                               "<file>",
                                               0,
                                               24,
                                               "Location(s) of files containing Ion streams of shared symbol tables from which to populate a catalog. This catalog will be used by all readers and writers when encountering shared symbol table import descriptors.");

  struct arg_file *proc_import_fs = arg_filen("i",
                                              "imports",
                                              "<file>",
                                              0,
                                              24,
                                              "Location(s) of files containing list(s) of shared symbol table import descriptors. These imports will be used by writers during serialization. If a catalog is available (see: --catalog), the writer will attempt to match those import descriptors to actual shared symbol tables using the catalog.");

  struct arg_file *proc_perf_f = arg_file0("p",
                                           "perf-report",
                                           "<file>",
                                           "PerformanceReport location. If left unspecified, a performance report is not generated.");

  struct arg_str *proc_filter = arg_str0("F",
                                         "filter",
                                         "<filter>",
                                         "JQ-style filter to perform on the input stream(s) before writing the result.");

  struct arg_file *proc_traverse = arg_file0("t",
                                             "traverse",
                                             "<file>",
                                             "Location of a file containing a stream of ReadInstructions to use when reading the input stream(s) instead of performing a full traversal.");

  struct arg_file *proc_input_fs = arg_filen(NULL, NULL, "<file>", 0, 24, "Input file(s).");

  struct arg_end *proc_end = arg_end(20);

  void *proc_argtable[] =
      {proc, proc_out_f, proc_err_f, proc_output_type, proc_catalog_fs, proc_import_fs, proc_perf_f, proc_filter,
       proc_traverse, proc_input_fs, proc_end};

  int proc_nerrors;

  // verify argtable entries were allocated successfully
  if (arg_nullcheck(argtable) != 0 || arg_nullcheck(proc_argtable) != 0) {
    std::cerr << "Unable to allocate memory for argument parsing" << std::endl;
    EXIT(FAIL);
  }

  nerrors = arg_parse(argc, argv, argtable);
  proc_nerrors = arg_parse(argc, argv, proc_argtable);

  if (nerrors == 0) {
    exit_code = help_or_version(help->count, version->count);
  }
  if (proc_nerrors == 0) {
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
        proc_input_fs->filename[0]
                                    );
  } else {
    if (proc->count > 0) {
      std::cout << "Invalid input provided to ion process" << std::endl;
      arg_print_errors(stdout, proc_end, ION_CLI_PNAME);
      std::cout << "Usage : " << std::endl;
      arg_print_syntaxv(stdout, proc_argtable, "\n");
      EXIT(FAIL);
    } else {
      std::cout << "Invalid input provided to ion" << std::endl;
      arg_print_errors(stdout, end, ION_CLI_PNAME);
      std::cout << "Usage : " << std::endl;
      arg_print_syntaxv(stdout, argtable, "\n");
      EXIT(FAIL);
    }
  }

  exit:
  /* deallocate each non-null entry in each argtable */
  arg_freetable(argtable, sizeof(argtable) / sizeof(argtable[0]));
  arg_freetable(proc_argtable, sizeof(proc_argtable) / sizeof(proc_argtable[0]));

  return exit_code;

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

