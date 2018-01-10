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

#ifndef IONC_CLI_H
#define IONC_CLI_H

#include <string>
#include <iostream>
#include <vector>
#include <inc/ion_errors.h>
#include <ion_event_equivalence.h>
#include <ion_event_util.h>
#include <ion_catalog_impl.h>

#define ION_CLI_VERSION "1.0"

typedef enum _ion_cli_input_format {
    IO_TYPE_FILE = 0,
    IO_TYPE_CONSOLE,
    IO_TYPE_MEMORY
} ION_CLI_IO_TYPE;

// TODO make all these classes?
/**
 * Arguments shared by the process, compare, and extract commands.
 */
typedef struct _ion_cli_common_args {
    ION_CLI_IO_TYPE output_type;
    std::string output;
    ION_CLI_IO_TYPE error_report_type;
    std::string error_report;
    std::string output_format;
    ION_CLI_IO_TYPE catalogs_format;
    std::vector<std::string> catalogs;
    ION_CLI_IO_TYPE inputs_format;
    std::vector<std::string> input_files;
} ION_CLI_COMMON_ARGS;

typedef struct _ion_cli_process_args {
    std::string perf_report;
    std::string filter;
    ION_CLI_IO_TYPE traverse_format;
    std::string traverse;
    ION_CLI_IO_TYPE imports_format;
    std::vector<std::string> imports;
} ION_CLI_PROCESS_ARGS;

typedef struct _ion_cli_reader_context {
    ION_READER_OPTIONS options;
    hREADER reader;
    FILE *file_stream;
    std::string input_location;
    ION_STREAM *ion_stream;
} ION_CLI_READER_CONTEXT;

iERR ion_cli_command_compare(ION_CLI_COMMON_ARGS *common_args, ION_EVENT_COMPARISON_TYPE comparison_type, ION_STRING *output, IonEventReport *report);
iERR ion_cli_command_process(ION_CLI_COMMON_ARGS *common_args, ION_CLI_PROCESS_ARGS *process_args, ION_STRING *output, IonEventReport *report);

iERR ion_cli_write_error_report(IonEventReport *report, ION_CLI_COMMON_ARGS *common_args);

#endif //IONC_CLI_H
