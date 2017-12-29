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

#define ION_CLI_VERSION "1.0"

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
    ION_CLI_PROCESS_COMPARE_ARGS process_compare_args;
    std::string perf_report;
    std::string filter;
    std::string traverse;
} ION_CLI_PROCESS_ARGS;

typedef struct _ion_cli_compare_args {
    ION_CLI_PROCESS_COMPARE_ARGS process_compare_args;
    COMPARISON_TYPE comparison_type;
} ION_CLI_COMPARE_ARGS;

typedef struct _ion_cli_reader_context {
    ION_READER_OPTIONS options;
    hREADER reader;
    FILE *file_stream;
    std::string input_location;
    ION_STREAM *ion_stream;
    IonEventStream *event_stream;
} ION_CLI_READER_CONTEXT;

iERR ion_cli_parse(std::vector<std::string> const &argv);

iERR ion_cli_read_stream(ION_CLI_READER_CONTEXT *reader_context, ION_CATALOG *catalog, IonEventStream *stream, IonEventResult *result);

iERR ion_cli_command_process_standard(ION_EVENT_WRITER_CONTEXT *writer_context, ION_CLI_COMMON_ARGS *common_args, ION_CATALOG *catalog, IonEventResult *result);
void ion_cli_command_compare_streams(COMPARISON_TYPE comparison_type, IonEventStream *lhs, IonEventStream *rhs, IonEventResult *result);
iERR ion_cli_command_compare_standard(ION_CLI_COMMON_ARGS *common_args, ION_CLI_COMPARE_ARGS *compare_args, IonEventReport *report, IonEventResult *result);

#endif //IONC_CLI_H
