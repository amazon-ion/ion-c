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
#include <ionc/ion_errors.h>
#include "ion_event_equivalence.h"
#include "ion_event_util.h"
#include "ion_event_stream_impl.h"
#include "ion_catalog_impl.h"

#define ION_CLI_VERSION "1.0"

typedef enum _ion_cli_input_format {
    IO_TYPE_FILE = 0,
    IO_TYPE_CONSOLE,
    IO_TYPE_MEMORY
} ION_CLI_IO_TYPE;

/**
 * Describes an input to or output from a CLI command.
 */
class IonCliIO {
public:
    ION_CLI_IO_TYPE type;
    std::string contents;

    IonCliIO(){
        this->type = IO_TYPE_FILE;
    }

    IonCliIO(std::string contents, ION_CLI_IO_TYPE type=IO_TYPE_FILE) {
        this->contents = contents;
        this->type = type;
    }
};

/**
 * Arguments shared by the process, compare, and extract commands.
 */
class IonCliCommonArgs {
public:
    IonCliIO output;
    IonCliIO error_report;
    ION_EVENT_OUTPUT_TYPE output_format;
    std::vector<IonCliIO> catalogs;
    std::vector<IonCliIO> input_files;

    IonCliCommonArgs() {
        output_format = OUTPUT_TYPE_TEXT_PRETTY;
    }
};

/**
 * Arguments specific to the process command.
 */
class IonCliProcessArgs {
public:
    IonCliIO perf_report;
    std::string filter;
    IonCliIO traverse;
    std::vector<IonCliIO> imports;

    IonCliProcessArgs() {}
};

iERR ion_cli_command_compare(IonCliCommonArgs *common_args, ION_EVENT_COMPARISON_TYPE comparison_type,
                             ION_STRING *output, IonEventReport *report);

iERR ion_cli_command_process(IonCliCommonArgs *common_args, IonCliProcessArgs *process_args, ION_STRING *output,
                             IonEventReport *report);

iERR ion_cli_write_error_report(IonEventReport *report, IonCliCommonArgs *common_args);

/**
 * Accepts the `output` parameter to an `ion_cli_command_*` function
 * and frees any memory allocated during that function. NOTE: if
 * `output` itself was dynamically allocated, it remains the caller's
 * responsibility to free it.
 */
void ion_cli_free_command_output(ION_STRING *output);

#endif //IONC_CLI_H
