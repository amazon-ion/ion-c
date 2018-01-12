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


class IonCliIO {
public:
    ION_CLI_IO_TYPE type;
    std::string contents;

    IonCliIO(){
        memset(this, 0, sizeof(IonCliIO));
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
    std::string output_format; // TODO store as enum?
    std::vector<IonCliIO> catalogs;
    std::vector<IonCliIO> input_files;

    IonCliCommonArgs() {
        memset(this, 0, sizeof(IonCliCommonArgs));
    }
};

class IonCliProcessArgs {
public:
    IonCliIO perf_report;
    std::string filter;
    IonCliIO traverse;
    std::vector<IonCliIO> imports;

    IonCliProcessArgs() {
        memset(this, 0, sizeof(IonCliProcessArgs));
    }
};

class IonCliReaderContext {
public:
    ION_READER_OPTIONS options;
    hREADER reader;
    FILE *file_stream;
    std::string input_location;
    ION_STREAM *ion_stream;

    IonCliReaderContext() {
        memset(this, 0, sizeof(IonCliReaderContext));
    }
};

iERR ion_cli_command_compare(IonCliCommonArgs *common_args, ION_EVENT_COMPARISON_TYPE comparison_type, ION_STRING *output, IonEventReport *report);
iERR ion_cli_command_process(IonCliCommonArgs *common_args, IonCliProcessArgs *process_args, ION_STRING *output, IonEventReport *report);

iERR ion_cli_write_error_report(IonEventReport *report, IonCliCommonArgs *common_args);

#endif //IONC_CLI_H
