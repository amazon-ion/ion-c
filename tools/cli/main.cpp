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
#include <docopt_util.h>

int main(int argc, char **argv)
{
    if (argc == 1) {
        // Interactive mode
        std::string input_line;
        int err = 0;
        std::cout << "> ";
        while (std::getline(std::cin, input_line) && !err) {
            err = ion_cli_parse(split(input_line));
            std::cout << std::endl << "> ";
        }
        std::cout << "Exit" << std::endl;
        return err;
    }
    // Non-interactive mode.
    return ion_cli_parse({ argv + 1, argv + argc });
}