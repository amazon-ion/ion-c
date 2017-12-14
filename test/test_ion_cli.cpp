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

#include <gtest/gtest.h>
#include "cli.h"

TEST(IonCli, ManualInputs) {
    std::vector<std::string> args;
    args.push_back("process");
    args.push_back("--output-format");
    args.push_back("events");
    args.push_back("../ion-tests/iontestdata/good/symbols.ion");
    ion_cli_parse(args);
}

TEST(IonCli, One) {
    std::vector<std::string> args;
    args.push_back("process");
    args.push_back("../build/tmp/one_events.ion");
    ion_cli_parse(args);
}

TEST(IonCli, CompareNullsBasic) {
    std::vector<std::string> args;
    args.push_back("compare");
    args.push_back("../ion-tests/iontestdata/good/allNulls.ion");
    ion_cli_parse(args);
}

TEST(IonCli, CompareListsEquivs) {
    std::vector<std::string> args;
    args.push_back("compare");
    args.push_back("--comparison-type");
    args.push_back("equivs");
    args.push_back("../ion-tests/iontestdata/good/equivs/lists.ion");
    ion_cli_parse(args);
}

TEST(IonCli, CompareSexpsNonequivs) {
    std::vector<std::string> args;
    args.push_back("compare");
    args.push_back("--comparison-type");
    args.push_back("nonequivs");
    args.push_back("../ion-tests/iontestdata/good/non-equivs/sexps.ion");
    ion_cli_parse(args);
}

TEST(IonCli, CompareAnnotatedIvmsEmbeddedNonequivs) {
    std::vector<std::string> args;
    args.push_back("compare");
    args.push_back("--comparison-type");
    args.push_back("nonequivs");
    args.push_back("../ion-tests/iontestdata/good/non-equivs/annotatedIvms.ion");
    ion_cli_parse(args);
}
