/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#ifndef IONC_GATHER_VECTORS_H
#define IONC_GATHER_VECTORS_H

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <iostream>
#include <vector>

#include <ion.h>

#ifdef ION_PLATFORM_WINDOWS
#define ION_TEST_PATH_SEPARATOR_CHAR '\\'
#else
#define ION_TEST_PATH_SEPARATOR_CHAR '/'
#endif

/** Returns true if file should be included. */
typedef BOOL (*FILE_PREDICATE_FN)(std::string filename);

typedef enum _test_file_type
{
    FILETYPE_BINARY = 1,
    FILETYPE_TEXT,
    FILETYPE_ALL

} TEST_FILE_TYPE;

typedef enum _test_file_classification
{
    CLASSIFICATION_GOOD_BASIC = 1,
    CLASSIFICATION_GOOD_EQUIVS,
    CLASSIFICATION_GOOD_NONEQUIVS,
    CLASSIFICATION_GOOD_TIMESTAMP_EQUIVTIMELINE,
    CLASSIFICATION_BAD

} TEST_FILE_CLASSIFICATION;

std::string find_ion_tests_path();
std::string join_path(std::string prefix, std::string suffix);

static const std::string iontests_path = find_ion_tests_path();
static const std::string good_path = join_path("iontestdata", "good");
static const std::string bad_path = join_path("iontestdata", "bad");
static const std::string bad_timestamp_path = join_path(bad_path, "timestamp");
static const std::string good_equivs_path = join_path(good_path, "equivs");
static const std::string good_nonequivs_path = join_path(good_path, "non-equivs");
static const std::string good_timestamp_path = join_path(good_path, "timestamp");
static const std::string good_timestamp_equivtimeline_path = join_path(good_timestamp_path, "equivTimeline");

static const std::string full_good_path = join_path(iontests_path, good_path);
static const std::string full_good_equivs_path = join_path(full_good_path, "equivs");
static const std::string full_good_nonequivs_path = join_path(full_good_path, "non-equivs");
static const std::string full_good_timestamp_equivtimeline_path = join_path(iontests_path, good_timestamp_equivtimeline_path);
static const std::string full_bad_path = join_path(iontests_path, bad_path);

/**
 * Gather all files of the given type and classification under the ion-tests directory.
 * @param filetype - specifies binary, text, or all files.
 * @param classification - specifies good basic, good equivs, good nonequivs, good timestamp equivtimeline, or bad files.
 * @param files_out - sink for the matching files.
 * @return IERR_OK on success.
 */
iERR gather_files(
      TEST_FILE_TYPE filetype
    , TEST_FILE_CLASSIFICATION classification
    , std::vector<std::string> *files_out
);

#endif //IONC_GATHER_VECTORS_H
