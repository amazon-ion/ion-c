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

#include "ion_event_util.h"

decContext g_IonEventDecimalContext = {
        ION_EVENT_DECIMAL_MAX_DIGITS,   // max digits (arbitrarily high -- raise if test data requires more)
        DEC_MAX_MATH,                   // max exponent
        -DEC_MAX_MATH,                  // min exponent
        DEC_ROUND_HALF_EVEN,            // rounding mode
        DEC_Errors,                     // trap conditions
        0,                              // status flags
        0                               // apply exponent clamp?
};

void ion_event_initialize_writer_options(ION_WRITER_OPTIONS *options) {
    memset(options, 0, sizeof(ION_WRITER_OPTIONS));
    options->decimal_context = &g_IonEventDecimalContext;
    options->max_container_depth = 100; // Arbitrarily high; if any test vector exceeds this depth, raise this threshold.
    options->max_annotation_count = 100; // "
}

void ion_event_initialize_reader_options(ION_READER_OPTIONS *options) {
    memset(options, 0, sizeof(ION_READER_OPTIONS));
    options->decimal_context = &g_IonEventDecimalContext;
    options->max_container_depth = 100; // Arbitrarily high; if any test vector exceeds this depth, raise this threshold.
    options->max_annotation_count = 100; // "
}

