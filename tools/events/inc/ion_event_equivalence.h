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

#ifndef IONC_ION_EVENT_EQUIVALENCE_H
#define IONC_ION_EVENT_EQUIVALENCE_H

#include <cmath>
#include <ionc/ion.h>
#include "ion_event_stream.h"

typedef enum _ion_event_comparison_type {
    COMPARISON_TYPE_EQUIVS = 0,
    COMPARISON_TYPE_NONEQUIVS,
    COMPARISON_TYPE_BASIC,
    COMPARISON_TYPE_EQUIVTIMELINE,
    COMPARISON_TYPE_UNKNOWN
} ION_EVENT_COMPARISON_TYPE;

/**
 * Tests the given IonEventStreams for equivalence, meaning that the corresponding values in each stream
 * must all be equivalent.
 */
BOOL ion_compare_streams(IonEventStream *stream_expected, IonEventStream *stream_actual, IonEventResult *result = NULL);

/**
 * Compares the comparison sets contained in the given stream based on the given comparison type. A comparison set
 * is a top-level sequence type (list or s-expression) that contains values or embedded streams that will be compared
 * for equivalence or non-equivalence against all other values or embedded streams in that sequence.
 */
BOOL ion_compare_sets(IonEventStream *stream_expected, IonEventStream *stream_actual,
                      ION_EVENT_COMPARISON_TYPE comparison_type, IonEventResult *result = NULL);

// Equivalence checks. If the optional failure_message output parameter is supplied and the given values are not
// equivalent, failure_message will be populated with a message explaining why. If the optional result output parameter
// is supplied, it will be populated with an IonEventErrorDescription if an error occurs during the comparison. If an
// error occurs, the function will always return FALSE.

BOOL ion_equals_bool(BOOL *expected, BOOL *actual, std::string *failure_message = NULL, IonEventResult *result = NULL);


BOOL ion_equals_string(ION_STRING *expected, ION_STRING *actual, std::string *failure_message = NULL,
                       IonEventResult *result = NULL);


BOOL ion_equals_symbol(ION_SYMBOL *expected, ION_SYMBOL *actual, std::string *failure_message = NULL,
                       IonEventResult *result = NULL);


BOOL ion_equals_int(ION_INT *expected, ION_INT *actual, std::string *failure_message = NULL,
                    IonEventResult *result = NULL);


BOOL ion_equals_decimal(ION_DECIMAL *expected, ION_DECIMAL *actual, std::string *failure_message = NULL,
                        IonEventResult *result = NULL);


BOOL ion_equals_float(double *expected, double *actual, std::string *failure_message = NULL,
                      IonEventResult *result = NULL);

/**
 * Uses ion_timestamp_equals as the comparison method.
 */
BOOL ion_equals_timestamp(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message = NULL,
                          IonEventResult *result = NULL);

/**
 * Uses ion_timestamp_instant_equals as the comparison method.
 */
BOOL ion_equals_timestamp_instant(ION_TIMESTAMP *expected, ION_TIMESTAMP *actual, std::string *failure_message,
                                  IonEventResult *ION_RESULT_ARG);

#endif //IONC_ION_EVENT_EQUIVALENCE_H
