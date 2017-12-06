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

#ifndef IONC_ION_EVENT_UTIL_H
#define IONC_ION_EVENT_UTIL_H

#include "ion.h"

/**
 * Converts an ION_TYPE to a switchable int representing the given type's ID.
 */
#define ION_TID_INT(type) (int)(ION_TYPE_INT(type) >> 8)

#define ION_EVENT_DECIMAL_MAX_DIGITS 10000
#define ION_EVENT_DECIMAL_MAX_STRLEN (ION_EVENT_DECIMAL_MAX_DIGITS + 14) // 14 extra bytes as specified by decNumber.

/**
 * Global variable that holds the decimal context to be used throughout the tools and
 * tests. Initialized to contain arbitrarily high limits, which may be raised if
 * necessary, to avoid loss of precision.
 */
extern decContext g_IonEventDecimalContext;

/**
 * Initializes the given reader options using arbitrarily high limits.
 * @param options - the options to initialize.
 */
void ion_event_initialize_reader_options(ION_READER_OPTIONS *options);

/**
 * Initializes the given writer options using arbitrarily high limits.
 * @param options - the options to initialize.
 */
void ion_event_initialize_writer_options(ION_WRITER_OPTIONS *options);

#endif //IONC_ION_EVENT_UTIL_H
