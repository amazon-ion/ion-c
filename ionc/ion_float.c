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

#include "ion.h"

#define ION_FLOAT_NEG_ZERO_DOUBLE 0x8000000000000000

BOOL ion_float_is_negative_zero(double value) {
    long neg_zero_bits = ION_FLOAT_NEG_ZERO_DOUBLE;
    return !memcmp(&neg_zero_bits, &value, sizeof(double));
}

