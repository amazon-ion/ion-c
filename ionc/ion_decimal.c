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
#include "ion_helpers.h"

iERR ion_decimal_equals(const decQuad *left, const decQuad *right, decContext *context, BOOL *is_equal) {
    iENTER;
    decQuad left_canonical, right_canonical;
    uint8_t left_coefficient[DECQUAD_Pmax], right_coefficient[DECQUAD_Pmax];

    if (!left || !right || !context || !is_equal) FAILWITH(IERR_INVALID_ARG);

    if (!decQuadIsCanonical(left)) {
        decQuadCanonical(&left_canonical, left);
    }
    else {
        left_canonical = *left;
    }
    if (!decQuadIsCanonical(right)) {
        decQuadCanonical(&right_canonical, right);
    }
    else {
        right_canonical = *right;
    }

    if (decQuadGetExponent(&left_canonical) != decQuadGetExponent(&right_canonical)) {
        goto not_equal;
    }
    if (decQuadGetCoefficient(&left_canonical, left_coefficient)
            != decQuadGetCoefficient(&right_canonical, right_coefficient)) {
        goto not_equal;
    }
    if (memcmp(left_coefficient, right_coefficient, DECQUAD_Pmax)) {
        goto not_equal;
    }
    *is_equal = TRUE;
    SUCCEED();
not_equal:
    *is_equal = FALSE;
    iRETURN;
}

