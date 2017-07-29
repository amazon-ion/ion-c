/*
 * Copyright 2011-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#ifndef IONC_ION_DECIMAL_IMPL_H
#define IONC_ION_DECIMAL_IMPL_H

#include "ion_types.h"

#define ION_DECNUMBER_UNITS_SIZE(decimal_digits) \
    (sizeof(decNumberUnit) * ((((decimal_digits) / DECDPUN) + (((decimal_digits) % DECDPUN) ? 1 : 0))))

// NOTE: each decNumber has DECNUMUNITS preallocated units in its lsu array. These provide space for (DECNUMUNITS * DECDPUN)
// decimal digits. Therefore, space for an additional (decimal_digits - (DECNUMUNITS * DECDPUN)) digits is needed.
#define ION_DECNUMBER_SIZE(decimal_digits) \
    (sizeof(decNumber) + ((decimal_digits > (DECNUMUNITS * DECDPUN)) ? ION_DECNUMBER_UNITS_SIZE(decimal_digits - (DECNUMUNITS * DECDPUN)) : 0))

#define ION_DECIMAL_IS_NUMBER(dec) (dec->type == ION_DECIMAL_TYPE_NUMBER || dec->type == ION_DECIMAL_TYPE_NUMBER_OWNED)

#define ION_DECIMAL_SAVE_STATUS(decimal_status, decimal_context, status_flags) \
    decimal_status = decContextSaveStatus(decimal_context, status_flags); \
    decContextClearStatus(decimal_context, status_flags);

#define ION_DECIMAL_TEST_AND_RESTORE_STATUS(decimal_status, decimal_context, status_flags) \
    if (decContextTestStatus(decimal_context, status_flags)) { \
        /* Status failure occurred; fail loudly. */ \
        FAILWITH(IERR_NUMERIC_OVERFLOW); \
    } \
    decContextRestoreStatus(decimal_context, decimal_status, status_flags);

#ifdef __cplusplus
extern "C" {
#endif

iERR _ion_decimal_number_alloc(void *owner, SIZE decimal_digits, decNumber **p_number);
iERR _ion_decimal_from_string_helper(const char *str, decContext *context, hOWNER owner, decQuad *p_quad, decNumber **p_num);
iERR _ion_decimal_to_string_quad_helper(const decQuad *value, char *p_string);
iERR _ion_decimal_to_string_number_helper(const decNumber *value, char *p_string);

#ifdef __cplusplus
}
#endif

#endif //IONC_ION_DECIMAL_IMPL_H
