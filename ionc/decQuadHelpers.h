/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#ifndef DECQUADHELPERS_H_
#define DECQUADHELPERS_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BILLION      (int64_t)1000000000            /* 10**9                 */

//
// these are actually in decQuadHelpers.c
// they really should be part of the decimal package, but
// that's a bit more trouble than it's worth just at the moment (and
// that would also be more work than the specific cases we need just now as well)
//
ION_API_EXPORT void    ion_quad_get_exponent_and_shift(const decQuad *quad_value, decContext *set, decQuad *p_int_mantissa, int32_t *p_exp);
ION_API_EXPORT iERR    ion_quad_get_quad_from_digits_and_exponent(uint64_t value, int32_t exp, decContext *set, BOOL is_negative, decQuad *p_quad);

// exported method for getting packed representation.
// decimal package may be statically linked into the DLL (on win32) with no export
// this makes sure this facility is exported.
// This is equivalent to decQuadToPacked.
ION_API_EXPORT void    ion_quad_get_packed_and_exponent_from_quad(const decQuad *quad_value, uint8_t *p_packed, int32_t *p_exp);

uint64_t decQuadToUInt64(const decQuad *df, decContext *set, BOOL *p_overflow, BOOL *p_is_negative);
double   decQuadToDouble(const decQuad *dec, decContext *set);

#ifdef __cplusplus
}
#endif

#endif /* DECQUADHELPERS_H_ */
