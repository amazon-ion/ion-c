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

/**
 * This module provides routines for working with arbitrary-precision decimal numbers in accordance with the decimal
 * data model defined in the Ion Specification.
 *
 * For computational APIs, the output parameter (usually the first) is considered to be an uninitialized value (i.e.
 * no effort is made to free its associated memory before overwriting it) UNLESS it is the same reference as one of
 * the operands. If it is the same reference as one of the operands, the operation will be performed in-place if
 * possible. If not possible, its memory will be freed (if necessary) and replaced with newly allocated memory to hold
 * the result. This means that it is important not to reuse ION_DECIMAL instances as output parameters for non-in-place
 * operations without freeing them using `ion_decimal_free` first. Doing so could result in a memory leak.
 *
 * APIs without documentation within this file are shims to counterparts within the decNumber library. These
 * counterparts are named dec[Number|Quad]<X>, where X is the camelCased function name without the `ion_decimal_`
 * prefix. Detailed documentation for those APIs can be found within the decNumber library.
 *
 * To avoid memory leaks, `ion_decimal_free` should be called on all ION_DECIMALs before they go out of scope AND before
 * they are overwritten by non-in-place operations.
 */

/**@file */

#ifndef ION_DECIMAL_H_
#define ION_DECIMAL_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifndef DECNUMDIGITS
    #error DECNUMDIGITS must be defined to be >= DECQUAD_Pmax
#elif DECNUMDIGITS < DECQUAD_Pmax
    #error DECNUMDIGITS must be defined to be >= DECQUAD_Pmax
#endif

/**
 * Provides the byte size required to hold the string representation of the given decimal.
 */
#define ION_DECIMAL_STRLEN(ion_decimal) \
    ((size_t)(((ion_decimal)->type == ION_DECIMAL_TYPE_QUAD) \
        ? DECQUAD_String \
        : (((ion_decimal)->type == ION_DECIMAL_TYPE_UNKNOWN) \
            ? -1 \
            : ((ion_decimal)->value.num_value->digits + 14) /* +14 is specified by decNumberToString. */ \
        ) \
    ))

#ifdef __cplusplus
extern "C" {
#endif


/*
 * Internal structure definitions. The internals should NOT be depended upon (or cared about) by the end user.
 */

/**
 * Determines which value of the _ion_decimal's `value` field is active.
 */
typedef enum {
    ION_DECIMAL_TYPE_UNKNOWN = 0,
    /**
     * The _ion_decimal holds a decQuad.
     */
    ION_DECIMAL_TYPE_QUAD = 1,
    /**
     * The _ion_decimal holds an unowned decNumber.
     */
    ION_DECIMAL_TYPE_NUMBER = 2,
    /**
     * The _ion_decimal holds a decNumber whose memory is managed by an owner.
     */
    ION_DECIMAL_TYPE_NUMBER_OWNED = 3,

} ION_DECIMAL_TYPE;

struct _ion_decimal {

    ION_DECIMAL_TYPE type;

    union {
        decQuad quad_value;
        decNumber *num_value;
    } value;
};


/* Memory management */

/**
 * Zeroes the given ION_DECIMAL in-place. NOTE: this has better performance than memset in certain environments.
 *
 * @param value - the value to zero.
 * @return IERR_OK (no errors are possible).
 */
ION_API_EXPORT iERR ion_decimal_zero(ION_DECIMAL *value);

/**
 * If necessary, copies the given decimal's internal data so that owner of that data may safely go out of scope. This is
 * useful, for example, when it is necessary to keep the value in scope after the reader that produced it is closed.
 * Values produced through calls to `ion_decimal_*` APIs (with the possible exception of `ion_decimal_from_number`) do
 * NOT need to be claimed.
 *
 * @param value - The value to claim.
 */
ION_API_EXPORT iERR ion_decimal_claim(ION_DECIMAL *value);

/**
 * Frees any memory that was allocated when constructing this value. This should be called to clean up all ION_DECIMALs.
 *
 * @param value - The value to free.
 */
ION_API_EXPORT iERR ion_decimal_free(ION_DECIMAL *value);


/* Conversions */

/**
 * Converts the given ION_DECIMAL to a string. `ION_DECIMAL_STRLEN` may be used to determine the amount of space
 * required to hold the string representation.
 *
 * @return IERR_OK (no errors are possible).
 */
ION_API_EXPORT iERR ion_decimal_to_string(const ION_DECIMAL *value, char *p_string);

/**
 * Converts the given string to its ION_DECIMAL representation.
 *
 * @param value - a non-null pointer to the resulting decimal.
 * @param str - a null-terminated string representing a decimal. Exponents (if any) may be indicated using either 'd'
 *   or 'e'.
 * @param context - the context to use for the conversion. If the decimal lies outside of the context's limits, an error
 *   is raised.
 * @return IERR_NUMERIC_OVERFLOW if the decimal lies outside of the context's limits, otherwise IERR_OK.
 */
ION_API_EXPORT iERR ion_decimal_from_string(ION_DECIMAL *value, const char *str, decContext *context);

/**
 * Represents the given uint32 as an ION_DECIMAL.
 *
 * @return IERR_OK (no errors are possible).
 */
ION_API_EXPORT iERR ion_decimal_from_uint32(ION_DECIMAL *value, uint32_t num);

/**
 * Represents the given int32 as an ION_DECIMAL.
 *
 * @return IERR_OK (no errors are possible).
 */
ION_API_EXPORT iERR ion_decimal_from_int32(ION_DECIMAL *value, int32_t num);

/**
 * Represents the given decQuad as an ION_DECIMAL. The caller IS NOT required to keep the given decQuad in scope for the
 * lifetime of the resulting ION_DECIMAL.
 *
 * @return IERR_OK (no errors are possible).
 */
ION_API_EXPORT iERR ion_decimal_from_quad(ION_DECIMAL *value, decQuad *quad);

/**
 * Represents the given decNumber as an ION_DECIMAL. This function does not allocate or copy any memory, so the caller
 * IS required to keep the given decNumber in scope for the lifetime of the resulting ION_DECIMAL. If desired, the
 * caller can alleviate this requirement by calling `ion_decimal_claim` on the resulting ION_DECIMAL (note that this
 * forces a copy). It is the caller's responsibility to eventually free any dynamically allocated memory used by the
 * given decNumber (calling `ion_decimal_free` will not free this memory).
 *
 * @return IERR_OK (no errors are possible).
 */
ION_API_EXPORT iERR ion_decimal_from_number(ION_DECIMAL *value, decNumber *number);

/**
 * Converts the given ION_INT to its ION_DECIMAL representation.
 */
ION_API_EXPORT iERR ion_decimal_from_ion_int(ION_DECIMAL *value, decContext *context, ION_INT *p_int);

/**
 * Converts the given ION_DECIMAL to its ION_INT representation. If the given ION_DECIMAL is not an integer,
 * IERR_INVALID_ARG will be returned; rounding will never occur. If rounding is desired, use
 * `ion_decimal_to_integral_exact` or `ion_decimal_to_integral_value` first.
 */
ION_API_EXPORT iERR ion_decimal_to_ion_int(const ION_DECIMAL *value, decContext *context, ION_INT *p_int);

ION_API_EXPORT iERR ion_decimal_to_int32(const ION_DECIMAL *value, decContext *context, int32_t *p_int);
ION_API_EXPORT iERR ion_decimal_to_uint32(const ION_DECIMAL *value, decContext *context, uint32_t *p_int);


/* Operator APIs (computational) */

ION_API_EXPORT iERR ion_decimal_fma(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, const ION_DECIMAL *fhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_add(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_and(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_divide(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_divide_integer(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_max(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_max_mag(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_min(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_min_mag(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_multiply(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_or(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_quantize(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_remainder(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_remainder_near(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_rotate(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_scaleb(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_shift(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_subtract(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_xor(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_abs(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_invert(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_logb(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_minus(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_plus(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_reduce(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_to_integral_exact(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);
ION_API_EXPORT iERR ion_decimal_to_integral_value(ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context);


/* Utility APIs (non-computational) */

ION_API_EXPORT uint32_t ion_decimal_digits(const ION_DECIMAL *value);
ION_API_EXPORT int32_t ion_decimal_get_exponent(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_radix(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_same_quantum(const ION_DECIMAL *lhs, const ION_DECIMAL *rhs);
ION_API_EXPORT uint32_t ion_decimal_is_integer(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_is_subnormal(const ION_DECIMAL *value, decContext *context);
ION_API_EXPORT uint32_t ion_decimal_is_normal(const ION_DECIMAL *value, decContext *context);
ION_API_EXPORT uint32_t ion_decimal_is_finite(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_is_infinite(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_is_nan(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_is_negative(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_is_zero(const ION_DECIMAL *value);
ION_API_EXPORT uint32_t ion_decimal_is_canonical(const ION_DECIMAL *value);

/* Comparisons */

/**
 * Compares ION_DECIMALs for ordering and equivalence under the Ion data model. A negative result indicates that `left`
 * is less than `right`. A positive result indicates that `left` is greater than `right`. A result of zero indicates
 * that `left` and `right` are equivalent under the Ion data model. Non-equivalent values are ordered according to the
 * IEEE 754 total ordering.
 */
ION_API_EXPORT iERR ion_decimal_compare(const ION_DECIMAL *left, const ION_DECIMAL *right, decContext *context, int32_t *result);

/**
 * Compares decQuads for equivalence under the Ion data model. That is, the sign, coefficient, and exponent must be
 * equivalent for the normalized values (even for zero).
 *
 * @deprecated - use of decQuads directly is deprecated. ION_DECIMAL should be used. See `ion_decimal_equals`.
 */
ION_API_EXPORT iERR ion_decimal_equals_quad(const decQuad *left, const decQuad *right, decContext *context, BOOL *is_equal);

/**
 * Compares ION_DECIMALs for equivalence under the Ion data model. That is, the sign, coefficient, and exponent must be
 * equivalent for the normalized values (even for zero).
 */
ION_API_EXPORT iERR ion_decimal_equals(const ION_DECIMAL *left, const ION_DECIMAL *right, decContext *context, BOOL *is_equal);

/* Copies */

ION_API_EXPORT iERR ion_decimal_canonical(ION_DECIMAL *value, const ION_DECIMAL *rhs);
ION_API_EXPORT iERR ion_decimal_copy(ION_DECIMAL *value, const ION_DECIMAL *rhs);
ION_API_EXPORT iERR ion_decimal_copy_abs(ION_DECIMAL *value, const ION_DECIMAL *rhs);
ION_API_EXPORT iERR ion_decimal_copy_negate(ION_DECIMAL *value, const ION_DECIMAL *rhs);
ION_API_EXPORT iERR ion_decimal_copy_sign(ION_DECIMAL *value, const ION_DECIMAL *rhs, const ION_DECIMAL *lhs, decContext *context);

#ifdef __cplusplus
}
#endif

#endif /* ION_DECIMAL_H_ */
