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

#include "ion.h"
#include "ion_helpers.h"
#include "ion_decimal_impl.h"
#include "decimal128.h"

/* Internal-only API building blocks */

#define ION_DECIMAL_AS_QUAD(ion_decimal) &ion_decimal->value.quad_value
#define ION_DECIMAL_AS_NUMBER(ion_decimal) ion_decimal->value.num_value

#define ION_DECIMAL_IF_QUAD(ion_decimal) \
decQuad *quad_value; \
decNumber *num_value; \
switch(ion_decimal->type) { \
    case ION_DECIMAL_TYPE_QUAD: \
        quad_value = ION_DECIMAL_AS_QUAD(ion_decimal);

#define ION_DECIMAL_ELSE_IF_NUMBER(ion_decimal) \
        break; \
    case ION_DECIMAL_TYPE_NUMBER_OWNED: \
    case ION_DECIMAL_TYPE_NUMBER: \
        num_value = ION_DECIMAL_AS_NUMBER(ion_decimal);

#define ION_DECIMAL_ENDIF \
        break; \
    default: \
        FAILWITH(IERR_INVALID_ARG); \
}

#define ION_DECIMAL_SWITCH(value, if_quad, if_number) \
iENTER; \
ION_DECIMAL_IF_QUAD(value) { \
    if_quad; \
} \
ION_DECIMAL_ELSE_IF_NUMBER(value) { \
    if_number; \
} \
ION_DECIMAL_ENDIF; \
iRETURN;

/* Memory management */

iERR _ion_decimal_number_alloc(void *owner, SIZE decimal_digits, decNumber **p_number) {
    iENTER;
    ASSERT(p_number);
    if (!owner) {
        *p_number = ion_xalloc(ION_DECNUMBER_SIZE(decimal_digits));
    }
    else {
        *p_number = ion_alloc_with_owner(owner, ION_DECNUMBER_SIZE(decimal_digits));
    }
    if (*p_number == NULL) {
        FAILWITH(IERR_NO_MEMORY);
    }
    iRETURN;
}

iERR ion_decimal_zero(ION_DECIMAL *value) {
    ION_DECIMAL_SWITCH(value, decQuadZero(quad_value), decNumberZero(num_value));
}

iERR ion_decimal_claim(ION_DECIMAL *value) {
    iENTER;
    decNumber *copy;

    switch (value->type) {
        case ION_DECIMAL_TYPE_QUAD:
        case ION_DECIMAL_TYPE_NUMBER:
            // Nothing needs to be done: the decQuads live within the ION_DECIMAL and unowned decNumbers are not tied
            // to an owner.
            break;
        case ION_DECIMAL_TYPE_NUMBER_OWNED:
            // The decNumber may have been allocated with an owner, meaning its memory will go out of scope when that
            // owner is closed. This copy extends that scope until ion_decimal_free.
            IONCHECK(_ion_decimal_number_alloc(NULL, ION_DECIMAL_AS_NUMBER(value)->digits, &copy));
            decNumberCopy(copy, ION_DECIMAL_AS_NUMBER(value));
            ION_DECIMAL_AS_NUMBER(value) = copy;
            value->type = ION_DECIMAL_TYPE_NUMBER;
            break;
        default:
            FAILWITH(IERR_INVALID_ARG);
    }
    iRETURN;
}

iERR ion_decimal_free(ION_DECIMAL *value) {
    iENTER;
    switch (value->type) {
        case ION_DECIMAL_TYPE_NUMBER:
            ion_xfree(ION_DECIMAL_AS_NUMBER(value));
            ION_DECIMAL_AS_NUMBER(value) = NULL;
            break;
        default:
            // Nothing needs to be done; the memory was not dynamically allocated.
            break;
    }
    value->type = ION_DECIMAL_TYPE_UNKNOWN;
    iRETURN;
}

/* Conversions */

iERR _ion_decimal_from_string_helper(const char *str, decContext *context, hOWNER owner, decQuad *p_quad, decNumber **p_num) {
    iENTER;
    const char *cp;
    uint32_t   saved_status;
    SIZE       decimal_digits = 0;

    // NOTE: decFloatFromString and decNumberFromString have been modified to accept both 'e' and 'd'. If the decNumber
    // implementation is updated or swapped out, arrangements need to be made here (or there) to ensure this function
    // can parse exponents denoted by either 'e' or 'd'.
    ION_DECIMAL_SAVE_STATUS(saved_status, context, DEC_Inexact);
    decQuadFromString(p_quad, str, context);
    if (decContextTestStatus(context, DEC_Inexact)) {
        if (p_num) {
            for (cp = str; *cp; cp++) {
                if (*cp == 'd' || *cp == 'D' || *cp == 'e' || *cp == 'E') {
                    break;
                }
                if (*cp != '.') {
                    decimal_digits++;
                }
            }
            decContextClearStatus(context, DEC_Inexact);
            IONCHECK(_ion_decimal_number_alloc(owner, decimal_digits, p_num));
            decNumberFromString(*p_num, str, context);
            if (decContextTestStatus(context, DEC_Inexact)) {
                // The value is too large to fit in any decimal representation. Rather than silently losing precision,
                // fail.
                FAILWITH(IERR_NUMERIC_OVERFLOW);
            }
        }
        else {
            // The value is too large to fit in a decQuad. Rather than silently losing precision, fail.
            FAILWITH(IERR_NUMERIC_OVERFLOW);
        }
    }
    decContextRestoreStatus(context, saved_status, DEC_Inexact);

    iRETURN;
}

iERR ion_decimal_from_string(ION_DECIMAL *value, const char *str, decContext *context) {
    iENTER;
    decNumber *num_value = NULL;
    IONCHECK(_ion_decimal_from_string_helper(str, context, NULL, ION_DECIMAL_AS_QUAD(value), &num_value));
    if (num_value) {
        value->type = ION_DECIMAL_TYPE_NUMBER;
        ION_DECIMAL_AS_NUMBER(value) = num_value;
    }
    else {
        value->type = ION_DECIMAL_TYPE_QUAD;
    }
    iRETURN;
}

void _ion_decimal_to_string_to_ion(char *p_string) {
    char *exp, *dec;
    exp = strchr(p_string, 'E');
    dec = strchr(p_string, '.');
    if (exp) *exp = 'd';
    if (!dec && !exp) {
        strcat(p_string, "d0");
    }
}

#define ION_DECIMAL_TO_STRING_HELPER_BUILDER(is_signed, is_infinite, is_nan, is_zero, digits, to_string) \
    iENTER; \
    ASSERT(value); \
    BOOL is_negative = is_signed(value); \
    if (is_infinite(value)) { \
        strcpy(p_string, is_negative ? "-inf" : "+inf"); \
    } \
    else if (is_nan(value)) { \
        strcpy(p_string, "nan"); \
    } \
    else if (is_zero(value) && !is_negative && digits != 1) { \
        strcpy(p_string, "0d0"); \
    } \
    else { \
        to_string(value, p_string); \
        _ion_decimal_to_string_to_ion(p_string); \
    } \
    iRETURN; \

iERR _ion_decimal_to_string_quad_helper(const decQuad *value, char *p_string) {
    ION_DECIMAL_TO_STRING_HELPER_BUILDER(decQuadIsSigned, decQuadIsInfinite, decQuadIsNaN,
                                         decQuadIsZero, decQuadDigits(value), decQuadToString);
}

iERR _ion_decimal_to_string_number_helper(const decNumber *value, char *p_string) {
    ION_DECIMAL_TO_STRING_HELPER_BUILDER(decNumberIsNegative, decNumberIsInfinite, decNumberIsNaN,
                                         decNumberIsZero, value->digits, decNumberToString);
}

iERR ion_decimal_to_string(const ION_DECIMAL *value, char *p_string) {
    ION_DECIMAL_SWITCH(
        value,
        IONCHECK(_ion_decimal_to_string_quad_helper(quad_value, p_string)),
        IONCHECK(_ion_decimal_to_string_number_helper(num_value, p_string))
    );
}

iERR ion_decimal_from_uint32(ION_DECIMAL *value, uint32_t num) {
    iENTER;
    decQuadFromUInt32(ION_DECIMAL_AS_QUAD(value), num);
    value->type = ION_DECIMAL_TYPE_QUAD;
    iRETURN;
}

iERR ion_decimal_from_int32(ION_DECIMAL *value, int32_t num) {
    iENTER;
    decQuadFromInt32(ION_DECIMAL_AS_QUAD(value), num);
    value->type = ION_DECIMAL_TYPE_QUAD;
    iRETURN;
}

iERR ion_decimal_from_quad(ION_DECIMAL *value, decQuad *quad) {
    iENTER;
    decQuadCopy(ION_DECIMAL_AS_QUAD(value), quad);
    value->type = ION_DECIMAL_TYPE_QUAD;
    iRETURN;
}

iERR ion_decimal_from_number(ION_DECIMAL *value, decNumber *number) {
    iENTER;
    ION_DECIMAL_AS_NUMBER(value) = number;
    value->type = ION_DECIMAL_TYPE_NUMBER_OWNED;
    iRETURN;
}

#define ION_DECIMAL_CONVERSION_API_BUILDER(name, type, if_quad, if_number) \
iERR name(const ION_DECIMAL *value, decContext *context, type *p_out) { \
    iENTER; \
    uint32_t status; \
    ASSERT(p_out); \
    ION_DECIMAL_SAVE_STATUS(status, context, (DEC_Inexact | DEC_Invalid_operation)); \
    ION_DECIMAL_IF_QUAD(value) { \
        *p_out = if_quad(quad_value, context, context->round); \
    } \
    ION_DECIMAL_ELSE_IF_NUMBER(value) { \
        *p_out = if_number(num_value, context); \
    } \
    ION_DECIMAL_ENDIF; \
    ION_DECIMAL_TEST_AND_RESTORE_STATUS(status, context, (DEC_Inexact | DEC_Invalid_operation)); \
    iRETURN; \
}

ION_DECIMAL_CONVERSION_API_BUILDER(ion_decimal_to_int32, int32_t, decQuadToInt32Exact, decNumberToInt32);
ION_DECIMAL_CONVERSION_API_BUILDER(ion_decimal_to_uint32, uint32_t, decQuadToUInt32Exact, decNumberToUInt32);

iERR ion_decimal_to_ion_int(const ION_DECIMAL *value, decContext *context, ION_INT *p_int) {
    iENTER;
    if (!ion_decimal_is_integer(value)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    ION_DECIMAL_IF_QUAD(value) {
        IONCHECK(ion_int_from_decimal(p_int, quad_value, context))
    } \
    ION_DECIMAL_ELSE_IF_NUMBER(value) {
        IONCHECK(_ion_int_from_decimal_number(p_int, num_value, context))
    }
    ION_DECIMAL_ENDIF;
    iRETURN;
}

iERR ion_decimal_from_ion_int(ION_DECIMAL *value, decContext *context, ION_INT *p_int) {
    ION_DECIMAL_SWITCH (
        value,
        IONCHECK(ion_int_to_decimal(p_int, quad_value, context)),
        IONCHECK(_ion_int_to_decimal_number(p_int, num_value, context))
    );
}

/* Internal-only operator API building blocks */

#define ION_DECIMAL_LHS_BIT ((uint8_t)0x1)
#define ION_DECIMAL_RHS_BIT ((uint8_t)0x2)
#define ION_DECIMAL_FHS_BIT ((uint8_t)0x4)

#define ION_DECIMAL_QUAD_TO_NUM(dec, dn, bit) \
    if ((decnum_mask & bit) == 0) { \
        decQuadToNumber(ION_DECIMAL_AS_QUAD(dec), dn); \
    } \
    else { \
        ASSERT(ION_DECIMAL_IS_NUMBER(dec)); \
        dn = ION_DECIMAL_AS_NUMBER(dec); \
    } \

#define ION_DECIMAL_CONVERT_THREE_OPERAND \
    /* These decNumbers only hold values that fit in a decQuad. Each decNumber is guaranteed to have enough space for \
     * any decQuad value (because compilation fails if DECNUMDIGITS < 34), so these don't need extra padding. */ \
    decNumber dn1, dn2, dn3; \
    decNumber *op1 = &dn1, *op2 = &dn2, *op3 = &dn3; \
    ION_DECIMAL_QUAD_TO_NUM(lhs, op1, ION_DECIMAL_LHS_BIT); \
    ION_DECIMAL_QUAD_TO_NUM(rhs, op2, ION_DECIMAL_RHS_BIT); \
    ION_DECIMAL_QUAD_TO_NUM(fhs, op3, ION_DECIMAL_FHS_BIT);

#define ION_DECIMAL_CONVERT_TWO_OPERAND \
    decNumber dn1, dn2; \
    decNumber *op1 = &dn1, *op2 = &dn2; \
    ION_DECIMAL_QUAD_TO_NUM(lhs, op1, ION_DECIMAL_LHS_BIT); \
    ION_DECIMAL_QUAD_TO_NUM(rhs, op2, ION_DECIMAL_RHS_BIT);

#define ION_DECIMAL_RESTORE_QUAD_TWO_OPERAND \
    if (operand_mask & ION_DECIMAL_LHS_BIT) { \
        decQuadCopy(ION_DECIMAL_AS_QUAD(lhs), &temp); \
    } \
    if (operand_mask & ION_DECIMAL_RHS_BIT) { \
        decQuadCopy(ION_DECIMAL_AS_QUAD(rhs), &temp); \
    }

#define ION_DECIMAL_RESTORE_QUAD_THREE_OPERAND \
    ION_DECIMAL_RESTORE_QUAD_TWO_OPERAND; \
    if (operand_mask & ION_DECIMAL_FHS_BIT) { \
        decQuadCopy(ION_DECIMAL_AS_QUAD(fhs), &temp); \
    }

#define ION_DECIMAL_DECNUMBER_CALCULATE(calculate, calculate_operand_mask) \
    decNumber *temp; \
    IONCHECK(_ion_decimal_number_alloc(NULL, context->digits, &temp)); \
    calculate; \
    if (calculate_operand_mask) { \
        IONCHECK(ion_decimal_free(value)); \
    } \
    ION_DECIMAL_AS_NUMBER(value) = temp; \
    value->type = ION_DECIMAL_TYPE_NUMBER;

#define ION_DECIMAL_QUAD_CALCULATE(calculate, on_overflow, recalculate) \
    ION_DECIMAL_SAVE_STATUS(status, context, DEC_Inexact); \
    calculate; \
    value->type = ION_DECIMAL_TYPE_QUAD; \
    if (decContextTestStatus(context, DEC_Inexact)) { \
        /* The operation overflowed the maximum decQuad precision. Convert operands and result to decNumbers. */ \
        on_overflow; \
        recalculate; \
    } \
    decContextRestoreStatus(context, status, DEC_Inexact);

#define ION_DECIMAL_DO_NOTHING /*nothing*/

#define ION_DECIMAL_OVERFLOW_API_BUILDER(name, all_decnums_mask, api_params, calculate_quad, quad_args, \
                                         calculate_decnum_mask, calculate_operand_mask, restore_quad,  \
                                         calculate_number, number_args, helper_params, standardize_operands, \
                                         converted_args, helper_args) \
iERR _##name##_standardized helper_params { \
    iENTER; \
    standardize_operands; \
    ION_DECIMAL_DECNUMBER_CALCULATE(calculate_number converted_args, calculate_operand_mask); \
    iRETURN; \
} \
iERR _##name##_number helper_params { \
    iENTER; \
    ASSERT(decnum_mask == all_decnums_mask); \
    ION_DECIMAL_DECNUMBER_CALCULATE(calculate_number number_args, calculate_operand_mask); \
    iRETURN; \
} \
iERR _##name##_quad_in_place helper_params { \
    iENTER; \
    uint32_t status; \
    uint8_t operand_mask = calculate_operand_mask; \
    decQuad temp; \
    ASSERT(decnum_mask == 0 && operand_mask != 0); \
    decQuadCopy(&temp, ION_DECIMAL_AS_QUAD(value)); \
    ION_DECIMAL_QUAD_CALCULATE(calculate_quad quad_args, /*on_overflow=*/restore_quad, _##name##_standardized helper_args); \
    iRETURN; \
} \
iERR _##name##_quad helper_params { \
    iENTER; \
    uint32_t status; \
    ASSERT(decnum_mask == 0); \
    if (calculate_operand_mask) { \
        /* At least one of the operands is the same as the output parameter. */ \
        IONCHECK(_##name##_quad_in_place helper_args); \
    } \
    else { \
        ION_DECIMAL_QUAD_CALCULATE(calculate_quad quad_args, /*on_overflow=*/ION_DECIMAL_DO_NOTHING, _##name##_standardized helper_args); \
    } \
    iRETURN; \
} \
iERR name api_params { \
    iENTER; \
    uint8_t decnum_mask = calculate_decnum_mask; \
    if (decnum_mask > 0 && decnum_mask < all_decnums_mask) { \
        /* Not all operands have the same type. Convert all non-decNumbers to decNumbers. */ \
        IONCHECK(_##name##_standardized helper_args); \
    } \
    else if (decnum_mask){ \
        /* All operands are decNumbers. */ \
        IONCHECK(_##name##_number helper_args); \
    } \
    else { \
        /* All operands are decQuads. */ \
        IONCHECK(_##name##_quad helper_args); \
    } \
    iRETURN; \
}

#define ION_DECIMAL_COMPUTE_API_BUILDER_THREE_OPERAND(name, calculate_quad, calculate_number) \
    ION_DECIMAL_OVERFLOW_API_BUILDER ( \
        name, \
        /*all_decnums_mask=*/(ION_DECIMAL_LHS_BIT | ION_DECIMAL_RHS_BIT | ION_DECIMAL_FHS_BIT), \
        /*api_params=*/(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, const ION_DECIMAL *fhs, decContext *context), \
        calculate_quad, \
        /*quad_args=*/(ION_DECIMAL_AS_QUAD(value), ION_DECIMAL_AS_QUAD(lhs), ION_DECIMAL_AS_QUAD(rhs), ION_DECIMAL_AS_QUAD(fhs), context), \
        /*calculate_decnum_mask=*/((ION_DECIMAL_IS_NUMBER(lhs) ? ION_DECIMAL_LHS_BIT : 0) | (ION_DECIMAL_IS_NUMBER(rhs) ? ION_DECIMAL_RHS_BIT : 0) | (ION_DECIMAL_IS_NUMBER(fhs) ? ION_DECIMAL_FHS_BIT : 0)), \
        /*calculate_operand_mask=*/((value == lhs ? ION_DECIMAL_LHS_BIT : 0) | (value == rhs ? ION_DECIMAL_RHS_BIT : 0) | (value == fhs ? ION_DECIMAL_FHS_BIT : 0)), \
        /*restore_quad=*/ION_DECIMAL_RESTORE_QUAD_THREE_OPERAND, \
        calculate_number, \
        /*number_args=*/(temp, ION_DECIMAL_AS_NUMBER(lhs), ION_DECIMAL_AS_NUMBER(rhs), ION_DECIMAL_AS_NUMBER(fhs), context), \
        /*helper_params=*/(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, const ION_DECIMAL *fhs, decContext *context, uint8_t decnum_mask), \
        /*standardize_operands=*/ION_DECIMAL_CONVERT_THREE_OPERAND, \
        /*converted_args=*/(temp, op1, op2, op3, context), \
        /*helper_args=*/(value, lhs, rhs, fhs, context, decnum_mask) \
    )

#define ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(name, calculate_quad, calculate_number) \
    ION_DECIMAL_OVERFLOW_API_BUILDER ( \
        name, \
        /*all_decnums_mask=*/(ION_DECIMAL_LHS_BIT | ION_DECIMAL_RHS_BIT), \
        /*api_params=*/(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context), \
        calculate_quad, \
        /*quad_args=*/(ION_DECIMAL_AS_QUAD(value), ION_DECIMAL_AS_QUAD(lhs), ION_DECIMAL_AS_QUAD(rhs), context), \
        /*calculate_decnum_mask=*/((ION_DECIMAL_IS_NUMBER(lhs) ? ION_DECIMAL_LHS_BIT : 0) | (ION_DECIMAL_IS_NUMBER(rhs) ? ION_DECIMAL_RHS_BIT : 0)), \
        /*calculate_operand_mask=*/((value == lhs ? ION_DECIMAL_LHS_BIT : 0) | (value == rhs ? ION_DECIMAL_RHS_BIT : 0)), \
        /*restore_quad=*/ION_DECIMAL_RESTORE_QUAD_TWO_OPERAND, \
        calculate_number, \
        /*number_args=*/(temp, ION_DECIMAL_AS_NUMBER(lhs), ION_DECIMAL_AS_NUMBER(rhs), context), \
        /*helper_params=*/(ION_DECIMAL *value, const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context, uint8_t decnum_mask), \
        /*standardize_operands=*/ION_DECIMAL_CONVERT_TWO_OPERAND, \
        /*converted_args=*/(temp, op1, op2, context), \
        /*helper_args=*/(value, lhs, rhs, context, decnum_mask) \
    )

#define ION_DECIMAL_BASIC_API_BUILDER(name, api_params, calculate_quad, calculate_number) \
iERR name api_params { \
    iENTER; \
    value->type = rhs->type; \
    ION_DECIMAL_IF_QUAD(rhs) { \
        calculate_quad; \
    } \
    ION_DECIMAL_ELSE_IF_NUMBER(rhs) { \
        if (value != rhs) { \
            /* Need to make sure enough space is allocated to hold the result. Because single-operand operations can't \
             * overflow, this only needs to be done for non-in-place calculations, and only needs enough space to hold
             * the number of digits in rhs. */ \
            IONCHECK(_ion_decimal_number_alloc(NULL, num_value->digits, &ION_DECIMAL_AS_NUMBER(value))); \
            value->type = ION_DECIMAL_TYPE_NUMBER; \
        } \
        calculate_number; \
    } \
    ION_DECIMAL_ENDIF; \
    iRETURN; \
}

#define ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(name, calculate_quad_func, calculate_number_func) \
    ION_DECIMAL_BASIC_API_BUILDER( \
        name, \
        (ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context), \
        calculate_quad_func(ION_DECIMAL_AS_QUAD(value), quad_value, context), \
        calculate_number_func(ION_DECIMAL_AS_NUMBER(value), num_value, context) \
    );

/* Operator APIs (computational) */

ION_DECIMAL_COMPUTE_API_BUILDER_THREE_OPERAND(ion_decimal_fma, decQuadFMA, decNumberFMA);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_add, decQuadAdd, decNumberAdd);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_and, decQuadAnd, decNumberAnd);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_divide, decQuadDivide, decNumberDivide);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_divide_integer, decQuadDivideInteger, decNumberDivideInteger);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_max, decQuadMax, decNumberMax);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_max_mag, decQuadMaxMag, decNumberMaxMag);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_min, decQuadMin, decNumberMin);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_min_mag, decQuadMinMag, decNumberMinMag);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_multiply, decQuadMultiply, decNumberMultiply);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_or, decQuadOr, decNumberOr);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_quantize, decQuadQuantize, decNumberQuantize);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_remainder, decQuadRemainder, decNumberRemainder);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_remainder_near, decQuadRemainderNear, decNumberRemainderNear);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_rotate, decQuadRotate, decNumberRotate);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_scaleb, decQuadScaleB, decNumberScaleB);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_shift, decQuadShift, decNumberShift);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_subtract, decQuadSubtract, decNumberSubtract);
ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_xor, decQuadXor, decNumberXor);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_abs, decQuadAbs, decNumberAbs);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_invert, decQuadInvert, decNumberInvert);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_logb, decQuadLogB, decNumberLogB);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_minus, decQuadMinus, decNumberMinus);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_plus, decQuadPlus, decNumberPlus);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_reduce, decQuadReduce, decNumberReduce);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND(ion_decimal_to_integral_exact, decQuadToIntegralExact, decNumberToIntegralExact);

ION_DECIMAL_BASIC_API_BUILDER( \
    ion_decimal_to_integral_value, \
    (ION_DECIMAL *value, const ION_DECIMAL *rhs, decContext *context), \
    decQuadToIntegralValue(ION_DECIMAL_AS_QUAD(value), quad_value, context, context->round), \
    decNumberToIntegralValue(ION_DECIMAL_AS_NUMBER(value), num_value, context) \
);

/* Utility APIs (non-computational) */

#define ION_DECIMAL_UTILITY_API_BUILDER(name, if_quad, if_number) \
uint32_t name(const ION_DECIMAL *value) { \
    ION_DECIMAL_SWITCH(value, return if_quad(quad_value), return if_number(num_value)); \
}

ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_is_finite, decQuadIsFinite, decNumberIsFinite);
ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_is_infinite, decQuadIsInfinite, decNumberIsInfinite);
ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_is_nan, decQuadIsNaN, decNumberIsNaN);
ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_is_negative, decQuadIsNegative, decNumberIsNegative);
ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_is_zero, decQuadIsZero, decNumberIsZero);
ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_is_canonical, decQuadIsCanonical, decNumberIsCanonical);
ION_DECIMAL_UTILITY_API_BUILDER(ion_decimal_radix, decQuadRadix, decNumberRadix);

uint32_t ion_decimal_is_normal(const ION_DECIMAL *value, decContext *context) {
    ION_DECIMAL_SWITCH(value, return decQuadIsNormal(quad_value), return decNumberIsNormal(num_value, context));
}

uint32_t ion_decimal_is_subnormal(const ION_DECIMAL *value, decContext *context) {
    ION_DECIMAL_SWITCH(value, return decQuadIsSubnormal(quad_value), return decNumberIsSubnormal(num_value, context));
}

int32_t ion_decimal_get_exponent(const ION_DECIMAL *value) {
    ION_DECIMAL_SWITCH(value, return decQuadGetExponent(quad_value), return num_value->exponent);
}

uint32_t ion_decimal_digits(const ION_DECIMAL *value) {
    ION_DECIMAL_SWITCH(value, return decQuadDigits(quad_value), return num_value->digits);
}

uint32_t ion_decimal_same_quantum(const ION_DECIMAL *lhs, const ION_DECIMAL *rhs) {
    return (uint32_t)(ion_decimal_get_exponent(lhs) == ion_decimal_get_exponent(rhs));
}

uint32_t ion_decimal_is_integer(const ION_DECIMAL *value) {
    ION_DECIMAL_SWITCH(value, return decQuadIsInteger(quad_value), return num_value->exponent == 0);
}

/* Comparisons */

iERR _ion_decimal_compare_quad(const decQuad *left, const decQuad *right, decContext *context, int32_t *result) {
    iENTER;
    decQuad res;
    decQuadCompareTotal(&res, left, right);
    *result = decQuadToInt32(&res, context, context->round);
    iRETURN;
}

iERR _ion_decimal_compare_number(const decNumber *left, const decNumber *right, decContext *context, int32_t *result) {
    iENTER;
    decNumber res;
    decNumberCompareTotal(&res, left, right, context);
    *result = decNumberToInt32(&res, context);
    iRETURN;
}

iERR _ion_decimal_comare_helper(const ION_DECIMAL *lhs, const ION_DECIMAL *rhs, decContext *context, int32_t *result) {
    iENTER;
    int decnum_mask = (ION_DECIMAL_IS_NUMBER(lhs) ? ION_DECIMAL_LHS_BIT : 0) | (ION_DECIMAL_IS_NUMBER(rhs) ? ION_DECIMAL_RHS_BIT : 0);
    ION_DECIMAL_CONVERT_TWO_OPERAND;
    IONCHECK(_ion_decimal_compare_number(op1, op2, context, result));
    iRETURN;
}

iERR ion_decimal_compare(const ION_DECIMAL *left, const ION_DECIMAL *right, decContext *context, int32_t *result) {
    iENTER;
    ASSERT(result);
    if (ION_DECIMAL_IS_NUMBER(left) ^ ION_DECIMAL_IS_NUMBER(right)) {
        if (left->type == ION_DECIMAL_TYPE_UNKNOWN || right->type == ION_DECIMAL_TYPE_UNKNOWN) {
            FAILWITH(IERR_INVALID_ARG);
        }
        IONCHECK(_ion_decimal_comare_helper(left, right, context, result));
        SUCCEED();
    }
    switch (left->type) {
        case ION_DECIMAL_TYPE_QUAD:
            IONCHECK(_ion_decimal_compare_quad(ION_DECIMAL_AS_QUAD(left), ION_DECIMAL_AS_QUAD(right), context, result));
            break;
        case ION_DECIMAL_TYPE_NUMBER_OWNED:
        case ION_DECIMAL_TYPE_NUMBER:
            IONCHECK(_ion_decimal_compare_number(ION_DECIMAL_AS_NUMBER(left), ION_DECIMAL_AS_NUMBER(right), context, result));
            break;
        default:
        FAILWITH(IERR_INVALID_ARG);
    }
    iRETURN;
}

iERR ion_decimal_equals_quad(const decQuad *left, const decQuad *right, decContext *context, BOOL *is_equal) {
    iENTER;
    int32_t result;
    IONCHECK(_ion_decimal_compare_quad(left, right, context, &result));
    *is_equal = !result;
    iRETURN;
}

iERR ion_decimal_equals(const ION_DECIMAL *left, const ION_DECIMAL *right, decContext *context, BOOL *is_equal) {
    iENTER;
    int32_t result;
    IONCHECK(ion_decimal_compare(left, right, context, &result));
    *is_equal = !result;
    iRETURN;
}

/* Copies */

#define ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND_NO_CONTEXT(name, calculate_quad_func, calculate_number_func) \
    ION_DECIMAL_BASIC_API_BUILDER( \
        name, \
        (ION_DECIMAL *value, const ION_DECIMAL *rhs), \
        calculate_quad_func(ION_DECIMAL_AS_QUAD(value), quad_value), \
        calculate_number_func(ION_DECIMAL_AS_NUMBER(value), num_value) \
    );

ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND_NO_CONTEXT(ion_decimal_canonical, decQuadCanonical, decNumberCopy); /* decNumbers are always canonical. */
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND_NO_CONTEXT(ion_decimal_copy, decQuadCopy, decNumberCopy);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND_NO_CONTEXT(ion_decimal_copy_abs, decQuadCopyAbs, decNumberCopyAbs);
ION_DECIMAL_COMPUTE_API_BUILDER_ONE_OPERAND_NO_CONTEXT(ion_decimal_copy_negate, decQuadCopyNegate, decNumberCopyNegate);

void _decQuad_copy_sign_drop_context(decQuad *value, const decQuad *lhs, const decQuad *rhs, decContext *context) {
    decQuadCopySign(value, lhs, rhs);
}

void _decNumber_copy_sign_drop_context(decNumber *value, const decNumber *lhs, const decNumber *rhs, decContext *context) {
    decNumberCopySign(value, lhs, rhs);
}

ION_DECIMAL_COMPUTE_API_BUILDER_TWO_OPERAND(ion_decimal_copy_sign, _decQuad_copy_sign_drop_context, _decNumber_copy_sign_drop_context);
