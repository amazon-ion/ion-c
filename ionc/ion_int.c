/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

//
// Ion int, arbitrary integer representation
//
// Primary use cases are deserialization and serialization
// of arbitarty integer values in either text (decimal) 
// or binary (base 256) representation.  This requires
// the ability to convert between the two representations.
// This does include a limited abilty to get the values
// out in more convnetional data formats as well.
//
// ion int's are generally immutable. That is the value
// cannot be changed but it may be necessary to create
// the "other" representation after the initial value has
// already been set.  This mutates the internal structure
// but does not change the exernally visible value.
//
// There are likely to be problems if the integer gets
// up in to the 2 gig bits size, since a number of the
// routines calculate "bit count" using a SIZE or perhaps
// an int32_t or an int.  (this isn't likely to be the
// biggest issue, but it's known)
//

#define ION_INT_INITTO(x) =(x)
#define ION_INT_GLOBAL /* static */
#include "ion_internal.h"

iERR ion_int_alloc(void *owner, ION_INT **piint)
{
    iENTER;
    ION_INT *iint;

    if (!piint) FAILWITH(IERR_INVALID_ARG);

    iint = (ION_INT *)_ion_int_realloc_helper(NULL, 0, owner, sizeof(ION_INT));
    if (!iint) FAILWITH(IERR_NO_MEMORY);

    _ion_int_init(iint, owner);
    *piint = iint;
    SUCCEED();

    iRETURN;
}


void ion_int_free(ION_INT *iint) 
{
    if (iint && NULL == iint->_owner) {
        if (iint->_digits) {
            ion_xfree(iint->_digits);
            iint->_digits = NULL;;
        }
        ion_xfree(iint);  // TODO: what allocator cover should I be using here?  xalloc?
    }
    return;
}


iERR ion_int_init(ION_INT *iint, void *owner)
{
    iENTER;  
    if (!iint) FAILWITH(IERR_INVALID_ARG);
    _ion_int_init(iint, owner);
    SUCCEED();
    iRETURN;
}


iERR ion_int_is_null(ION_INT *iint, BOOL *p_is_null)
{
    iENTER;
    BOOL is_null;

    if (!iint) {
        FAILWITH(IERR_INVALID_ARG);
    }

    is_null = _ion_int_is_null_helper(iint);
    if (*p_is_null) {
        *p_is_null = is_null;
    }
    SUCCEED();

    iRETURN;
}


iERR ion_int_is_zero(ION_INT *iint, BOOL *p_bool) 
{
    iENTER;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_bool))

    if (iint->_signum == 0) {
        *p_bool = TRUE;
    }
    else {
        ASSERT(iint->_digits); // if iint isn't 0 or null, there better be some bits
        *p_bool = _ion_int_is_zero_bytes(iint->_digits, iint->_len);
    }
    SUCCEED();

    iRETURN;
}


iERR ion_int_compare(ION_INT *iint1, ION_INT *iint2, int *p_result)
{
    iENTER;
    BOOL      is_null1, is_null2;
    SIZE    bits1, bits2;
    SIZE    count;
    II_DIGIT  digit1, digit2;
    II_DIGIT *digits1, *digits2;

    if (!iint1) FAILWITH(IERR_INVALID_ARG);
    if (!iint2) FAILWITH(IERR_INVALID_ARG);
    if (!p_result) FAILWITH(IERR_INVALID_ARG);
    
    if (iint1 == iint2) {
        *p_result = 0;
        SUCCEED();
    }

    IONCHECK(ion_int_is_null(iint1, &is_null1));
    IONCHECK(ion_int_is_null(iint2, &is_null2));
    if (is_null1 || is_null2) {
        *p_result = (is_null1 - is_null2);  // TODO : really?
        SUCCEED();
    }
    
    // check the sign value
    *p_result = (iint1->_signum != iint2->_signum);
    if (*p_result) goto not_equal;

    // sign is the same, we'll clear out the zero case here
    if (iint1->_signum == 0) goto equal;
    
    // otherwise we look at the  most bits
    bits1 = _ion_int_highest_bit_set_helper(iint1);
    bits2 = _ion_int_highest_bit_set_helper(iint2);
    *p_result = bits2 - bits1;
    if (*p_result) goto not_equal;
    
    // finally - we have to actually check the bits themselves
    count = ((bits1 - 1) / II_BITS_PER_II_DIGIT) + 1;
    digits1 = iint1->_digits + (iint1->_len - count);
    digits2 = iint2->_digits + (iint2->_len - count);
    while(count-- > 0) {
        digit1 = *digits1++;
        digit2 = *digits2++;
        *p_result = digit2 - digit1;
        if (*p_result) goto not_equal;
    }
    goto equal;

equal:
    *p_result = 0;
    SUCCEED();

not_equal:
    if (iint1->_signum < 0) {
        *p_result = -(*p_result);
    }
    SUCCEED();

    iRETURN;
}


iERR ion_int_signum(ION_INT *iint, int32_t *p_signum) // signum = function(t) {
{
    iENTER;

    if (!iint || !p_signum) {
        FAILWITH(IERR_INVALID_ARG);
    }

    *p_signum = iint->_signum;
    SUCCEED();

    iRETURN;
}


iERR ion_int_highest_bit_set(ION_INT *iint, SIZE *p_pos)
{
    iENTER;
    
    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_pos))
    
    *p_pos = _ion_int_highest_bit_set_helper(iint);
    SUCCEED();
  
    iRETURN;
}


iERR ion_int_from_string(ION_INT *iint, const iSTRING p_str)
{
    iENTER;
    
    IONCHECK(_ion_int_validate_arg_with_ptr(iint, p_str));
    IONCHECK(_ion_int_from_chars_helper(iint, (const char *)(p_str->value), p_str->length));
    SUCCEED();
  
    iRETURN;
}

iERR ion_int_from_chars(ION_INT *iint, const char *p_chars, SIZE char_limit)
{
    iENTER;
    
    IONCHECK(_ion_int_validate_arg_with_ptr(iint, p_chars));
    IONCHECK(_ion_int_from_chars_helper(iint, p_chars, char_limit));
    SUCCEED();
  
    iRETURN;
}

iERR _ion_int_from_chars_helper(ION_INT *iint, const char *str, SIZE len)
{
    iENTER;
    const char *cp, *end;
    int        signum = 1;
    int        decimal_digits, bits, ii_length;
    BOOL       is_zero;
    II_DIGIT  *digits, d;
 

    cp = str;
    end = cp + len;
      
    // skip leading white space
    while (cp < end && isspace(*cp)) cp++;
    if (cp >= end) FAILWITH(IERR_INVALID_SYNTAX);

    switch(*cp) {
    case 'n':
        if (strncmp(cp, "null", 5) ||  strncmp(cp, "null.int", 9)) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        iint->_signum = 0;
        iint->_len = 0;
        iint->_digits = NULL;
        SUCCEED();
    case II_MINUS:
        signum = -1;
        // fall through to plus, then to default
    case II_PLUS:
        cp++;
        if (cp >= end) FAILWITH(IERR_INVALID_SYNTAX);
        // fall through
    case '0': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9':
        break;
    default:
        FAILWITH(IERR_INVALID_SYNTAX);
    }
    
    decimal_digits = (SIZE)(end - cp); // since these live within a string whose length is of type SIZE
    if (*cp == '0') {
        if (decimal_digits > 1 && *(cp+1) == '0') {
            // only 1 leading zero
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        decimal_digits--; // we don't count the leading zero for this, it doesn't add bits
    }
    
    bits = (SIZE)((II_BITS_PER_DEC_DIGIT * decimal_digits) + 1);
    ii_length = (SIZE)(((double)(bits - 1) / II_BITS_PER_II_DIGIT) + 1);
    IONCHECK(_ion_int_extend_digits(iint, ii_length, TRUE));
    
    is_zero = TRUE;
    digits = iint->_digits;
    while (cp < end) {
        if (!isdigit(*cp)) FAILWITH(IERR_INVALID_SYNTAX);
        d = *cp++ - '0';
        if (d) is_zero = FALSE;
        //mult_add(iint, iint, 10, d);
        IONCHECK(_ion_int_multiply_and_add(digits, iint->_len, II_STRING_BASE, d));
    }
    
    // set the signum value now
    if (is_zero) {
        iint->_signum = 0;
    }
    else {
        iint->_signum = signum;
    }
    SUCCEED();
  
    iRETURN;
}


iERR ion_int_from_hex_string(ION_INT *iint, const iSTRING p_str)
{
    iENTER;
    
    IONCHECK(_ion_int_validate_arg_with_ptr(iint, p_str));
    IONCHECK(_ion_int_from_hex_chars_helper(iint, (const char *)(p_str->value), p_str->length));
    SUCCEED();
  
    iRETURN;
}

iERR ion_int_from_hex_chars(ION_INT *iint, const char *p_chars, SIZE char_limit)
{
    iENTER;
    
    IONCHECK(_ion_int_validate_arg_with_ptr(iint, p_chars));
    IONCHECK(_ion_int_from_hex_chars_helper(iint, p_chars, char_limit));
    SUCCEED();
  
    iRETURN;
}

static int _ion_int_hex_digit_values[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //   0-15
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  16-31
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  32-47
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, //  48-63 0-9
	0,10,11,12,13,14,15, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  64-79 A-F
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  80-95
	0,10,11,12,13,14,15, 0, 0, 0, 0, 0, 0, 0, 0, 0, //  96-111 a-f
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 112-127
};

iERR _ion_int_from_hex_chars_helper(ION_INT *iint, const char *str, SIZE len)
{
    iENTER;
    const char *cp, *end;
    char       c;
    int        signum = 1;
    SIZE       hex_digits, bits, ii_length;
    BOOL       is_zero;
    II_DIGIT  *digits, d;
 
    cp = str;
    end = cp + len;
      
    // skip leading white space
    while (cp < end && isspace(*cp)) cp++;
    if (cp >= end) goto bad_syntax;

    switch(*cp) {
    case II_MINUS:
        signum = -1;
        cp++;
        if (cp >= end) goto bad_syntax;
        if (*cp != '0') goto bad_syntax;
        // fall through to leading 0
    case '0': 
        cp++;
        if (cp >= end) goto bad_syntax;
        if (*cp != 'x' && *cp != 'X') goto bad_syntax;
        cp++;
        if (cp >= end) goto bad_syntax;
        break;
    default:
        goto bad_syntax;
    }

    // scan past leading 0's
    while (*cp == '0') {
        cp++;
        if (cp >= end) break;
    }

    hex_digits = (SIZE)(end - cp); // since strings are limited to length SIZE
    bits = (SIZE)((II_BITS_PER_HEX_DIGIT * hex_digits) + 1);
    ii_length = (SIZE)(((double)(bits - 1) / II_BITS_PER_II_DIGIT) + 1);
    IONCHECK(_ion_int_extend_digits(iint, ii_length, TRUE));
    
    is_zero = TRUE;
    digits = iint->_digits;
    while (cp < end) {
        c = *cp++;
        if (!isxdigit(c)) goto bad_syntax;
        d = _ion_int_hex_digit_values[c];
        if (d) is_zero = FALSE;
        //mult_add(iint, iint, 10, d); 
        IONCHECK(_ion_int_multiply_and_add(digits, iint->_len, II_HEX_BASE, d));
    }
    
    // set the signum value now
    if (is_zero) {
        iint->_signum = 0;
    }
    else {
        iint->_signum = signum;
    }
    SUCCEED();

bad_syntax:
    FAILWITH(IERR_INVALID_SYNTAX);
  
    iRETURN;
}


iERR ion_int_from_bytes(ION_INT *iint, BYTE *buf, SIZE limit)
{
    iENTER;
    BOOL     is_neg, is_zero, may_overflow;
    SIZE   byte_idx, byte_count, bits, ii_length;
    BYTE     byte;

    IONCHECK(_ion_int_validate_arg_with_ptr(iint, buf));
    if (limit < 0) {
        FAILWITH(IERR_INVALID_ARG);
    }

    may_overflow = FALSE;
    byte = buf[0] & II_BYTE_MASK;
    if ((is_neg = ((byte & II_BYTE_SIGN_BIT) != 0)) == TRUE) {
        if (byte >= II_BYTE_NEG_OVERFLOW_LIMIT) {
            // where the most significant bit in the negative
            // value is on (or will be when the value is
            // inverted to convert from 2's complement to
            // signed magnitude representation) we may
            // room for an extra bit when we add 1 at the
            // very end, after copying the inverted buf
            may_overflow = TRUE;
        }
        // find first non-(0xff) byte (i.e. where are the bits that we want)
        // the least sig byte is meaningful in all cases (thus limit-1)
        for (byte_idx = 0; byte_idx < limit-1; byte_idx++) {
            if ((buf[byte_idx] &(0xff)) != (0xff)) break;
        }
    }
    else {
        // find first non-zero byte (i.e. where are the bits that we want)
        // the limit-1 case is handled by the 0 count below
        for (byte_idx = 0; byte_idx < limit; byte_idx++) {
            if (buf[byte_idx]) break;
        }
    }
  
    // check for zero
    byte_count = limit - byte_idx;
    if (byte_count == 0) {
        IONCHECK(_ion_int_zero(iint));
        SUCCEED();
    }

    // make sure we have enough space in the iint
    bits = 7 + (byte_count - 1) * 8;
    if (byte_idx > 0) {
        // we skipped a leading sign byte in tso the first byte
        // we process we want all 8 bits, not 7
        bits++;
    }
    if (may_overflow) {
        // if we're close enough adding 1 (which we do below
        // to convert from 2's complement to sign + absolute value)
        // that we may overflow the top bit we need to allow room
        // to do that
        bits++;
    }
    ii_length = (SIZE)(((bits - 1) / II_BITS_PER_II_DIGIT)+1);
    IONCHECK(_ion_int_extend_digits(iint, ii_length, TRUE));

    is_zero = _ion_int_from_bytes_helper(iint, buf, byte_idx, limit, is_neg, (byte_idx == 0));

    // check negative first because -1 looks like 0 to the helper
    if (is_neg) {
        // finish the 2's complement arithmetic
        IONCHECK(_ion_int_add_digit(iint->_digits, iint->_len, 1));
        iint->_signum = -1;
    }
    else if (is_zero) {
        iint->_signum = 0;
    }
    else {
        iint->_signum = 1;
    }

    iRETURN;
}


iERR ion_int_from_abs_bytes(ION_INT *iint, BYTE *buf, SIZE limit, BOOL is_negative)
{
    iENTER;
    BOOL     is_zero, may_overflow;
    SIZE   byte_idx, byte_count, bits, ii_length;
    BYTE     byte;

    IONCHECK(_ion_int_validate_arg_with_ptr(iint, buf));
    if (limit < 0) {
        FAILWITH(IERR_INVALID_ARG);
    }

    may_overflow = FALSE;
    byte = buf[0] & II_BYTE_MASK;
    
    // find first non-zero byte (i.e. where are the bits that we want)
    for (byte_idx = 0; byte_idx < limit; byte_idx++) {
        if (buf[byte_idx]) break;
    }
  
    // check for zero
    byte_count = limit - byte_idx;
    if (byte_count == 0) {
        IONCHECK(_ion_int_zero(iint));
        SUCCEED();
    }

    // make sure we have enough space in the iint
    bits = byte_count * 8;
    ii_length = (SIZE)(((bits - 1) / II_BITS_PER_II_DIGIT)+1);
    IONCHECK(_ion_int_extend_digits(iint, ii_length, TRUE));
    
    is_zero = _ion_int_from_bytes_helper(iint, buf, byte_idx, limit, FALSE, FALSE);

    if (is_zero) {
        iint->_signum = 0;
    }
    else  {
        iint->_signum = is_negative ? -1 : 1;
    }

    iRETURN;
}


iERR ion_int_from_long(ION_INT *iint, int64_t value)
{
    iENTER;
    SIZE  ii_length, digit_idx;
    int64_t temp;
    BOOL    is_neg;

    IONCHECK(_ion_int_validate_arg(iint));
    
    if (!value) {
        IONCHECK(_ion_int_zero(iint));
        SUCCEED();
    }

    if ((is_neg = (value < 0)) == TRUE) {
        value = -value;
    }

    ii_length = 0; 
    temp = value;
    while (temp) {
        temp >>= II_SHIFT;
        ii_length++;
    }

    IONCHECK(_ion_int_extend_digits(iint, ii_length, TRUE));

    for (digit_idx = iint->_len-1; value; digit_idx--) {
        iint->_digits[digit_idx] = (II_DIGIT)(value & II_MASK);
        value >>= II_SHIFT;
    }

    iint->_signum = is_neg ? -1 : 1;

    iRETURN;
}
  
  
iERR ion_int_from_decimal(ION_INT *iint, const decQuad *p_value)
{
    iENTER;
    BOOL     is_neg;
    SIZE   digit_idx, decimal_digits, bits, ii_length;
    II_DIGIT digit;
    decQuad  temp1, temp2;
    int32_t  is_zero;
  
  
    IONCHECK(_ion_int_validate_arg_with_ptr(iint, p_value));
    if (!decQuadIsFinite(p_value)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    if (!decQuadIsInteger(p_value)) {
        FAILWITH(IERR_INVALID_ARG);
    }

    // special case since zero is so common (and quick to test and set)
    if (decQuadIsZero(p_value)) {
        IONCHECK(_ion_int_zero(iint));
        SUCCEED();
    }

    is_neg = decQuadIsSigned(p_value);
    decQuadCopyAbs(&temp1, p_value);

    decimal_digits = decQuadDigits(&temp1);
    bits = (SIZE)(II_BITS_PER_DEC_DIGIT * decimal_digits) + 1;
    ii_length = (SIZE)((bits - 1) / II_BITS_PER_II_DIGIT) + 1;
    IONCHECK(_ion_int_extend_digits(iint, ii_length, TRUE));

_ion_int_dump_quad(&temp1, (int64_t)0);

    for (digit_idx = iint->_len-1; ; digit_idx--) 
    {
_ion_int_dump_quad(&temp1, (int64_t)1);
        is_zero = decQuadIsZero(&temp1);
        if (is_zero) break; // so I can see this in a debugger

//        decQuadAnd(&temp2, &temp1, &g_decQuad_Mask, &g_Context);
        decQuadRemainder(&temp2, &temp1, &g_digit_base, &g_Context);
_ion_int_dump_quad(&temp2, (int64_t)2);
        digit = decQuadToUInt32(&temp2, &g_Context, DEC_ROUND_DOWN);
        iint->_digits[digit_idx] = digit;
//        decQuadShift(&temp1, &temp1, &g_decQuad_Shift, &g_Context);
        decQuadDivideInteger(&temp1, &temp1, &g_digit_base, &g_Context);
    }

    // we don't have to zero the digits because we did during allocation

    iint->_signum = is_neg ? -1 : 1;

    iRETURN;
}


iERR ion_int_char_length(ION_INT *iint, SIZE *p_len)
{
    iENTER;
    SIZE len;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_len));

    len = _ion_int_get_char_len_helper(iint);
    *p_len = len;

    iRETURN;
}


iERR ion_int_to_char(ION_INT *iint, BYTE *p_str, SIZE len, SIZE *p_written)
{
    iENTER;
    SIZE decimal_digits;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_str));

    // calculate how many character we'll need from the number of bits needed
    decimal_digits = _ion_int_get_char_len_helper(iint);
    if (decimal_digits > len) {
        FAILWITH(IERR_BUFFER_TOO_SMALL);
    }

    IONCHECK(_ion_int_to_string_helper(iint, (char *)p_str, len, p_written));
    if (*p_written < len) {
        p_str[*p_written] = '\0';
    }
    
    iRETURN;
}


iERR ion_int_to_string(ION_INT *iint, hOWNER owner, iSTRING p_str)
{
    iENTER;
    SIZE decimal_digits;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_str));

    // calculate how many character we'll need from the number of bits needed
    decimal_digits = _ion_int_get_char_len_helper(iint) + 1;
    
    p_str->value = (BYTE *)_ion_int_realloc_helper(p_str->value, p_str->length, owner, decimal_digits);
    p_str->length = decimal_digits;
    if (NULL == p_str->value) FAILWITH(IERR_NO_MEMORY);
    
    IONCHECK(_ion_int_to_string_helper(iint, (char *)p_str->value, decimal_digits, (SIZE *)&(p_str->length)));
    if (p_str->length < decimal_digits) {
        p_str->value[p_str->length] = '\0';
    }

    iRETURN;
}


iERR ion_int_byte_length(ION_INT *iint, SIZE *p_byte_length)
{
    iENTER;
    SIZE len;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_byte_length));

    len = _ion_int_bytes_length_helper(iint);
    
    *p_byte_length = len;
    SUCCEED();
    
    iRETURN;
}


iERR ion_int_to_bytes(ION_INT *iint, SIZE starting_int_byte_offset 
                     ,BYTE *buffer, SIZE buffer_length 
                     ,SIZE *bytes_written
) {
    iENTER;
    BOOL     is_neg, sign_byte_needed = FALSE;
    ION_INT  neg, *tocopy;
    SIZE   bytes;
    SIZE   highbit, len;
    SIZE   written = 0;
    int      value8;
    
    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, buffer));
    if (starting_int_byte_offset < 0) {
        FAILWITH(IERR_INVALID_ARG);
    }
    
    if (_ion_int_is_zero(iint)) {
        if (starting_int_byte_offset == 0 && buffer_length > 0) {
            buffer[0] = 0;
            written++;
        }
        if (bytes_written) *bytes_written = written;
        SUCCEED();
    }
    else if (iint->_signum < 0) {
        // so for 2's complement we subtract 1 (that will happen below)
        // this means we need to make a copy of the bits since we don't
        // want to change the callers copy of the value
        _ion_int_init(&neg, NULL);
        highbit = _ion_int_highest_bit_set_helper(iint);
        len = highbit ? (((highbit - 1) / II_BITS_PER_II_DIGIT) + 1) : 1;
        IONCHECK(_ion_int_extend_digits(&neg, len, TRUE));
        memcpy(neg._digits, &iint->_digits[iint->_len - len], len * sizeof(II_DIGIT));
        IONCHECK(_ion_int_sub_digit(neg._digits, neg._len, 1));
        is_neg = TRUE;
        tocopy = &neg;
    }
    else {
        tocopy = iint;
        is_neg = FALSE;
  }
    
    bytes = _ion_int_abs_bytes_length_helper(tocopy);
    if (_ion_int_is_high_bytes_high_bit_set_helper(tocopy, bytes)) {
        if (starting_int_byte_offset == 0) {
            // here we write the "extra" initial byte which will only have
            // the sign bit (so it's either all 1's or all 0's)
            value8 = (is_neg) ? -1 : 0;
            buffer[0] = (BYTE)(value8 & II_BYTE_MASK);
            sign_byte_needed = TRUE;
        }
        else {
          // since the high byte isn't really there we just offset
          // our starting offset by 1 so that from this point on
          // starting_int_byte_offset is in our real bits and does
          // not count the sign byte.
          starting_int_byte_offset--;
        }
    }
    IONCHECK(_ion_int_to_bytes_helper(tocopy, bytes, starting_int_byte_offset, is_neg
                                    , &buffer[sign_byte_needed ? 1 : 0], buffer_length
                                    , &written)
    );
    
    if (bytes_written) {
    if (sign_byte_needed) written++;
        *bytes_written = written;
    }
    
    SUCCEED();

    iRETURN;
}


iERR ion_int_abs_bytes_length(ION_INT *iint, SIZE *p_byte_length)
{
    iENTER;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_byte_length));
    
    *p_byte_length = _ion_int_abs_bytes_length_helper(iint);
    SUCCEED();
    
    iRETURN;
}


iERR ion_int_to_abs_bytes(ION_INT *iint, SIZE starting_int_byte_offset
                        , BYTE *buffer, SIZE buffer_length
                        , SIZE *bytes_written
) {
    iENTER;
    SIZE bytes, written;
    
    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, buffer));
    if (starting_int_byte_offset < 0) {
        FAILWITH(IERR_INVALID_ARG);
    }

    if (_ion_int_is_zero(iint)) {
        if (starting_int_byte_offset == 0 && buffer_length > 0) {
            buffer[0] = 0;
            written = 1;
        }
        else {
            written = 0;
        }
        if (bytes_written) *bytes_written = written;
        SUCCEED();
    }
    
    bytes = _ion_int_abs_bytes_length_helper(iint);
    IONCHECK(_ion_int_to_bytes_helper(iint, bytes, starting_int_byte_offset, FALSE
                                    , buffer, buffer_length
                                    , bytes_written)
    );
    
    SUCCEED();

    iRETURN;
}


iERR ion_int_to_int64(ION_INT *iint, int64_t *p_int64)
{
    iENTER;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_int64));

    IONCHECK(_ion_int_to_int64_helper(iint, p_int64));
    SUCCEED();

    iRETURN;
}


iERR ion_int_to_int32(ION_INT *iint, int32_t *p_int32)
{
    iENTER;
    int64_t i64;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_int32));

    IONCHECK(_ion_int_to_int64_helper(iint, &i64));
    if (i64 < INT32_MIN || i64 > INT32_MAX) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    *p_int32 = (int32_t)i64;
    SUCCEED();

    iRETURN;
}


iERR ion_int_to_decimal(ION_INT *iint, decQuad *p_quad)
{
    iENTER;
    II_DIGIT *digits, *end, digit;
    decQuad   quad_digit;

    IONCHECK(_ion_int_validate_non_null_arg_with_ptr(iint, p_quad));

    decQuadZero(p_quad);
    digits = iint->_digits;
    end    = digits + iint->_len;

    while (digits < end) {
        digit = *digits++;
        decQuadFromInt32(&quad_digit, (int32_t)digit);
        decQuadMultiply(p_quad, &g_digit_base, p_quad, &g_Context);
        decQuadAdd(p_quad, &quad_digit, p_quad, &g_Context);
    }

    if (iint->_signum == -1) {
        decQuadMinus(p_quad, p_quad, &g_Context);
    }
    SUCCEED();

    iRETURN;
}


//////////////////////////////////////////////////////////////////
//
//  these are internally used helper functions
//  they do not check for valid arguments (other than
//  some ASSERT's)
//
///////////////////////////////////////////////////////////////////

void _ion_int_dump_quad(decQuad *quad, int64_t expected)
{
    char temp[100];
if (quad) return;
    decQuadToEngString(quad, temp);
    fprintf(stderr, "dump of %ld ", expected);
    fprintf(stderr, " as quad: %s\n", temp);
    return;
}

int _int_int_init_globals()
{
    decQuad two;
    int32_t temp;

    // invariant needed for add
    ASSERT(((II_LONG_DIGIT)UINT32_MAX) >= (((II_LONG_DIGIT)II_MAX_DIGIT)*2));
    // invariant needed for multiply
    ASSERT((UINT64_MAX) >= ( (((II_LONG_DIGIT)II_MAX_DIGIT) * ((II_LONG_DIGIT)II_MAX_DIGIT)) + ((II_LONG_DIGIT)II_MAX_DIGIT) )); 
    // invariant needed for multiply and add
    ASSERT((UINT64_MAX) >= ( (((II_LONG_DIGIT)II_MAX_DIGIT) * ((II_LONG_DIGIT)II_MAX_DIGIT)) + (((II_LONG_DIGIT)II_MAX_DIGIT)*2) )); 

    decContextDefault(&g_Context, DEC_INIT_DECQUAD);

    decQuadFromInt32(&two, 2);
    temp = (int32_t)(II_BASE / 2);
    decQuadFromInt32(&g_digit_base, temp);
    decQuadMultiply(&g_digit_base, &g_digit_base, &two, &g_Context);

    decQuadFromInt32(&g_decQuad_Mask, (int32_t)II_MASK);
    decQuadFromInt32(&g_decQuad_Shift, (int32_t)II_SHIFT);

#if 1
    _ion_int_dump_quad(&g_digit_base, (int64_t)II_BASE);
    _ion_int_dump_quad(&g_decQuad_Mask, (int64_t)II_MASK);
    _ion_int_dump_quad(&g_decQuad_Shift, (int64_t)II_SHIFT);
#endif

    return 0;
}


iERR _ion_int_validate_arg(const ION_INT *iint)
{
    iENTER;
    if (!iint) {
        FAILWITH(IERR_INVALID_ARG);
    }
    SUCCEED();
    iRETURN;
}


iERR _ion_int_validate_arg_with_ptr(const ION_INT *iint, const void *ptr)
{
    iENTER;
    if (!iint) {
        FAILWITH(IERR_INVALID_ARG);
    }
    if (!ptr) {
        FAILWITH(IERR_INVALID_ARG);
    }
    // this switch is intended to catch uninitialized iint's
    switch(iint->_signum) {
    case 1: case -1: case 0: 
      break;
    default:
      FAILWITH(IERR_INVALID_ARG);
    }
    SUCCEED();
    iRETURN;
}

iERR _ion_int_validate_non_null_arg_with_ptr(const ION_INT *iint, const void *ptr)
{
    iENTER;
    if (!iint || _ion_int_is_null_helper(iint)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    if (!ptr) {
        FAILWITH(IERR_INVALID_ARG);
    }
    SUCCEED();
    iRETURN;
}

void _ion_int_init(ION_INT *iint, void *owner)
{
    ASSERT(iint);
    iint->_owner  = owner;
    iint->_signum = 0;
    iint->_len    = 0;
    iint->_digits = NULL;
    return;
}


iERR _ion_int_zero(ION_INT *iint)
{
    iENTER;
    ASSERT(iint);
    IONCHECK(_ion_int_extend_digits(iint, 1, TRUE));
    iint->_signum = 0;
    SUCCEED();
    iRETURN;
}


void * _ion_int_realloc_helper(void *value, SIZE old_len, void *owner, SIZE new_len)
{
    if (old_len < new_len) {
        if (!owner) {
            if (value) ion_xfree(value);
            value = ion_xalloc(new_len);
        }
        else {
            value = ion_alloc_with_owner(owner, new_len);
        }
    }
    return value;
}


iERR _ion_int_extend_digits(ION_INT *iint, SIZE digits_needed, BOOL zero_fill)
{
    iENTER;
    SIZE  len;
    void   *temp;

    ASSERT(iint);

    digits_needed += 100;
    if (iint->_len < digits_needed) {
        // realloc
        len = digits_needed * sizeof(II_DIGIT);
        temp = _ion_int_realloc_helper(iint->_digits, iint->_len*sizeof(II_DIGIT), iint->_owner, len);
        if (!temp) FAILWITH(IERR_NO_MEMORY);
        iint->_digits = (II_DIGIT *)temp;
        iint->_len = digits_needed;
    }
    else {
        ASSERT(iint->_digits);
    }
    if (zero_fill) {
        // zero fill the digits
        ASSERT( iint && iint->_digits && (iint->_len > 0) );
        len = sizeof(II_DIGIT) * iint->_len;
        memset(iint->_digits, 0, len);
    }

    iRETURN;
}


II_DIGIT *_ion_int_buffer_temp_copy( II_DIGIT *orig_digits, SIZE len, II_DIGIT *cache_buffer, SIZE cache_len)
{
    ASSERT(orig_digits);

    if (cache_len < len || cache_buffer == NULL) {
        cache_buffer = ion_xalloc(len * sizeof(II_DIGIT));
    }
    if (cache_buffer) {
        memcpy(cache_buffer, orig_digits, len * sizeof(II_DIGIT));
    }
    return cache_buffer;
}


void _ion_int_free_temp(II_DIGIT *temp_buffer, II_DIGIT *cache_buffer)
{
    if (temp_buffer && temp_buffer != cache_buffer) {
        ion_xfree(temp_buffer);
    }
}


BOOL _ion_int_is_null_helper(const ION_INT *iint)
{
    BOOL is_null;
    is_null = (iint->_digits == NULL);
    return is_null;
}


BOOL _ion_int_is_zero(const ION_INT *iint)
{
    ASSERT(iint);
    if (iint->_signum == 0) return TRUE;
    return FALSE;
}


BOOL _ion_int_is_zero_bytes(const II_DIGIT *digits, SIZE len)
{
    SIZE ii;
    ASSERT(digits);
    for (ii=0; ii<len; ii++) {
        if (digits[ii] != 0) return FALSE;
    }
    return TRUE;
}


SIZE _ion_int_highest_bit_set_helper(const ION_INT *iint)
{
    iENTER;
    II_DIGIT *digits, msd;
    SIZE    ii, len, bits = 0;

    len = iint->_len;
    if (len < 1) return 0;
    ASSERT(iint->_digits);

    digits = iint->_digits;
    for (ii=0; ii<len; ii++) {
       if ((msd = digits[ii]) != 0) break;
    }
    // if there are any bits set
    if (ii<len) {
        // first compute how many whole digits there are
        bits = (len - ii - 1) * II_BITS_PER_II_DIGIT;  // the extra -1 since we'll count the bits in the most significan digit below
        // now we see how many are actually set in the
        // most significat digit (which we broke on above)
        while (msd) { // as long as any bit is set, shift it over and count 1 more
            msd >>= 1;
            bits++;
        }
    }
    else {
        // no bits set?  That's a zero for you.
        // so we don't touch 'bits' from it's initial value of 0
    }
    return bits;
}


BOOL _ion_int_from_bytes_helper(ION_INT *iint, BYTE *buf, SIZE byte_idx, SIZE limit, BOOL invert, BOOL includes_sign_byte)
{
    BOOL     is_zero;
    int      digit_idx;
    BYTE     byte, *byte_ptr, *byte_ptr_limit;
    II_DIGIT digit;
    int      to_copy, waiting_bits, available_space;

    ASSERT(iint);
    ASSERT(buf);
    ASSERT(limit >= 0);

    // we'll flip this off if we see any non-zero byte go by
    is_zero = TRUE;

    // set up the byte iteration and load the first byte
    // we'll be copying from the right most bit to the left
    // or least significant bit to the most significant
    byte_ptr = &buf[limit-1];
    byte_ptr_limit = &buf[byte_idx];
    byte = *byte_ptr;
    if (invert) {
        // undo the "complement" part of two's complement
        byte = ~byte;
    }
    byte &= II_BYTE_MASK;
    waiting_bits = II_BITS_PER_BYTE;
    if ((byte_ptr == byte_ptr_limit) && includes_sign_byte) {
        // strip of the sign bit (if we didn't skip it already)
        // and note we only have 7 bits
        waiting_bits--;
    }
  
    // setup out digit iteration
    digit_idx = iint->_len - 1;
    digit = 0;
    available_space = II_BITS_PER_II_DIGIT;
  
    // now we copy bits until we have used all the bits in the input
    for (;;) {
        // copy the waiting bits
        to_copy = (available_space < waiting_bits) ? available_space : waiting_bits;
        // trim the bits off the byte that we're going to copy later
        // then move them over so that they're in the right spot in the digit
        digit |= ( (byte & (II_BYTE_MASK >> (II_BITS_PER_BYTE - to_copy))) 
                  << (II_BITS_PER_II_DIGIT - available_space) 
                );
        byte >>= to_copy;              // throw away the bits we've just copied
        available_space -= to_copy;
        waiting_bits -= to_copy;

        // if digit full
        if (available_space < 1) {
            // write digit
            if (digit) is_zero = FALSE;
            ASSERT( digit_idx >= 0 );
            iint->_digits[digit_idx] = digit;
            digit_idx--;
            digit = 0;
            available_space = II_BITS_PER_II_DIGIT;
        }

        // if all copied
        if (waiting_bits < 1) {
            // if no more bits - done
            byte_ptr--;
            if (byte_ptr < byte_ptr_limit) {
                break;
            }
            // get more bits - remember to invert the negative
            // to undo the "complement" part of two's complement
            byte = *byte_ptr;
            if (invert) {
                byte = ~byte;
            }
            byte &= II_BYTE_MASK;

            // while we generally have 8 bits per byte the most significant
            // byte has the sign, which isn't copied
            waiting_bits = II_BITS_PER_BYTE;
            if (byte_ptr == byte_ptr_limit && includes_sign_byte) {
                waiting_bits--;
            }
        }
    }

    // if we have a partially filled digit we need to write it now
    if (available_space < II_BITS_PER_II_DIGIT) {
        // write digit
        if (digit) is_zero = FALSE;
        ASSERT( digit_idx >= 0 );
        iint->_digits[digit_idx] = digit;
        digit_idx--;
    }

    // zero out any leading digits we didn't happen to fill
    while (digit_idx >= 0) {
        iint->_digits[digit_idx--] = 0;
    }

    return is_zero;
}


SIZE _ion_int_get_char_len_helper(const ION_INT *iint)
{
    SIZE bits, decimal_digits;
    
    ASSERT(iint);
    // calculate how many character we'll need from the number of bits needed
    bits = _ion_int_highest_bit_set_helper(iint);
    decimal_digits = DECIMAL_DIGIT_COUNT_FROM_BITS(bits);
    if (iint->_signum < 0) {
        decimal_digits++; // room for minus sign
    }
    // no: decimal_digits++; // room for (uncounted) null terminator

    return decimal_digits;
}


iERR _ion_int_to_string_helper(ION_INT *iint, char *strbuf, SIZE buflen, SIZE *p_written) 
{
    iENTER;
    II_DIGIT  small_copy[II_SMALL_DIGIT_ARRAY_LENGTH];
    II_DIGIT *digits = NULL, remainder;
    SIZE      decimal_digits, len;
    char      c, *cp, *end, *head, *tail;

    ASSERT(iint && !_ion_int_is_null_helper(iint));
    ASSERT(strbuf);
    
    decimal_digits = _ion_int_get_char_len_helper(iint);
    ASSERT(buflen >= decimal_digits);

    len = iint->_len;
    digits = _ion_int_buffer_temp_copy( iint->_digits, len, small_copy, II_SMALL_DIGIT_ARRAY_LENGTH );
    if (digits == NULL) {
        FAILWITH(IERR_NO_MEMORY);
    }

    // calculate the digits from least to most significant
    for (cp = strbuf, end = cp + buflen; cp < end; cp++) {
        if (_ion_int_is_zero_bytes(digits, len)) break;
        IONCHECK(_ion_int_divide_by_digit(digits, len, II_STRING_BASE, &remainder));
        ASSERT(remainder >= 0 && remainder <= 9);
        *cp = (BYTE)((remainder & 0xff)+'0');
    }
    ASSERT((end <= (cp + buflen)) && _ion_int_is_zero_bytes(digits, len));
    
    // zero is an edge case, the loop above never sets a character
    // as it jumps out at the beginning of the first iteration
    if (cp == strbuf) {
        *cp++ = '0';
    }
    
    if (iint->_signum < 0) {
        *cp++ = '-';
    }
    *cp = 0;
    
    // now we know the real length (the estimate from the
    // allocation can be off by 1
    decimal_digits = (SIZE)(cp - strbuf); // limited by buflen
    
    // now we reverse the characters (bytes) in the string
    // to go from high to low
    for (head = strbuf, tail = cp-1; head < tail; head++, tail--) {
        c = *head;
        *head = *tail;
        *tail = c;
    }
    
    // and set the callers string to the freshly minted string
    if (p_written) {
        *p_written = decimal_digits;
    }
    SUCCEED();

fail:
    _ion_int_free_temp(digits, small_copy);
    RETURN(__file__, __line__, __count__, err);
}


BOOL _ion_int_is_high_bytes_high_bit_set_helper(const ION_INT *iint, SIZE abs_byte_count)
{
    SIZE   highbit, digitidx, bitidx;
    II_DIGIT digit;
    int      highbitvalue;

    ASSERT(iint);
    ASSERT(!_ion_int_is_null_helper(iint));

    // extract the high order bit or the high order byte to
    // see if it is set of not (if it is we'll need an extra
    // byte for the signed representation)
    highbit = abs_byte_count * 8;

    // if the highbit is reified in our digits we need to
    // actually look at it, in some cases the highbit(s)
    // will be off the end of our digit bits (off the left,
    // or most sigificant bit, side) and therefore 0.
    if (highbit < (iint->_len * (SIZE)II_BITS_PER_II_DIGIT)) {
        digitidx = iint->_len - (((highbit - 1) / II_BITS_PER_II_DIGIT) + 1); // here digitidx 1 is low order digit
        digit = iint->_digits[digitidx]; // array element 0 is high order digit, so invert
        bitidx = (highbit % II_BITS_PER_II_DIGIT);
        if (bitidx == 0) bitidx = II_BITS_PER_II_DIGIT;
        highbitvalue = (digit >> (bitidx - 1)) & 1;
        if (highbitvalue) {
            // if the high bit is set, we need an extra byte
            // for serialization since the high bit is, in
            // practice, the sign bit
            return TRUE;
        }
    }
    return FALSE;
}


SIZE _ion_int_bytes_length_helper(const ION_INT *iint)
{
    SIZE bits, bytes;

    ASSERT(iint);
    ASSERT(!_ion_int_is_null_helper(iint));

    bits = _ion_int_highest_bit_set_helper(iint);
    if (bits == 0) {
        // zero is a special case - (bits-1) will wrap, and
        // for zero we write 1 byte of zeros (even though
        // a length of zero would imply this).
        bytes = 1;
    }
    else {
        bytes = ((bits - 1) / 8) + 1;
        if (bits % 8 == 0) {
            // if the topmost bit is used we need an extra byte
            // to make room for the sign bit
            bytes++;
        }
    }
    return bytes;
}


iERR _ion_int_to_bytes_helper(ION_INT *iint
                            , SIZE bytes_in_int
                            , SIZE starting_int_byte_offset
                            , BOOL is_neg
                            , BYTE *buffer
                            , SIZE buffer_length
                            , SIZE *bytes_written
)
{
    iENTER;
    SIZE tocopy, available32, needed8, available_bits;
    SIZE written = 0;
    int    idx32, count32, value32, value8;

    // see how many bits there are to copy
    // note: we always copy whole bytes worth
    count32 = (int)iint->_len;
    ASSERT(count32 >= 0);
    if (starting_int_byte_offset >= bytes_in_int) {
        SUCCEED();
    }
    
    // if there are any bytes left to copy
    tocopy = ((bytes_in_int - starting_int_byte_offset) * II_BITS_PER_BYTE);
       
    // always loaded from high to low
    available32 = (tocopy % 31);          // number of bits in the first digit we'll copy (may be outside the digit array)
    needed8 = II_BITS_PER_BYTE;           // we always fill whole bytes
        
    // array index of first digit we'll be copying from
    // note that this may be negative in cases such as
    // copying 31 bits which requires 4 bytes (or 32 bits)
    idx32 = ( count32 - (((tocopy - 1) / II_BITS_PER_II_DIGIT)) ) - 1;
    ASSERT((idx32 >= -1) && (idx32 < count32));  // if bytes == 4, idx32 is -1

    // initialize the from and to "buffers", if we don't have enough
    // elements in the digit array the available bits are all zeros
    value32 = (idx32 >= 0) ? iint->_digits[idx32] : 0;
    value8 = 0;

    while (written < buffer_length) {
        tocopy = (needed8 > available32) ? available32 : needed8;
        available_bits  = (value32 >> (available32 - tocopy));  // puts the least significant source bit at pos 0
        available_bits &= (II_BYTE_MASK >> (II_BITS_PER_BYTE - tocopy)); // drops high order non-copied bits
        value8 |= available_bits << (needed8 - tocopy); // shifts bits to copy to the right spot
        needed8 -= tocopy;
        available32 -= tocopy;
        if (!needed8) {
            if (is_neg) value8 = ~value8;
            buffer[written++] = (BYTE)(value8 & II_BYTE_MASK);
            if (written >= buffer_length) break;                
            value8 = 0;
            needed8 = II_BITS_PER_BYTE;
        }
        if (!available32) {
            idx32++;
            if (idx32 >= count32) break;
            value32 = iint->_digits[idx32];
            available32 = II_BITS_PER_II_DIGIT;
        }
    }
    if (bytes_written) {
        *bytes_written = written;
    }
    SUCCEED();
    
    iRETURN;
}


SIZE _ion_int_abs_bytes_length_helper(const ION_INT *iint)
{
    SIZE bits, bytes;

    ASSERT(iint);
    ASSERT(!_ion_int_is_null_helper(iint));

    bits = _ion_int_highest_bit_set_helper(iint);
    if (bits == 0) {
        // special case since bits-1 (below) would wrap
        // and we'll write 1 byte anyway
        bytes = 1;
    }
    else {
        bytes = ((bits - 1) / 8) + 1;
    }
    return bytes;
}


iERR _ion_int_to_int64_helper(ION_INT *iint, int64_t *p_int64)
{
    iENTER;
    II_DIGIT *digits, *end, digit;
    int64_t   value = 0;

    digits = iint->_digits;
    end    = digits + iint->_len;
    while (digits < end) {
        digit = *digits++;
        value <<= II_SHIFT;
        value += digit;
        if (value < 0) {
             FAILWITH(IERR_NUMERIC_OVERFLOW);
        }
    }

    if (iint->_signum == -1) {
        value = -value;
    }
    *p_int64 = value;
    SUCCEED();

    iRETURN;
}


iERR _ion_int_add_digit(II_DIGIT *digits
                      , SIZE   digit_count
                      , II_DIGIT value
) {
    iENTER;
    II_DIGIT      digit;
    II_LONG_DIGIT temp, lvalue = (II_LONG_DIGIT)value;
    int           ii;

    ASSERT( digits );
    ASSERT( (value < II_BASE) && (value >= 0) );

    // add until there's nothing left to carry or no place to put it
    for (ii=digit_count; ii>0 && (lvalue != 0); ) {
        ii--;
        digit = digits[ii];
        temp = ((II_LONG_DIGIT)digit) + lvalue; // size of bin and dec digits must allow this to never overflow
        digits[ii] = (II_DIGIT)(temp & II_MASK);
        lvalue = (temp >> II_SHIFT);
    }
    ASSERT((lvalue == 0) && "this add doesn't support increasing the number of digits");

    SUCCEED();
    iRETURN;
}


iERR _ion_int_sub_digit(II_DIGIT *digits
                      , SIZE   digit_count
                      , II_DIGIT value
) {
    iENTER;
    II_DIGIT      digit;
    II_LONG_DIGIT temp, lvalue = (II_LONG_DIGIT)value;
    int           ii;
    BOOL          carry = FALSE;

    ASSERT( digits );
    ASSERT( (value < II_BASE) && (value >= 0) );

    // add until there is no value left to subtract
    for (ii=digit_count; ii>0 && (lvalue != 0); ) {
        ii--;
        digit = digits[ii];
        temp = ((II_LONG_DIGIT)digit) - lvalue; // size of bin and dec digits must allow this to never overflow
        if (temp < 0) {
            temp += II_MAX_DIGIT;
            lvalue = 1;
        }
        else {
            lvalue = 0;
        }
        digits[ii] = (II_DIGIT)(temp & II_MASK);
    }
    ASSERT((lvalue == 0) && "this add doesn't support increasing the number of digits");

    SUCCEED();
    iRETURN;
}


/* - not used ??
iERR _ion_int_multiply_by_digit(II_DIGIT *digits
                              , SIZE    digit_count
                              , II_DIGIT  value
) {
    iENTER;
    II_DIGIT      digit;
    II_LONG_DIGIT temp, carry = 0;
    int           ii;

    ASSERT( digits );
    ASSERT( (value < BYTE_BASE) && (value >= 0) );

    for (ii=digit_count; ii>0; ) {
        ii--;
        digit = digits[ii];
        temp = (((II_LONG_DIGIT)digit) * value) + carry;
        digits[ii]= (II_DIGIT)(temp & II_MASK);
        carry = temp >> II_SHIFT;
    }
    ASSERT((carry == 0) && "this mult doesn't support increasing the number of digits" == FALSE);

    SUCCEED();
    iRETURN;
}
*/

iERR _ion_int_multiply_and_add(II_DIGIT *digits
                              , SIZE   digit_count
                              , II_DIGIT mult_value
                              , II_DIGIT add_value
) {
    iENTER;
    II_DIGIT      digit;
    II_LONG_DIGIT temp, carry = add_value;
    int           ii;

    ASSERT( digits );
    ASSERT( (mult_value < II_BASE) && (mult_value >= 0) );
    ASSERT( (add_value < II_BASE) && (add_value >= 0) );

    for (ii=digit_count; ii>0; ) {
        ii--;
        digit = digits[ii];
        temp = (((II_LONG_DIGIT)digit) * mult_value) + carry;
        digits[ii]= (II_DIGIT)(temp & II_MASK);
        carry = temp >> II_SHIFT;
    }
    ASSERT((carry == 0) && "this mult_add doesn't support increasing the number of digits");

    SUCCEED();
    iRETURN;
}


iERR _ion_int_divide_by_digit(II_DIGIT *digits
                            , SIZE      digit_count
                            , II_DIGIT  value
                            , II_DIGIT *p_remainder
) {
    iENTER;
    II_DIGIT      digit;
    II_LONG_DIGIT temp, new_digit, remainder = 0, lvalue = value;
    SIZE        ii;

    ASSERT( digits );
    ASSERT( (value < II_BASE) && (value > 0) );

    for (ii=0; ii<digit_count; ii++) {
        digit = digits[ii];
        temp = ((II_LONG_DIGIT)digit) | (remainder << II_SHIFT);
        new_digit = ( temp / lvalue );
        ASSERT( (new_digit & ~II_MASK) == 0 );  // this *should* not ever overflow
        digits[ii] = (II_DIGIT)new_digit;
        remainder = temp - (new_digit * lvalue);
        ASSERT( remainder < II_BASE );
    }
    ASSERT((remainder < II_BASE) && (remainder >= 0));

    *p_remainder = (II_DIGIT)remainder;
    SUCCEED();

    iRETURN;
}
