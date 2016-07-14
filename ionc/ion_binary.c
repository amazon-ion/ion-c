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

#include <limits.h>
#include "ion_internal.h"

// These field formats are always used in some context that clearly indicates the number of octets in the field.
int ion_binary_len_uint_64(uint64_t value) {
    int len = 0;
    while (value > 0) {
        len++;
        value >>= 8;
    };
    return len;
}

// These field formats are always used in some context that clearly indicates the number of octets in the field.
int ion_binary_len_int_64(int64_t value) {
    int top_byte, len = 0;
    if (value != 0) {
        uint64_t unsignedValue = abs_int64(value);
        len = ion_binary_len_uint_64(unsignedValue);
        top_byte = (int)(unsignedValue >> ((len - 1) * 8));
        if ((top_byte & 0x80) != 0) {
            // if the high order bit of the value being written
            // is set, we need an extra byte to hold the sign bit
            len++;
        }
    }
    return len;
}

int ion_binary_len_var_uint_64(uint64_t value) {
    int len = 0;
    do {
        len++;
        value >>=7;
    } while (value > 0);
    return len;
}

// for the signed 7 bit variable length format
// the high order bit is reserved for the stop
// bit.  And the first bit used is reserved for
// the sign bit.  So we get 6 "real" bits in the
// first byte and 7 bits per byte thereafter.
int ion_binary_len_var_int_64( int64_t value )
{
    BOOL msbIsOne = FALSE;  // true if the last byte's 7th bit is 1. required for proper serialization of signed varint
    int  len = 0;

    value = abs_int64(value);

    // we'll write 7 bits at a time 
    // note that 0 goes through once
    do {
        len++;
        msbIsOne = value & 0x40;
        value >>= 7;
    } while ( value );

    if (msbIsOne) {
      len++;    // if the 7th bit was set we need an extra byte to hold the sign all by itself
    }

    return len;
}

int ion_binary_len_ion_decimal(decQuad *value, decContext *context )
{
    int     len;
    int64_t mantissa;
    int32_t exponent;

    // handle the zero value here, both in nibble 0, and -0 and 0 with sig digits
    if (decQuadIsZero(value)) {
        exponent = decQuadGetExponent(value);
        if (decQuadIsSigned(value)) {
            len = ion_binary_len_var_int_64( exponent );
            len += 1;  // the size of 1 == signed int 0 (+ or -)
        }
        else if (exponent != 0) {
            len = ion_binary_len_var_int_64( exponent );
        }
        else {
            // a "true" 0 we write out as the low nibble 0
            len = 0;
        }
        return len;
    }
    
    // if it's not some sort of zero then we have to decode 
    // the value and calculate the size of the exponent and 
    // the mantissa seperately

    // first decode the value
    ion_quad_get_digits_and_exponent_from_quad(value, context, &mantissa, &exponent);

    // now we know the pieces so can calculate the overall length
    len  = ion_binary_len_var_int_64(exponent);
    len += ion_binary_len_int_64(mantissa);

    return len;
}

int ion_binary_len_ion_float( double value )
{
    int len = 0;
    if (value == 0) {
        len = 0;
    }
    else {
        len = sizeof(value); // doubles are IEEE 754 64 bit binary floating point
    }
    return len;
}

int ion_binary_len_ion_timestamp( ION_TIMESTAMP *value, decContext *context )
{
    return ion_timestamp_binary_len(value, context);
}

iERR ion_binary_read_var_int_32(ION_STREAM *pstream, int32_t *p_value) 
{
    iENTER;
    int64_t int64Value = 0;

    IONCHECK (ion_binary_read_var_int_64(pstream, &int64Value));

    *p_value = (int32_t)int64Value;
    if (*p_value != int64Value) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }

    iRETURN;
}

iERR ion_binary_read_var_int_64(ION_STREAM *pstream, int64_t *p_value)
{
    iENTER;
    uint64_t unsignedValue = 0;
    BOOL     is_negative = FALSE;
    int      b;

    // read the first byte
    // first byte doesn't need to shift and has two bits 
    // to mask off - the stop bit and the sign bit
    ION_GET(pstream, b);
    if (b & 0x40) {
        is_negative = TRUE;
    }
    unsignedValue = (b & 0x3F);
    if ((b & 0x80) != 0) goto return_value;

    do {
        ION_GET(pstream, b);
        unsignedValue = (unsignedValue << 7) | (b & 0x7F);
        if ((b & 0x80) != 0) {
            goto return_value;
        }
    } while ((unsignedValue & HIGH_BIT_INT64) == 0);
    

    // if we get here we have more bits than we have room for :(
    FAILWITH(IERR_NUMERIC_OVERFLOW);

return_value:
    if (b < 0) FAILWITH(IERR_UNEXPECTED_EOF);
    IONCHECK (cast_to_int64(unsignedValue, is_negative, p_value));

    iRETURN;
}

iERR ion_binary_read_var_uint_32(ION_STREAM *pstream, uint32_t *p_value)
{
    iENTER;
    uint64_t uint64Value = 0;
    IONCHECK (ion_binary_read_var_uint_64(pstream, &uint64Value));
    *p_value = (uint32_t)uint64Value;
    if (*p_value != uint64Value) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR ion_binary_read_var_uint_64(ION_STREAM *pstream, uint64_t *p_value)
{
    iENTER;
    uint64_t retvalue = 0;
    int      b;

    // read the first byte
    ION_GET(pstream, b);
    retvalue = (b & 0x7F);
    if ((b & 0x80) != 0) goto return_value;

    do {
        ION_GET(pstream, b);
        retvalue = (retvalue << 7) | (b & 0x7F);
        if ((b & 0x80) != 0) {
            goto return_value;
        }
    } while ((retvalue & HIGH_BIT_INT64) == 0);

    // if we get here we have more bits than we have room for :(
    FAILWITH(IERR_NUMERIC_OVERFLOW);

return_value:
    if (b < 0) FAILWITH(IERR_UNEXPECTED_EOF);
    *p_value = retvalue;

    iRETURN;
}

iERR ion_binary_read_uint_32(ION_STREAM *pstream, int32_t len, uint32_t *p_value)
{
    iENTER;
    uint64_t retvalue = 0;
    IONCHECK(ion_binary_read_uint_64(pstream, len, &retvalue));
    *p_value = (uint32_t)retvalue;
    if (*p_value != retvalue) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR ion_binary_read_uint_64(ION_STREAM *pstream, int32_t len, uint64_t *p_value)
{
    iENTER;
    uint64_t retvalue = 0;
    int     b = 0;

    if (len > sizeof(uint64_t)) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }

    while (len > 0) {
        ION_GET(pstream, b);
        retvalue = (retvalue << 8) | b;
        len--;
    }
    if (b < 0) FAILWITH(IERR_UNEXPECTED_EOF); // ION_GET will return EOF's so we can just test the last byte read
    *p_value = retvalue;

    iRETURN;
}

iERR ion_binary_read_ion_int(ION_STREAM *pstream, int32_t len, BOOL is_negative, ION_INT *p_value)
{
    iENTER;
    int       b;
	int       bits, digit_count;
	II_DIGIT *digits;

    if (len < 1) {
		IONCHECK(_ion_int_zero(p_value));
		SUCCEED();
	}
	else {
		bits = len * II_BITS_PER_BYTE;
		digit_count = II_DIGIT_COUNT_FROM_BITS(bits);
		IONCHECK(_ion_int_extend_digits(p_value, digit_count, TRUE));
		digits = p_value->_digits;
		digit_count = p_value->_len;
        while (len--) {
			ION_GET(pstream, b);
            if (b < 0) FAILWITH(IERR_UNEXPECTED_EOF); // we have to test each one since mult and add will fail otherwise
			IONCHECK(_ion_int_multiply_and_add(digits, digit_count, II_BYTE_BASE, b));
        }
		// it's hard to say if the is_zero test, which checks 31 bits at a time,
		// or a test of the digits as they're being loaded is faster ...
        if (_ion_int_is_zero_bytes(p_value->_digits, p_value->_len)) {
			p_value->_signum = 0;
		}
		else {
			p_value->_signum = (is_negative ? -1 : 1);
		}
    }

    iRETURN;
}


iERR ion_binary_read_int_32(ION_STREAM *pstream, int32_t len, int32_t *p_value, BOOL *isNegativeZero)
{
    iENTER;
    int64_t int64Value = 0;

    IONCHECK(ion_binary_read_int_64(pstream, len, &int64Value, isNegativeZero));
    *p_value = (uint32_t)int64Value;
    if (*p_value != int64Value) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }

    iRETURN;
}

iERR ion_binary_read_int_64(ION_STREAM *pstream, int32_t len, int64_t *p_value, BOOL *isNegativeZero)
{
    iENTER;
    BOOL     isNegative = FALSE;
    uint64_t unsignedValue = 0;
    int      b;

    if (len) {
        // read the first byte, it's special since it carries the sign
        ION_GET(pstream, b);
        len--;
        if ((isNegative = (b & 0x80)) != 0) {
            b &= 0x7f;
        }

        unsignedValue = 0;
        if (len > 0) {
            IONCHECK(ion_binary_read_uint_64(pstream, len, &unsignedValue));
            b <<= (len * 8);
            unsignedValue = b | unsignedValue;
        }
        else {
            unsignedValue = b;
        }
    }

    IONCHECK(cast_to_int64(unsignedValue, isNegative, p_value));
    if (*p_value == 0 && isNegative) {
        *isNegativeZero = TRUE;
    }
    else {
        *isNegativeZero = FALSE;
    }

    iRETURN;
}


iERR ion_binary_read_double(ION_STREAM *pstream, int32_t len, double *p_value)
{
    iENTER;
    uint64_t intvalue = 0;
    int      b;

    ASSERT(pstream != NULL);
    ASSERT(p_value != NULL);

    if (len == 0) {
        *p_value = 0;
        SUCCEED();
    }
    if (len != sizeof(double)) {
        FAILWITHMSG(IERR_INVALID_BINARY, "Invalid binary size for double typed variable");
    }

    while (len > 0) {
        ION_GET(pstream, b);
        intvalue = (intvalue << 8) | b;
        len--;
    }
    if (b < 0) FAILWITH(IERR_UNEXPECTED_EOF); // ION_GET will return EOF's so we can just test the last byte read
    
    // int will fix any endian issues since (as far as I can find) the
    // endianness of int and floating point are the same
    *p_value = *((double *)&intvalue);

    iRETURN;
}

iERR ion_binary_read_decimal(ION_STREAM *pstream, int32_t len, decContext *context, decQuad *p_value)
{
    iENTER;
    int64_t start_exp, finish_exp, value_len;
    int32_t value_len_int32;
    int64_t mantissa;
    int32_t exponent;
    BOOL    isNegativeZero;

    ASSERT(pstream != NULL);
    ASSERT(len >= 0);
    ASSERT(context != NULL);
    ASSERT(p_value != NULL);

    if (len == 0) {
        decQuadZero(p_value);
        SUCCEED();
    }

    // we'll watch the start and end to know how many of the bytes
    // of the value were used by the exponent, the rest (if any)
    // will make up the mantissa
    start_exp = ION_INPUT_STREAM_POSITION(pstream);
    IONCHECK(ion_binary_read_var_int_32(pstream, &exponent));
    finish_exp = ION_INPUT_STREAM_POSITION(pstream);
    value_len = len - (finish_exp - start_exp);

    if (value_len < 0) {
        FAILWITHMSG(IERR_INVALID_BINARY, "Invalid binary size for decimal");
    }
    value_len_int32 = (int32_t)(value_len);

    isNegativeZero = FALSE;
    if (value_len_int32 == 0) {
        mantissa = 0;
    }
    else {
        IONCHECK(ion_binary_read_int_64(pstream, value_len_int32, &mantissa, &isNegativeZero));
    }

    // use our helper to put these values into the decimal
    ion_quad_get_quad_from_digits_and_exponent(mantissa, exponent, context, isNegativeZero, p_value);

    iRETURN;
}

iERR ion_binary_read_timestamp(ION_STREAM *pstream, int32_t len, decContext *context, ION_TIMESTAMP *p_value)
{
    iERR err = ion_timestamp_binary_read((ION_STREAM *)pstream, len, context, p_value);
    return err;
}

iERR ion_binary_write_float_value( ION_STREAM  *pstream, double value )
{
    iENTER;
    uint64_t intvalue = 0;
    BYTE     image[ UINT_64_IMAGE_LENGTH ];
    BYTE    *pb = &image[UINT_64_IMAGE_LENGTH - 1];
    int      len;

    ASSERT( UINT_64_IMAGE_LENGTH == 8 );                // we do depend on this here
    ASSERT( UINT_64_IMAGE_LENGTH == sizeof(value) );    // we also depend on this here
    ASSERT( pstream != NULL );

    // this copy allows us to make endian issues just int issues
    intvalue = *((uint64_t *)&value);

    // write 8 bits at a time into the temp buffer
    // from least to most significant, backwards
    // that is starting from the back of the temp
    // buffer (image) - and here we always write all 8 bytes
    for (len = 8; len; len--) {
        *pb-- = (BYTE)(intvalue & 0xff);
        intvalue >>= 8;
    }

    // now write the bytes out from most to least significant
    // to the output stream
    ASSERT((pb + 1) == image);

    IONCHECK(ion_binary_write_byte_array(pstream, image, 0, UINT_64_IMAGE_LENGTH));

    iRETURN;
}

iERR ion_binary_write_decimal_value( ION_STREAM *pstream, decQuad *value, decContext *context)
{
    iENTER;
    int64_t mantissa;
    int32_t exponent;

    ASSERT(pstream != NULL);
    ASSERT(value != NULL);

    // Could be 0e10, -0d0 or true 0 "0d0"
    if (decQuadIsZero(value)) {
        exponent = decQuadGetExponent(value);

        if (decQuadIsSigned(value)) {
            IONCHECK( ion_binary_write_var_int_64( pstream, exponent ));
            IONCHECK( ion_binary_write_int_64(pstream, 0, decQuadIsSigned(value)));
        }
        else if (exponent != 0) {
            IONCHECK( ion_binary_write_var_int_64( pstream, exponent ));
            // 0 mantissa does not need to be written out.
        }
        else {
            // a "true" 0 we already wrote out as the low nibble 0
            // If the value is zero ( i.e., 0d0) then L of Type Value field is zero and there are no length or representation fields.
        }
        SUCCEED();
    }
    
    // we write out the exponent and then the signed unscaled bits
    ion_quad_get_digits_and_exponent_from_quad(value, context, &mantissa, &exponent);

    // we know the exponent - we can write it out right now
    IONCHECK(ion_binary_write_var_int_64(pstream, exponent));
    IONCHECK(ion_binary_write_int_64(pstream, mantissa, FALSE));

    iRETURN;
}

iERR ion_binary_write_timestamp_value( ION_STREAM *pstream,  ION_TIMESTAMP *value, decContext *context )
{
    iERR err = ion_timestamp_binary_write( (ION_STREAM *)pstream, value, context );
    return err;
}

iERR ion_binary_write_int32_with_field_sid( ION_STREAM *pstream, SID sid, int32_t value )
{
    iENTER;
    int      len, tid;
    uint32_t unsignedValue;

    ASSERT(pstream != NULL);

    if (value < 0) {
        tid = TID_NEG_INT;
    }
    else {
        tid = TID_POS_INT;
    }
    unsignedValue = abs_int32(value);

    len = ion_binary_len_uint_64( unsignedValue );
    IONCHECK( ion_binary_write_var_uint_64( pstream, sid ));
    ION_PUT( pstream, makeTypeDescriptor( tid, len ));
    IONCHECK( ion_binary_write_uint_64( pstream, unsignedValue ));

    iRETURN;
}

iERR ion_binary_write_int64_with_field_sid( ION_STREAM *pstream, SID sid, int64_t value )
{
    iENTER;
    int      len, tid;
    uint64_t unsignedValue;

    ASSERT(pstream != NULL);

    if (value < 0) {
        tid = TID_NEG_INT;
        value = -value;
    }
    else {
        tid = TID_POS_INT;
    }
    unsignedValue = abs_int64(value);

    len = ion_binary_len_uint_64( unsignedValue );
    IONCHECK( ion_binary_write_var_uint_64( pstream, sid ) );
    IONCHECK( ion_binary_write_type_desc_with_length( pstream, tid, len ) );
    IONCHECK( ion_binary_write_uint_64( pstream, unsignedValue ) );

    iRETURN;
}

iERR ion_binary_write_string_with_field_sid( ION_STREAM *pstream, SID sid, ION_STRING *str)
{
    iENTER;

    IONCHECK(ion_binary_write_var_uint_64( pstream, sid ));
    IONCHECK(ion_binary_write_string_with_td_byte( pstream, str ));

    iRETURN;
}

iERR ion_binary_write_string_with_td_byte( ION_STREAM *pstream, ION_STRING *str )
{
    iENTER;
    SIZE written;
    ASSERT(pstream != NULL);

    if (ION_STRING_IS_NULL(str)) {
        ION_PUT( pstream, makeTypeDescriptor( TID_STRING, ION_lnIsNull ));
    }
    else {
        IONCHECK( ion_binary_write_type_desc_with_length( pstream, TID_STRING, str->length ));
        IONCHECK( ion_stream_write( pstream, str->value, str->length, &written ));
        if (written != str->length) FAILWITH(IERR_UNEXPECTED_EOF);
    }

    iRETURN;
}

iERR ion_binary_write_type_desc_with_length( ION_STREAM *pstream, int type, int32_t len )
{
    iENTER;
    ASSERT(pstream != NULL);

    if (len >= ION_lnIsVarLen) {
        ION_PUT( pstream, makeTypeDescriptor( type, ION_lnIsVarLen ));
        IONCHECK( ion_binary_write_var_uint_64( pstream, len ));
    }
    else {
        ION_PUT( pstream, makeTypeDescriptor( type, len ));
    }

    iRETURN;
}

iERR ion_binary_write_byte_array(ION_STREAM *pstream, BYTE image[], int startIndex, int endIndex) 
{
    iENTER;
    int ii;

    for (ii = startIndex; ii < endIndex; ii++) {
        ION_PUT( pstream, image[ii] );
    }
    SUCCEED();

    iRETURN;
}

iERR ion_binary_write_var_uint_64( ION_STREAM *pstream, uint64_t value )
{
    iENTER;
    BYTE  image[ VAR_UINT_64_IMAGE_LENGTH ];
    BYTE *pb = &image[VAR_UINT_64_IMAGE_LENGTH - 1];

    ASSERT( VAR_UINT_64_IMAGE_LENGTH == 10);      // we do depend on this in a switch below
    ASSERT( pstream != NULL );

    // the code in the else depends on the value being non-zero or it will
    // encounter a pb edge condition of not writting any bytes into image
    do {
        *pb-- = value & 0x7f;
        value >>= 7;
    } while (value > 0);
    pb++;
    // mark the least significant bits byte with the stop flag
    image[VAR_UINT_64_IMAGE_LENGTH - 1] |= 0x80;

    // now write the bytes out from most to least significant
    // to the output stream (+1 to make it easier to understand)
    ASSERT((pb - image) < MAX_SIZE);
    IONCHECK(ion_binary_write_byte_array(pstream, image, (SIZE)(pb - image), VAR_UINT_64_IMAGE_LENGTH));

    iRETURN;
}

iERR ion_binary_write_uint_64(ION_STREAM *pstream, uint64_t value)
{
    iENTER;
    BYTE  image[ UINT_64_IMAGE_LENGTH ];
    BYTE *pb = &image[UINT_64_IMAGE_LENGTH - 1];

    ASSERT( UINT_64_IMAGE_LENGTH == 8);      // we do depend on this here
    ASSERT( pstream != NULL );

    // write 8 bits at a time into the temp buffer
    // from least to most significant, backwards
    // that is starting from the back of the temp
    // buffer (image)
    do {
        *pb-- = (BYTE)(value & 0xff);
        value >>= 8;
    } while (value > 0);

    // now write the bytes out from most to least significant
    // to the output stream
    ASSERT(((pb - image)+1) < UINT_64_IMAGE_LENGTH);
    IONCHECK(ion_binary_write_byte_array(pstream, image, (SIZE)((pb - image)+1), UINT_64_IMAGE_LENGTH));
    iRETURN;
}

iERR ion_binary_write_var_int_64( ION_STREAM *pstream, int64_t value )
{
    iENTER;
    BYTE     image[ VAR_INT_64_IMAGE_LENGTH ];
    BYTE    *pb = &image[VAR_INT_64_IMAGE_LENGTH - 1];
    BOOL     is_negative = (value < 0);
    BOOL     msbIsOne = FALSE;  // true if the last byte's 7th bit is 1. required for proper serialization of signed varint
    uint64_t unsignedValue;

    ASSERT( VAR_INT_64_IMAGE_LENGTH == 10);      // we do depend on this here
    ASSERT( pstream != NULL );

    unsignedValue = abs_int64(value);

    // write 7 bits at a time in, from least to most significant
    // note that 0 goes through once
    do {
        *pb-- = unsignedValue & 0x7f;
        msbIsOne = unsignedValue & 0x40;
        unsignedValue >>= 7;
    } while( unsignedValue );

    // mark the least significant bits byte with the stop flag
    image[VAR_INT_64_IMAGE_LENGTH - 1] |= 0x80;

    // and we write the sign bit in the most significant byte
    if (!msbIsOne) {
      pb++;    // go to the previous byte if only last 7th bit is not 1!!!
    }
    else {
      *pb = 0; // otherwise initialize the byte we haven't written yet
    }
    if (is_negative) *pb |= 0x40;

    // now write the bytes out from most to least significant
    // to the output stream (+1 to make it easier to understand)
    ASSERT((pb - image) < MAX_SIZE); // because it is limited by the number of bits in a uint64 value
    IONCHECK(ion_binary_write_byte_array(pstream, image, (SIZE)(pb - image), VAR_INT_64_IMAGE_LENGTH));

    iRETURN;
}

iERR ion_binary_write_int_64(ION_STREAM *pstream, int64_t value, BOOL isNegativeZero)
{
    iENTER;
    BYTE     image[ INT_64_IMAGE_LENGTH ];
    BYTE    *pb = &image[INT_64_IMAGE_LENGTH - 1];
    BOOL     is_negative = (value < 0) || ((value == 0) && isNegativeZero);
    uint64_t unsignedValue;

    ASSERT( INT_64_IMAGE_LENGTH == 9);      // we do depend on this here
    ASSERT( pstream != NULL );

    unsignedValue = abs_int64(value);

    // write 8 bits at a time into the temp buffer
    // from least to most significant, backwards
    // that is starting from the back of the temp
    // buffer (image)
    do {
        *pb-- = (BYTE)(unsignedValue & 0xff);
        unsignedValue >>= 8;
    } while( unsignedValue );

    // now put in the sign bit, and an extra byte if that's needed
    pb++; // go back to the last byte written (the most significant one)
    if (((*pb) & 0x80) != 0) {
        // if the high bit of the most significant byte is set
        // then we need another byte to keep the sign bit to itself
        pb--;
        *pb = 0;
    }
    // we know there's room for the sign bit, so set if (if that's appropriate)
    if (is_negative) {
        *pb |= 0x80;
    }

    // now write the bytes out from most to least significant
    // to the output stream
    ASSERT((pb - image) < MAX_SIZE); // because it is limited by the number of bits in a uint64 value
    IONCHECK(ion_binary_write_byte_array(pstream, image, (SIZE)(pb - image), INT_64_IMAGE_LENGTH));

    iRETURN;
}
