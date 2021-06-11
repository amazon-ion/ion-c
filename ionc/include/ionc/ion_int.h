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
/**@file */

//
// Ion int, arbitrary integer representation
//
// Primary use cases are deserialization and serialization
// of arbitarty integer values in either text (decimal) 
// or binary (base 256) representation.  This requires
// the ability to convert between the two representations.
// This also includes a limited abilty to get the values
// out in more conventional data formats as well.
//
// This version X is mildly optimized for a 32 bit 
// int with 64 bit int support (i.e. 32 bit arch, 
// even though 64 bit is common)
//

#ifndef ION_INT_H_
#define ION_INT_H_

	#ifndef ION_INT_INITTO
	#define ION_INT_INITTO /* nothing */
	  #ifdef ION_INT_INIT
	  #undef ION_INT_INIT
	  #endif
	#else
	  #ifndef ION_INT_INIT
	  #define ION_INT_INIT init
	  #endif
	#endif

	#ifndef ION_INT_GLOBAL
	#define ION_INT_GLOBAL extern
	#endif

#include <string.h>
#include "ion_types.h"
#include "ion_platform_config.h"
#include "ion_decimal.h"

#ifdef __cplusplus
extern "C" {
#endif

// moved to ion_types.h typedef struct _ion_int        ION_INT;

typedef uint32_t II_DIGIT;
typedef uint64_t II_LONG_DIGIT;

#define II_PLUS            '+'
#define II_MINUS           '-'

#define II_BASE                   ((uint32_t)0x80000000)
#define II_MASK                   ((uint32_t)0x7FFFFFFF)
#define II_SHIFT                  ((uint32_t)31)

#define II_BITS_PER_II_DIGIT        II_SHIFT
#define II_DIGIT_COUNT_FROM_BITS(bits) (((bits) == 0) ? 1 : (((((int)bits) - 1) / II_BITS_PER_II_DIGIT) + 1))

#define II_STRING_BASE              10
#define II_BITS_PER_DEC_DIGIT       3.35 /* upper bound beyond 1 gig */
#define II_DEC_DIGIT_PER_BITS       3.32191780821918 /* lower bound */
#define II_II_DIGITS_PER_DEC_DIGIT  0.108064516 /* or: (3.35/31) */
#define II_DEC_DIGITS_PER_II_DIGIT  9.253731343 /* or: (1/.108064516) */

#define DECIMAL_DIGIT_COUNT_FROM_BITS(bits) (((bits) == 0) ? 1 : ((SIZE)(((double)(bits) / II_DEC_DIGIT_PER_BITS) + 1)))

#define II_BITS_PER_HEX_DIGIT       4
#define II_HEX_BASE                 16
#define II_HEX_RADIX_CHARS          "xX"
#define II_BITS_PER_BINARY_DIGIT    1
#define II_BINARY_BASE              2
#define II_BINARY_RADIX_CHARS       "bB"

#define II_MAX_DIGIT               (II_MASK)  /* aka 2,147,483,647 decimal, 2 gig */
#define II_BITS_PER_BYTE            8
#define II_BYTE_BASE                256
#define II_BYTE_MASK                0xFF
#define II_BYTE_SIGN_BIT            0x80
#define II_BYTE_NEG_OVERFLOW_LIMIT  0xFE

#define II_INT64_BIT_THRESHOLD     (sizeof(int64_t)*8-2) /* sign and 1 for good measure */

#define II_SMALL_DIGIT_ARRAY_LENGTH ((256 / II_BITS_PER_II_DIGIT)+1)


typedef struct _ion_int {
    void     *_owner;
    int       _signum;       // sign, +1 or -1, or 0
    SIZE    _len;          // number of digits in the _digits array (-1 if null)
    II_DIGIT *_digits;       // array of "digits" in some large base (2^31 currently)
} _ion_int;

ION_INT_GLOBAL II_DIGIT        g_int_zero_bytes[] 
#ifdef ION_INT_INIT
  = { (II_DIGIT)0 }
#endif
;
ION_INT_GLOBAL ION_INT         g_Int_Zero
#ifdef ION_INT_INIT
  = { NULL,
      0,
      1,
      g_int_zero_bytes
  }
#endif
;
ION_INT_GLOBAL ION_INT         g_Int_Null
#ifdef ION_INT_INIT
  = { NULL,
      0,
      0,
      NULL
  }
#endif
;

ION_INT_GLOBAL THREAD_LOCAL_STORAGE BOOL            g_ion_int_globals_initialized; // NOTE: this is initialized to 0 according to C standard.
ION_INT_GLOBAL THREAD_LOCAL_STORAGE decQuad         g_digit_base_quad;
ION_INT_GLOBAL THREAD_LOCAL_STORAGE decNumber       g_digit_base_number;


//////////////////////////////////////////////////////////////
// public functions
//////////////////////////////////////////////////////////////
ION_API_EXPORT iERR ion_int_alloc           (void *owner, ION_INT **piint);
ION_API_EXPORT void ion_int_free            (ION_INT *iint);
ION_API_EXPORT iERR ion_int_init            (ION_INT *iint, void *owner);
ION_API_EXPORT iERR ion_int_copy            (ION_INT *dst, ION_INT *src, void *owner);

ION_API_EXPORT iERR ion_int_is_null         (ION_INT *iint, BOOL *p_is_null);
ION_API_EXPORT iERR ion_int_is_zero         (ION_INT *iint, BOOL *p_bool);
ION_API_EXPORT iERR ion_int_compare         (ION_INT *left, ION_INT *right, int *p_result);
ION_API_EXPORT iERR ion_int_signum          (ION_INT *iint, int32_t *p_signum);
ION_API_EXPORT iERR ion_int_highest_bit_set (ION_INT *iint, SIZE *p_pos);

ION_API_EXPORT iERR ion_int_from_string     (ION_INT *iint, const iSTRING p_str);
ION_API_EXPORT iERR ion_int_from_hex_string (ION_INT *iint, const iSTRING p_str);
ION_API_EXPORT iERR ion_int_from_binary_string(ION_INT *iint, const iSTRING p_str);
ION_API_EXPORT iERR ion_int_from_chars      (ION_INT *iint, const char *p_chars, SIZE char_limit);
ION_API_EXPORT iERR ion_int_from_hex_chars  (ION_INT *iint, const char *p_chars, SIZE char_limit);
ION_API_EXPORT iERR ion_int_from_binary_chars(ION_INT *iint, const char *p_chars, SIZE char_limit);
ION_API_EXPORT iERR ion_int_from_bytes      (ION_INT *iint, BYTE *buf, SIZE limit);
ION_API_EXPORT iERR ion_int_from_abs_bytes  (ION_INT *iint, BYTE *buf, SIZE limit, BOOL is_negative);
ION_API_EXPORT iERR ion_int_from_long       (ION_INT *iint, int64_t value);

/**
 * @deprecated use of decQuads directly is deprecated. ION_DECIMAL should be used. See `ion_decimal_to_ion_int`.
 */
ION_API_EXPORT iERR ion_int_from_decimal    (ION_INT *iint, const decQuad *p_value, decContext *context);

ION_API_EXPORT iERR ion_int_char_length     (ION_INT *iint, SIZE *p_len);
ION_API_EXPORT iERR ion_int_to_char         (ION_INT *iint, BYTE *p_str, SIZE len, SIZE *p_written);
ION_API_EXPORT iERR ion_int_to_string       (ION_INT *iint, hOWNER owner, iSTRING p_str);

ION_API_EXPORT iERR ion_int_byte_length     (ION_INT *iint, SIZE *p_byte_length);
ION_API_EXPORT iERR ion_int_to_bytes        (ION_INT *iint, SIZE starting_int_byte_offset, BYTE *buffer, SIZE buffer_length, SIZE *bytes_written);
ION_API_EXPORT iERR ion_int_abs_bytes_length(ION_INT *iint, SIZE *p_byte_length);
ION_API_EXPORT iERR ion_int_to_abs_bytes    (ION_INT *iint, SIZE starting_int_byte_offset, BYTE *buffer, SIZE buffer_length, SIZE *bytes_written);
ION_API_EXPORT iERR ion_int_to_int64        (ION_INT *iint, int64_t *p_int64);
ION_API_EXPORT iERR ion_int_to_int32        (ION_INT *iint, int32_t *p_int32);

/**
 * @deprecated use of decQuads directly is deprecated. ION_DECIMAL should be used. See `ion_decimal_from_ion_int`.
 */
ION_API_EXPORT iERR ion_int_to_decimal      (ION_INT *iint, decQuad *p_quad, decContext *context);

//////////////////////////////////////////////////////////////
// internal functions
//////////////////////////////////////////////////////////////
void _ion_int_dump_quad(decQuad *quad, int64_t expected);
int  _ion_int_init_globals(void);

iERR _ion_int_from_decimal_number(ION_INT *iint, const decNumber *p_value, decContext *context);
iERR _ion_int_to_decimal_number(ION_INT *iint, decNumber *p_value, decContext *context);

iERR _ion_int_validate_arg(const ION_INT *iint);
iERR _ion_int_validate_arg_with_ptr(const ION_INT *iint, const void *ptr);
iERR _ion_int_validate_non_null_arg_with_ptr(const ION_INT *iint, const void *ptr);

void      _ion_int_init(ION_INT *iint, void *owner);
iERR      _ion_int_zero(ION_INT *iint);
void *    _ion_int_realloc_helper(void *value, SIZE old_len, void *owner, SIZE new_len);
iERR      _ion_int_extend_digits(ION_INT *iint, SIZE digits_needed, BOOL zero_fill);
II_DIGIT *_ion_int_buffer_temp_copy( II_DIGIT *orig_digits, SIZE len, II_DIGIT *cache_buffer, SIZE cache_len);
II_DIGIT *_ion_int_buffer_temp_copy( II_DIGIT *orig_digits, SIZE len, II_DIGIT *cache_buffer, SIZE cache_len);
void      _ion_int_free_temp(II_DIGIT *temp_buffer, II_DIGIT *cache_buffer);

BOOL      _ion_int_from_bytes_helper(ION_INT *iint, BYTE *buf, SIZE byte_idx, SIZE limit, BOOL invert, BOOL includes_sign_byte);
iERR      _ion_int_from_chars_helper(ION_INT *iint, const char *str, SIZE len);
iERR      _ion_int_from_radix_chars_helper(ION_INT *iint, const char *str, SIZE len, unsigned int *digit_values, unsigned int base, unsigned int bits_per_digit, const char *radix_chars);
iERR      _ion_int_from_hex_chars_helper(ION_INT *iint, const char *str, SIZE len);
iERR      _ion_int_from_binary_chars_helper(ION_INT *iint, const char *str, SIZE len);

BOOL _ion_int_is_null_helper(const ION_INT *iint);
BOOL _ion_int_is_zero(const ION_INT *iint);
BOOL _ion_int_is_zero_bytes(const II_DIGIT *digits, SIZE len);

SIZE _ion_int_highest_bit_set_helper(const ION_INT *iint);

SIZE _ion_int_get_char_len_helper(const ION_INT *iint);
iERR   _ion_int_to_string_helper(ION_INT *iint, char *strbuf, SIZE buflen, SIZE *p_written);

BOOL   _ion_int_is_high_bytes_high_bit_set_helper(const ION_INT *iint, SIZE abs_byte_count);
SIZE _ion_int_bytes_length_helper(const ION_INT *iint);
iERR   _ion_int_to_bytes_helper(ION_INT *iint, SIZE bytes_in_int, SIZE starting_int_byte_offset, BOOL is_neg, BYTE *buffer, SIZE buffer_length, SIZE *bytes_written);

SIZE _ion_int_abs_bytes_length_helper(const ION_INT *iint);
SIZE _ion_int_abs_bytes_signed_length_helper(const ION_INT *iint);
//iERR   _ion_int_to_abs_bytes_helper(ION_INT *iint, SIZE bytes_in_int, SIZE starting_int_byte_offset, BOOL is_neg, BYTE *buffer, SIZE buffer_length, SIZE *bytes_written);

iERR   _ion_int_to_int64_helper(ION_INT *iint, int64_t *p_int64);

iERR  _ion_int_add_digit(II_DIGIT *digits, SIZE digit_count, II_DIGIT value);
iERR  _ion_int_sub_digit(II_DIGIT *digits, SIZE digit_count, II_DIGIT value);
/* not used
iERR  _ion_int_multiply_by_digit(II_DIGIT *digits, SIZE digit_count, II_DIGIT value);
*/
iERR  _ion_int_multiply_and_add(II_DIGIT *digits, SIZE digit_count, II_DIGIT mult_value, II_DIGIT add_value);
iERR  _ion_int_divide_by_digit(II_DIGIT *digits, SIZE digit_count, II_DIGIT  value, II_DIGIT *p_remainder);

#ifdef __cplusplus
}
#endif

#endif /* ION_INT_H_ */
