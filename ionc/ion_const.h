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

//
// defines constants, such as static symbol id's and
// binary type values for Ion
//

#ifndef ION_CONST_H_
#define ION_CONST_H_

#ifdef __cplusplus
extern "C" {
#endif

#define DEFAULT_ANNOTATION_LIMIT         10
#define DEFAULT_WRITER_STACK_DEPTH       10

//#define DEFAULT_CHUNK_THRESHOLD     DEFAULT_BLOCK_SIZE // TODO - get the right size here!
    // default block size is (currently) 64k, it has been 256k and 128mb
    // so the current user alloc limit now would be 16k (it was 2k before this)
#define DEFAULT_USER_ALLOC_THRESHOLD   (DEFAULT_BLOCK_SIZE / 4)

    // default block size is (currently) 64k, it has been 256k and 128mb
    // so the current chunk threshold now would be 16k (it was 2k before this)
#define DEFAULT_CHUNK_THRESHOLD        (DEFAULT_USER_ALLOC_THRESHOLD)

    // default block size is (currently) 64k, it has been 256k and 128mb
    // so the current user alloc limit now would be 16k (it was 2k before this)
#define DEFAULT_SYMBOL_THRESHOLD        (DEFAULT_USER_ALLOC_THRESHOLD)

    // this is the size of the single buffer which will hold the set of annotations
    // being cached while reading a single value, all the annotations
#define DEFAULT_ANNOTATION_BUFFER_LIMIT (DEFAULT_USER_ALLOC_THRESHOLD)

#define MIN_ANNOTATION_LIMIT              1
#define MIN_WRITER_STACK_DEPTH            2
#define MIN_SYMBOL_THRESHOLD             32
#define MIN_CHUNK_THRESHOLD              32
#define MIN_ION_ALLOCATION_BLOCK_SIZE    32

#define ION_SYSTEM_VERSION  1

#define TID_NONE      -2
#define TID_EOF       -1
#define TID_NULL       0
#define TID_BOOL       1
#define TID_POS_INT    2
#define TID_NEG_INT    3
#define TID_FLOAT      4
#define TID_DECIMAL    5
#define TID_TIMESTAMP  6
#define TID_SYMBOL     7
#define TID_STRING     8
#define TID_CLOB       9
#define TID_BLOB      10 /* 0xa */
#define TID_LIST      11 /* 0xb */
#define TID_SEXP      12 /* 0xc */
#define TID_STRUCT    13 /* 0xd */
#define TID_UTA       14 /* 0xe USER TYPE ANNOTATION */
#define TID_UNUSED    15 /* 0xf */

// pseudo type to represent the outer datagram
// which is an sexp in other respects
#define TID_DATAGRAM  16

// pseudo types for reporting data that exceeds
// the imposed buffer limit (see 
#define TID_CHUNKED_STRING  17
#define TID_CHUNKED_SYMBOL  18
#define TID_CHUNKED_CLOB    19
#define TID_CHUNKED_BLOB    20

// pseudo type to represent the outer value
// if the user has executed an explicit seek 
// this is a datagram or an sexp in other respects
#define TID_SEEK_DATAGRAM   21

#define ION_lnIsNull           0x0f
#define ION_lnBooleanTrue      0x01
#define ION_lnBooleanFalse     0x00
#define I0N_lnNumericZero      0x00
#define ION_lnIsEmptyContainer 0x00
#define ION_lnIsOrderedStruct  0x01
#define ION_lnIsVarLen         0x0e

/**
 * Make a type descriptor from two nibbles; all of which are represented as
 * ints.
 *
 * @param highNibble must be a positive int between 0x00 and 0x0F.
 * @param lowNibble must be a positive int between 0x00 and 0x0F.INITTO((ION_TYPE) * @return the combined nibbles);
 */
#define makeTypeDescriptor(highNibble, lowNibble)  ((highNibble << 4) | lowNibble)

/**
 * Extract the type code (high nibble) from a type descriptor.
 *
 * @param td must be a positive int between 0x00 and 0xFF.
 *
 * @return the high nibble of the input byte, between 0x00 and 0x0F.
 */
#define getTypeCode(td)   (((td) >> 4) & 0xf)
#define getLowNibble(td)  ((td) & 0xf)

#define IonTrue   makeTypeDescriptor(TID_BOOL, ION_lnBooleanTrue)
#define IonFalse  makeTypeDescriptor(TID_BOOL, ION_lnBooleanFalse)

GLOBAL BYTE ION_VERSION_MARKER[ION_VERSION_MARKER_LENGTH]
#ifdef INIT_STATICS
 = { 0xe0, 0x01, 0x00, 0xea }
#endif
;

// UTF-8 constants
// these are used for various Unicode translation where
// we need to convert the utf-16 Java characters into
// unicode scalar values (utf-32 more or less) and back
#define ION_high_surrogate_value    0xD800
#define ION_low_surrogate_value     0xDC00
#define ION_either_surrogate_mask   0xF800 /* 0x3f << 10; or the top 6 bits is the marker the low 10 is the 1/2 character */
#define ION_specific_surrogate_mask 0xFC00 /* 0x3f << 10; or the top 6 bits is the marker the low 10 is the 1/2 character */
#define ION_surrogate_value_mask   ~0xFC00 /* 0x3f << 10; or the top 6 bits is the marker the low 10 is the 1/2 character */
#define ION_surrogate_utf32_offset  0x10000
#define ION_surrogate_utf32_shift   10
#define ION_max_unicode_scalar      0x10FFFF
#define ION_utf8_max_length         4

#define ION_UTF8_HEADER_BITS(c)     ((((uint32_t)(c)) >> 3) & 0x1f)  /* value from 0 to 31 */

#define ION_utf8_1byte_bits   7
#define ION_utf8_2byte_bits  11
#define ION_utf8_3byte_bits  16
#define ION_utf8_4byte_bits  21

#define ION_utf8_1byte_max  ((1 << ION_utf8_1byte_bits) - 1)  /* 1 << 7 = 128, -1 = 127 */
#define ION_utf8_2byte_max  ((1 << ION_utf8_2byte_bits) - 1)
#define ION_utf8_3byte_max  ((1 << ION_utf8_3byte_bits) - 1)
#define ION_utf8_4byte_max  ((1 << ION_utf8_4byte_bits) - 1)

#define ION_utf8_1byte_header       0
#define ION_utf8_1byte_mask         (ION_utf8_1byte_header | 0x80)
#define ION_utf8_2byte_header       0xc0
#define ION_utf8_2byte_mask         (ION_utf8_2byte_header | 0x20)
#define ION_utf8_3byte_header       0xe0
#define ION_utf8_3byte_mask         (ION_utf8_3byte_header | 0x10)
#define ION_utf8_4byte_header       0xf0
#define ION_utf8_4byte_mask         (ION_utf8_4byte_header | 0x08)

#define ION_utf8_trailing_header    0x80
#define ION_utf8_trailing_MASK      (ION_utf8_trailing_header | 0x40 )
#define ION_utf8_trailing_bits_mask 0x3f

#define CHAR2INT(c) ((int)(c) & 0xff)

// see ion_isHighSurrogate (et al) in ion_helpers.c
//#define ION_IS_SURROGATE(x)      (((x) & ION_either_surrogate_mask) == ION_high_surrogate_value)
//#define ION_IS_HIGH_SURROGATE(x) (((x) & ION_specific_surrogate_mask) == ION_high_surrogate_value)
//#define ION_IS_LOW_SURROGATE(x)  (((x) & ION_specific_surrogate_mask) == ION_low_surrogate_value)

#define ION_IS_ASCII_CHARACTER(c) (((c) & ~0x7f) == 0)

#define ION_is_utf8_1byte_header(c) (((c) & ION_utf8_1byte_mask) == ION_utf8_1byte_header)
#define ION_is_utf8_2byte_header(c) (((c) & ION_utf8_2byte_mask) == ION_utf8_2byte_header)
#define ION_is_utf8_3byte_header(c) (((c) & ION_utf8_3byte_mask) == ION_utf8_3byte_header)
#define ION_is_utf8_4byte_header(c) (((c) & ION_utf8_4byte_mask) == ION_utf8_4byte_header)
#define ION_is_utf8_trailing_char_header(c) (((c) & ION_utf8_trailing_MASK) == ION_utf8_trailing_header)

#define ION_unicode_byte_order_mark_utf8_start 0xEF
GLOBAL BYTE ION_unicode_byte_order_mark_utf8[] 
#ifdef INIT_STATICS
= { 0xEF, 0xBB, 0xBF }
#endif
;

// misc marginally useful constants
#define ION_BB_TOKEN_LEN     1

#define ION_BB_VAR_INT32_LEN_MAX   5 /* 31 bits (java limit) / 7 bits per byte = 5 bytes */
#define ION_BB_VAR_INT64_LEN_MAX  10
#define ION_BB_INT64_LEN_MAX       8
#define ION_BB_VAR_LEN_MIN         1
#define ION_BB_MAX_7BIT_INT      127
#define ION_INT32_SIZE             4
#define ION_UNT8_MAX_BYTE_LENGTH   4

#define MAX_MESSAGE_LENGTH       128 /* maximum length of diagnostic messages, esp for ion_*_to_str() */
#define MAX_INT64_LENGTH          20 /* length of base 10 digits of 2^64 -9,223,372,036,854,775,808*/
#define MAX_INT32_LENGTH          11 /* length of base 10 digits of 2^32 -2,147,483,647 */

#define MAX_BOOL_IMAGE             9 /* max length of a boolean image, true, false, null.bool */
#define MAX_INT64_IMAGE           23 /* ceil((64 bits)/(3 bits per dec digit min)) + 1 sign, really 21 */

#define MAX_INT64_DIVIDE_BY_10   922337203685477580LL
#define DOUBLE_IMAGE_LENGTH      316
                                     /*   1 - sign, 
                                      308 - decimal digits (+/- 10**308)
                                        1 - extra leading 0 for fractions, 
                                        1 - decimal point
                                        1 - 'e', 
                                        1 - sign of exponent, 
                                        3 - digits in exponent 
                                      ---
                                      316 TODO: this is really longer than it has much reason to be
                                     */

#ifdef __cplusplus
}
#endif

#endif
