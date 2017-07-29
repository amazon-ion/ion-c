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
// ion timestamp support routines
// includes toString and Parse (in Java terms)
//
// overall format is (from ion wiki):
//
//      null.timestamp                   // A null timestamp value
//      
//      2007-02-23                       // A day, equivalent to 2007-02-23T00:00:00-00:00
//      2007-02-23T12:14Z                // Seconds are optional, but timezone is not
//      2007-02-23T12:14:33.079-08:00    // A timestamp with millisecond precision and PST local time
//      2007-02-23T20:14:33.079Z         // The same instant in UTC ("zero" or "zulu")
//      2007-02-23T20:14:33.079+00:00    // The same instant with explicit local offset
//      

#ifndef ION_BINARY_H_INCLUDED
#define ION_BINARY_H_INCLUDED

#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif


#define ION_BINARY_MAKE_1_BYTE_INT( x )         ((x < 0) ? ((-(x)) | 0x80) : (x))
#define ION_BINARY_MAKE_1_BYTE_VAR_INT( x )     ((x < 0) ? ((-(x)) | 0x80 | 0x40) : ((x) | 0x80))
#define ION_BINARY_TYPE_DESC_LENGTH             (1)

#define ION_BINARY_VAR_INT_NEGATIVE_ZERO        (0x80 | 0x40)
#define ION_BINARY_VAR_INT_ZERO                 (0x80)
#define ION_TIMESTAMP_ZERO_ENCODING_LEN         (1)

#define ION_BINARY_UNKNOWN_LENGTH               (-1)

#define VAR_UINT_64_IMAGE_LENGTH                ((SIZE)(((sizeof(uint64_t)*8) / 7) + 1))
#define UINT_64_IMAGE_LENGTH                    ((SIZE)(sizeof(uint64_t)))
#define VAR_INT_64_IMAGE_LENGTH                 ((SIZE)(((sizeof(int64_t)*8) / 7) + 1)) /* same as var_uint */
#define INT_64_IMAGE_LENGTH                     ((SIZE)(sizeof(int64_t) + 1)) /* needs 1 extra byte for sign bit overflow */


/** Calculate the length of binary encoded uint.
 *
 */
ION_API_EXPORT int ion_binary_len_uint_64(uint64_t value);


/** Write the content of the given byte array to stream, from startIndex to endIndex
 *
 */
ION_API_EXPORT iERR ion_binary_write_byte_array(ION_STREAM *pstream, BYTE image[], int startIndex, int endIndex);
    
/** Calculate the length of binary encoded int.
 *
 */
ION_API_EXPORT int ion_binary_len_int_64(int64_t value);

/** Calculate the length of binary encoded variable uint.
 *
 */
ION_API_EXPORT int ion_binary_len_var_uint_64(uint64_t value);

/** Calculate the length of binary encoded variable int.
 *
 */
ION_API_EXPORT int ion_binary_len_var_int_64(int64_t value);

/** Calculate the length of a binary encoded variable int from the given unsigned value.
 *  If the highest bit in the most significant byte is set, space for one extra sign byte will be added.
 */
ION_API_EXPORT int ion_binary_len_int_64_unsigned(uint64_t value);

/** Get the size of ion binary representation fields of the given double value. Fixed at sizeof(double)
 *
 */
ION_API_EXPORT int ion_binary_len_ion_float(double value);

ION_API_EXPORT iERR ion_binary_read_var_int_32       (ION_STREAM *pstream, int32_t *p_value);
ION_API_EXPORT iERR ion_binary_read_var_int_64       (ION_STREAM *pstream, int64_t *p_value);
ION_API_EXPORT iERR ion_binary_read_var_uint_32      (ION_STREAM *pstream, uint32_t *p_value);
ION_API_EXPORT iERR ion_binary_read_var_uint_64      (ION_STREAM *pstream, uint64_t *p_value);
ION_API_EXPORT iERR ion_binary_read_uint_32          (ION_STREAM *pstream, int32_t len, uint32_t *p_value);
ION_API_EXPORT iERR ion_binary_read_uint_64          (ION_STREAM *pstream, int32_t len, uint64_t *p_value);
ION_API_EXPORT iERR ion_binary_read_int_32           (ION_STREAM *pstream, int32_t len, int32_t *p_value, BOOL *isNegativeZero);
ION_API_EXPORT iERR ion_binary_read_int_64           (ION_STREAM *pstream, int32_t len, int64_t *p_value, BOOL *isNegativeZero);
ION_API_EXPORT iERR ion_binary_read_ion_int          (ION_STREAM *pstream, int32_t len, BOOL is_negative, ION_INT *p_value);
ION_API_EXPORT iERR _ion_binary_read_mixed_int_helper(ION_READER *preader);

ION_API_EXPORT iERR ion_binary_read_double         (ION_STREAM *pstream, int32_t len, double *p_value);
ION_API_EXPORT iERR ion_binary_read_decimal        (ION_STREAM *pstream, int32_t len, decContext *context,
                                                    decQuad *p_quad, decNumber **p_num);
ION_API_EXPORT iERR ion_binary_read_timestamp      (ION_STREAM *pstream, int32_t len, decContext *context, ION_TIMESTAMP *p_value);
ION_API_EXPORT iERR ion_binary_read_string         (ION_STREAM *pstream, int32_t len, ION_STRING *p_value);

ION_API_EXPORT iERR ion_binary_write_float_value           ( ION_STREAM *pstream, double value );

ION_API_EXPORT iERR ion_binary_write_int32_with_field_sid  ( ION_STREAM *pstream, SID field_sid, int32_t value );
ION_API_EXPORT iERR ion_binary_write_int64_with_field_sid  ( ION_STREAM *pstream, SID sid, int64_t value );
ION_API_EXPORT iERR ion_binary_write_string_with_field_sid ( ION_STREAM *pstream, SID field_sid, ION_STRING *str );
ION_API_EXPORT iERR ion_binary_write_string_with_td_byte   ( ION_STREAM *pstream, ION_STRING *str );

ION_API_EXPORT iERR ion_binary_write_type_desc_with_length ( ION_STREAM *pstream, int tid, int32_t len );

/** Write out binary encoded uint.
 *
 */
ION_API_EXPORT iERR ion_binary_write_uint_64(ION_STREAM *pstream, uint64_t value);

/** Write out binary encoded int.
 * As int cannot represent -0, a special flag is passed in to indicate -0.
 * The flag only applies if value == 0
 *
 */
ION_API_EXPORT iERR ion_binary_write_int_64(ION_STREAM *pstream, int64_t value, BOOL isNegativeZero);

/**
 * Write an unsigned magnitude as a binary encoded int with the given sign.
 */
ION_API_EXPORT iERR ion_binary_write_int_64_unsigned(ION_STREAM *pstream, uint64_t value, BOOL isNegative);

/** Write out binary encoded variable uint.
 *
 */
ION_API_EXPORT iERR ion_binary_write_var_uint_64(ION_STREAM *pstream, uint64_t value);

/** Write out binary encoded variable int.
 *
 */
ION_API_EXPORT iERR ion_binary_write_var_int_64(ION_STREAM *pstream, int64_t value);
    

#ifdef __cplusplus
}
#endif

#endif // ION_BINARY_H_INCLUDED

