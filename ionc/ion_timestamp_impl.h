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

#ifndef ION_TIMESTAMP_IMPL_H_
#define ION_TIMESTAMP_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

// 9999-99-99T23:59:59.9999...999999+12:15\0
// the precision of the quad decimal allows 43 decimal digits
// with 31622400 minutes in a year that's 316,192,377,600 in minutes
// or about 12 digits of precision - that leave 31 digits for seconds
// so: 16 (through minutes) + 2 + 31 + 6 + 1 = 55 characters

#define ION_TIMESTAMP_NULL_IMAGE            "null.timestamp"
#define ION_TIMESTAMP_NULL_IMAGE_LEN        14
#define ION_TIMESTAMP_NULL_OFFSET_IMAGE     "-00:00"
#define ION_TIMESTAMP_NULL_OFFSET_IMAGE_LEN 6

#define ION_TS_NULL      0x00
#define ION_TT_BIT_TZ    0x80
#define ION_TS_SUB_DATE  (ION_TT_BIT_MIN | ION_TT_BIT_SEC | ION_TT_BIT_FRAC)

#define HAS_TZ_OFFSET(pt) IS_FLAG_ON((pt)->precision, ION_TT_BIT_TZ)

GLOBAL int JULIAN_DAY_PER_MONTH[2][12]
#ifdef INIT_STATICS
=  {
// jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec
    {   31,  28,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 },
    {   31,  29,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 },
}
#endif
;

BOOL _ion_timestamp_is_leap_year(int y);
int  _ion_timestamp_julian_day  (int month, int day, BOOL is_leapyear);
int  _ion_timestamp_month       (int julianday, BOOL is_leapyear);
int  _ion_timestamp_day         (int julianday, BOOL is_leapyear);

int32_t _ion_timestamp_julian_day_1_from_year(int32_t year);
void    _ion_timestamp_get_year_and_jan1_from_julian_day(int32_t day, int32_t *p_year, int32_t *p_jan_1_julian);

int     _ion_timestamp_julian_day0_from_year_using_list(int32_t julian_day, int low, int high);

iERR    _ion_timestamp_append_time_to_date(iTIMESTAMP ptime, int hours, int minutes, decQuad *p_seconds, decContext *pcontext);

iERR _ion_timestamp_to_string_int   (int value, int32_t width, char *start, char *end_of_buffer);
iERR _ion_timestamp_copy_to_buf     (char *dst, char *src, char *end_of_buffer, int*p_copied);

iERR _ion_timestamp_parse_int       (int *p_value, int32_t width, int terminator, char *cp, char *end_of_buffer);

int  ion_timestamp_binary_len( ION_TIMESTAMP *ptime, decContext *context );
iERR ion_timestamp_binary_read( ION_STREAM *pstream, int32_t len, decContext *context, ION_TIMESTAMP *p_value );
iERR ion_timestamp_binary_write( ION_STREAM *pstream,  ION_TIMESTAMP *value, decContext *context );

/** Initialize to null value.
 *
 */
iERR _ion_timestamp_initialize(iTIMESTAMP ptime);

/** fraction = value / scale.
 *
 */
iERR _ion_timestamp_get_dec_fraction_with_scale(iTIMESTAMP ptime, int32_t scale,
        int32_t value, decQuad *decRetValue, decContext *pcontext);

/** value = fraction * scale.
 *
 */
iERR _ion_timestamp_get_fraction_with_scale(iTIMESTAMP ptime, int32_t scale,
        int32_t *value, decContext *pcontext);

iERR _ion_timestamp_equals_helper(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2,
                                  BOOL *is_equal, decContext *pcontext, BOOL instant_only);

#ifdef __cplusplus
}
#endif

#endif /* ION_TIMESTAMP_IMPL_H_ */
