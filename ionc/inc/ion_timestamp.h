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

/**@file */

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

#ifndef ION_TIMESTAMP_H_
#define ION_TIMESTAMP_H_

#include "ion_types.h"
#include "ion_platform_config.h"
#include "time.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Structure to store time information.
 * time_t only has up to second precision, and time zone support is OS dependent.
 * _ion_timestamp uses decimal to store fraction of a second
 */
struct _ion_timestamp {
    /** Defined as ION_TS_YEAR, ION_TS_MONTH, ION_TS_DAY, ION_TS_MIN, ION_TS_SEC, ION_TS_FRAC
     *
     */
    uint8_t     precision;

    /** Time zone offset (+/- 24 hours), in term of minutes.
     *
     */
    int16_t     tz_offset;
    uint16_t    year, month, day;
    uint16_t    hours, minutes, seconds;

    /** Fraction of a second, e.g: 0.5, 0.01, etc
     *
     */
    decQuad     fraction;
};

#define ION_TT_BIT_YEAR  0x01
#define ION_TT_BIT_MONTH 0x02
#define ION_TT_BIT_DAY   0x04
#define ION_TT_BIT_MIN   0x10
#define ION_TT_BIT_SEC   0x20 /* with secs must have time & date */
#define ION_TT_BIT_FRAC  0x40 /* must have all */

#define ION_TS_YEAR      (0x0         | ION_TT_BIT_YEAR)
#define ION_TS_MONTH     (ION_TS_YEAR | ION_TT_BIT_MONTH)
#define ION_TS_DAY       (ION_TS_MONTH  | ION_TT_BIT_DAY)
#define ION_TS_MIN       (ION_TS_DAY  | ION_TT_BIT_MIN)
#define ION_TS_SEC       (ION_TS_MIN  | ION_TT_BIT_SEC)
#define ION_TS_FRAC      (ION_TS_SEC | ION_TT_BIT_FRAC)

#define ION_MAX_TIMESTAMP_STRING (26+DECQUAD_String) /* y-m-dTh:m:s.<dec>+h:m */ // TODO there is another definition of a similar constant in ion_debug.h that is shorter. Investigate.

/** Get the time precision for the given timestamp object.
 * The precision values are defined as ION_TS_YEAR, ION_TS_MONTH, ION_TS_DAY,
 * ION_TS_MIN, ION_TS_SEC and ION_TS_FRAC
 *
 */
ION_API_EXPORT iERR ion_timestamp_get_precision(const ION_TIMESTAMP *ptime, int *precision);

/** Get the string format of timestamp.
 *
 */
ION_API_EXPORT iERR ion_timestamp_to_string(ION_TIMESTAMP *ptime, char *buffer, SIZE buf_length,
        SIZE *output_length, decContext *pcontext);

/** Parse timestamp string and construct timestamp object in ptime.
 *  This expects a null terminated string.
 *
 */
ION_API_EXPORT iERR ion_timestamp_parse(ION_TIMESTAMP *ptime, char *buffer, SIZE length,
        SIZE *p_characters_used, decContext *pcontext);

/** Initialize ION_TIMESTAMP object with value specified in time_t
 * time_t can be constructed using time() or mktime(), timegm,
 * and it contains ION_TS_SEC precision.
 *
 * Higher precision fields (fraction) will be set to 0.
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_time_t(ION_TIMESTAMP *ptime, const time_t *time);

/** Fill time_t with value in ION_TIMESTAMP
 *
 */
ION_API_EXPORT iERR ion_timestamp_to_time_t(const ION_TIMESTAMP *ptime, time_t *time);

/** Comparing two timestamps to see whether they represent the same point in time.
 * Two timestamp of different precision will return false
 */
ION_API_EXPORT iERR ion_timestamp_equals(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2,
        BOOL *is_equal, decContext *pcontext);

/** Compare timestamps for instant equality only (i.e. precision and local offsets need not be equivalent).
 *  NOTE: if this has any use externally, it could be exposed. If not, it should be removed.
 */
ION_API_EXPORT iERR ion_timestamp_instant_equals(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2,
                                  BOOL *is_equal, decContext *pcontext);

/** Initialize ION_TIMESTAMP object with value specified.
 * It will have ION_TS_YEAR precision
 *
 * Higher precision fields (month, day, min, sec, fraction) will be set to 0.
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_year(ION_TIMESTAMP *ptime,
        int year);

/** Initialize ION_TIMESTAMP object with value specified.
 * It will have ION_TS_MONTH precision
 *
 * Higher precision fields (day, min, sec, fraction) will be set to 0.
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_month(ION_TIMESTAMP *ptime,
        int year, int month);

/** Initialize ION_TIMESTAMP object with value specified.
 * It will have ION_TS_DAY precision
 *
 * Higher precision fields (min, sec, fraction) will be set to 0.
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_day(ION_TIMESTAMP *ptime,
        int year, int month, int day);

/** Initialize ION_TIMESTAMP object with value specified.
 * It will have ION_TS_MIN precision
 *
 * Higher precision fields (sec, fraction) will be set to 0.
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_minute(ION_TIMESTAMP *ptime,
        int year, int month, int day, int hours, int minutes);

/** Initialize ION_TIMESTAMP object with value specified.
 * It will have ION_TS_SEC precision
 *
 * Higher precision field (fraction) will be set to 0.
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_second(ION_TIMESTAMP *ptime,
        int year, int month, int day, int hours, int minutes, int seconds);

/** Initialize ION_TIMESTAMP object with value specified.
 * *p_fraction have a range of (0, 1), not including 0 (without significant digit) and 1.
 * 0.0 (0d-1), 0.00(0d-2) is valid, while 0 or 0. is not.
 *
 * It will have ION_TS_FRAC precision
 *
 * Local time zone will be cleared. ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 */
ION_API_EXPORT iERR ion_timestamp_for_fraction(ION_TIMESTAMP *ptime,
        int year, int month, int day, int hours, int minutes, int seconds,
        decQuad *p_fraction, decContext *pcontext);

/** Get year
 */
ION_API_EXPORT iERR ion_timestamp_get_thru_year(ION_TIMESTAMP *ptime, int *p_year);

/** Get year, month
 *  If precision is not up to month, 0 will be returned as month value.
 */
ION_API_EXPORT iERR ion_timestamp_get_thru_month(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month);

/** Get year, month, day
 *  If precision is not up to month/day, 0 will be returned as month/day value.
 */
ION_API_EXPORT iERR ion_timestamp_get_thru_day(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day);

/** Get time up to minute precision.
 *  If precision is not up to hour/minute, 0 will be returned as hour/minute value.
 */
ION_API_EXPORT iERR ion_timestamp_get_thru_minute(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day, int *p_hour, int *p_minute);

/** Get time up to second precision.
 *  If precision is not up to hour/minute/second, 0 will be returned as hour/minute/second value.
 */
ION_API_EXPORT iERR ion_timestamp_get_thru_second(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day, int *p_hour, int *p_minute, int *p_second);

/** Get time up to fraction of second precision.
 *  If precision is not up to fraction, 0 will be returned.
 */
ION_API_EXPORT iERR ion_timestamp_get_thru_fraction(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day, int *p_hour, int *p_minute, int *p_second,
        decQuad *p_fraction);

/**
 * Determines whether a timestamp has a defined local offset (for example,
 * "+08:00" or "Z".  Otherwise, it's local offest is unknown ("-00:00"), and
 * effectively zero.
 *
 * @param ptime the timestamp to inspect.
 * @param p_has_local_offset the return value; false when the local offset is
 *   unknown.
 *
 * @return IERR_INVALID_ARG if any parameter is null.
 */
ION_API_EXPORT iERR ion_timestamp_has_local_offset(ION_TIMESTAMP *ptime, BOOL *p_has_local_offset);

/**
 * Gets the effective local offset of a timestamp.  The result is zero for
 * timestamps with offsets "Z", "+00:00", or "-00:00".  In other words, if
 * ion_timestamp_has_local_offset returns false, this returns zero.
 *
 * @param ptime the timestamp to inspect.
 * @param p_local_offset the return value, in minutes from GMT;
 *   zero when the local offset is unknown.
 *
 * @return IERR_INVALID_ARG if any parameter is null;
 *   IERR_NULL_VALUE if the timestamp is null.timestamp.
 */
ION_API_EXPORT iERR ion_timestamp_get_local_offset(ION_TIMESTAMP *ptime, int *p_offset_minutes);

/**
 * Removes any local offset from a timestamp.
 * Afterwards, ion_timestamp_has_local_offset will be false, and
 * ion_timestamp_get_local_offset will be zero.
 *
 * @param ptime the timestamp to alter.
 *
 * @return IERR_INVALID_ARG if any parameter is null.
 */
ION_API_EXPORT iERR ion_timestamp_unset_local_offset(ION_TIMESTAMP *ptime);

/**
 * Changes the local offset of a timestamp. If the timestamp has less than minute precision,
 * the given offset is ignored and the timestamp is unchanged.
 * If the timestamp is changed, ion_timestamp_has_local_offset will be true, and
 * ion_timestamp_get_local_offset will be the given offset.
 *
 * @param ptime the timestamp to alter.
 * @param offset_minutes the new local offset, in (positive or negative)
 *   minutes from GMT.
 *
 * @return IERR_INVALID_ARG if ptime is null, or if offset_minutes is outside
 * the range -23:59 to +23:59.
 */
ION_API_EXPORT iERR ion_timestamp_set_local_offset(ION_TIMESTAMP *ptime, int offset_minutes);

#ifdef __cplusplus
}
#endif

#endif
