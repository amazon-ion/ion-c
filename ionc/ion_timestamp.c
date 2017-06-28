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
// routines for handing ion timestamps in C
//

#include "ion_internal.h"
#include "ion_decimal_impl.h"

#ifdef ION_PLATFORM_ANDROID
// Taken from http://www.kernel.org/doc/man-pages/online/pages/man3/timegm.3.html
time_t
timegm(struct tm *tm)
{
    time_t ret;
    char *tz;
    
    tz = getenv("TZ");
    setenv("TZ", "", 1);
    tzset();
    ret = mktime(tm);
    if (tz)
        setenv("TZ", tz, 1);
    else
        unsetenv("TZ");
    tzset();
    return ret;
}
#endif

iERR ion_timestamp_for_time_t(ION_TIMESTAMP *ptime, const time_t *time)
{
    iENTER;
    struct tm tm;

    if (!ptime) FAILWITH(IERR_INVALID_ARG);
    if (!time) FAILWITH(IERR_INVALID_ARG);

#ifdef ION_PLATFORM_WINDOWS
    if (_gmtime64_s(&tm, time)) {
      FAILWITH(IERR_INVALID_ARG);
    }
#else
    if (gmtime_r(time, &tm) == NULL) {
        FAILWITH(IERR_INVALID_ARG);
    }
#endif

    _ion_timestamp_initialize(ptime);

    ptime->tz_offset= 0;
    ptime->year     = 1900 + tm.tm_year;   // tm_year: year since 1900
    ptime->month    = 1 + tm.tm_mon;       // tm_mon: month (0 – 11, 0 = January)
    ptime->day      = tm.tm_mday;          // tm_mday: day of the month (1 – 31)
    ptime->hours    = tm.tm_hour;          // tm_hour: hour (0 – 23)
    ptime->minutes  = tm.tm_min;           // tm_min: minutes (0 – 59)
    ptime->seconds  = tm.tm_sec;           // tm_sec: seconds (0 – 60, 60 = Leap second)

    // here we have a precision to seconds and not timezone
    SET_FLAG_ON(ptime->precision, ION_TS_SEC);
    SET_FLAG_OFF(ptime->precision, ION_TT_BIT_TZ);

    iRETURN;
}

iERR ion_timestamp_to_time_t(const ION_TIMESTAMP *ptime, time_t *time)
{
    iENTER;
    struct tm tm;

    if (!ptime) FAILWITH(IERR_INVALID_ARG);
    if (!time) FAILWITH(IERR_INVALID_ARG);

    memset(&tm, 0, sizeof(struct tm));

    tm.tm_year = ptime->year - 1900;   //  tm_year: year since 1900
    tm.tm_mon = ptime->month -1;       // tm_mon: month (0 – 11, 0 = January)
    tm.tm_mday = ptime->day;           // tm_mday: day of the month (1 – 31)
    tm.tm_hour = ptime->hours;         // tm_hour: hour (0 – 23)
    tm.tm_min = ptime->minutes;        // tm_min: minutes (0 – 59)
    tm.tm_sec = ptime->seconds;        // tm_sec: seconds (0 – 60, 60 = Leap second)
#ifndef ION_PLATFORM_WINDOWS
    tm.tm_zone = "GMT";
#endif

    if (HAS_TZ_OFFSET(ptime)) {
        tm.tm_min += (ptime->tz_offset);
    }

#ifndef ION_PLATFORM_WINDOWS
    // TODO ION-85 timegm() fails for timestamps before the Unix epoch.
    // It is also unportable!
    // See http://www.kernel.org/doc/man-pages/online/pages/man3/timegm.3.html
    *time = timegm(&tm);
#else
    *time = _mktime64(&tm);
#endif
    if (*time == -1) {
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }

    iRETURN;
}

iERR ion_timestamp_get_precision(const ION_TIMESTAMP *ptime, int *precision) {
    iENTER;

    if (!ptime) FAILWITH(IERR_INVALID_ARG);
    *precision = (ptime->precision & 0x7f);

    iRETURN;
}

iERR _ion_timestamp_initialize(ION_TIMESTAMP *ptime)
{
    iENTER;

    memset(ptime, 0, sizeof(ION_TIMESTAMP));
    decQuadZero(&ptime->fraction);  // this may not be necessary as zero is all bits zero IIRC
    SUCCEED();

    iRETURN;
}

iERR ion_timestamp_to_string(ION_TIMESTAMP *ptime, char *buffer, SIZE buf_length, SIZE *p_length_written, decContext *pcontext)
{
    iENTER;
    char   *pos = buffer;
    char   *end_of_buffer = pos + buf_length;
    char    temp[DECQUAD_String], *cp;
    int     offset, offset_hours, offset_mins, count, i;

    if (!buffer)         FAILWITH(IERR_INVALID_ARG);
    if ( buf_length < 1) FAILWITH(IERR_BUFFER_TOO_SMALL);
    if (!pcontext)       FAILWITH(IERR_INVALID_ARG);

    // options are: 
    //     date only - if no timezone and time == 00:00:00.000 (not further digits)
    //     date+time to seconds, if even seconds and 0's to decimal
    //     date+time.fraction
    //     with timezone offset if present

    // if it's null we output "null.timestamp"
    if (NULL == ptime) {
        IONCHECK(_ion_timestamp_copy_to_buf(pos, ION_TIMESTAMP_NULL_IMAGE, end_of_buffer, p_length_written));
        SUCCEED();
    }

    // if it's not null, we get out raw values
    IONCHECK(ion_timestamp_get_local_offset(ptime, &offset));

    // first we output the date, since we always have a date
    IONCHECK(_ion_timestamp_to_string_int(ptime->year, 4, pos, end_of_buffer));
    pos += 4;

    // if this is the end of the value, it's a loose year
    if (!IS_FLAG_ON(ptime->precision, ION_TT_BIT_MONTH)) {
        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = 'T';
        goto end_of_days;
    }

    // a dash before the month
    if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
    *pos++ = '-';


    IONCHECK(_ion_timestamp_to_string_int(ptime->month, 2, pos, end_of_buffer));
    pos += 2;

    // if this is the end of the value, it's a year-month timestamp
    if (!IS_FLAG_ON(ptime->precision, ION_TT_BIT_DAY)) {
        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = 'T';
        goto end_of_days;
    }

    // otherwise we need a dash before the day
    if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
    *pos++ = '-';


    IONCHECK(_ion_timestamp_to_string_int(ptime->day, 2, pos, end_of_buffer));
    pos += 2;

    // now we need to check the precision to see if there's time
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MIN)) {

        // output the time, starting with the 'T' and hours and minutes
        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = 'T';
        IONCHECK(_ion_timestamp_to_string_int(ptime->hours, 2, pos, end_of_buffer));
        pos += 2;
        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = ':';
        IONCHECK(_ion_timestamp_to_string_int(ptime->minutes, 2, pos, end_of_buffer));
        pos += 2;

        if (!IS_FLAG_ON(ptime->precision, ION_TT_BIT_SEC)) {
            goto end_of_days;
        }
        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = ':';
        IONCHECK(_ion_timestamp_to_string_int(ptime->seconds, 2, pos, end_of_buffer));
        pos += 2;

        if (!IS_FLAG_ON(ptime->precision, ION_TT_BIT_FRAC)) {
            goto end_of_days;
        }
        decQuadToString(&ptime->fraction, temp);
        cp = temp;
        if (decQuadIsSigned(&ptime->fraction)) {
            if (decQuadIsZero(&ptime->fraction)) {
                // Negative-zero fractional seconds are normalized to positive-zero.
                // NOTE: the binary reader will normalize negative-zero to positive-zero, but this protects
                // against the user manually constructing an ION_TIMESTAMP with negative-zero fractional seconds.
                ASSERT((*cp) == '-');
                cp++;
            }
            else {
                // Any other negative besides negative-zero is an error.
                FAILWITH(IERR_INVALID_BINARY);
            }
        }
        while (*cp == '0') cp++;  /* there should only be one '0' */
        if ((*cp) == '.') {
            IONCHECK(_ion_timestamp_copy_to_buf(pos, cp, end_of_buffer, &count));
        }
        else if ((*cp) == 'E' || (*cp) == 'e') {
            // This fraction is of the form 0E-N
            IONCHECK(_ion_timestamp_copy_to_buf(pos, ".", end_of_buffer, &count));
            pos += count;
            if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
            for (i = 0; i < -decQuadGetExponent(&ptime->fraction); i++) {
                IONCHECK(_ion_timestamp_copy_to_buf(pos, "0", end_of_buffer, &count));
                pos += count;
                if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
            }
            count = 0;
        }
        else if (decQuadGetExponent(&ptime->fraction) >= 0) {
            if (!decQuadIsZero(&ptime->fraction)) FAILWITH(IERR_INVALID_TIMESTAMP);
            // Fractions with zero coefficient and >= 0 exponent are ignored.
            count = 0;
        }
        else {
            FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Invalid fraction value");
        }
        pos += count;
    }

end_of_days:
    if (!HAS_TZ_OFFSET(ptime)) {
        if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MIN)) {
            IONCHECK(_ion_timestamp_copy_to_buf(pos, ION_TIMESTAMP_NULL_OFFSET_IMAGE, end_of_buffer, &count));
            pos += count;
        }
    }
    else if (offset == 0) {
        // zulu time, really +0 buf shorter
        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = 'Z';
    }
    else {
        // offset format is +/-HH:MM
        if (offset > 0) {
            if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
            *pos++ = '+';
        }
        else {
            if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
            *pos++ = '-';
            offset = -offset;
        }
        offset_hours = offset / 60;
        offset_mins  = offset - 60 * offset_hours;
        IONCHECK(_ion_timestamp_to_string_int(offset_hours, 2, pos, end_of_buffer));
        pos += 2;

        if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        *pos++ = ':';

        IONCHECK(_ion_timestamp_to_string_int(offset_mins, 2, pos, end_of_buffer));
        pos += 2;
    }

    // null terminate the string
    if (pos >= end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
    *pos = '\0';

    // finally, calculate how much we wrote into the buffer
    if (p_length_written) *p_length_written = (SIZE)(pos - buffer);   // TODO - this needs 64bit care

    iRETURN;
}

iERR _ion_timestamp_to_string_int(int32_t value, int32_t width, char *start, char *end_of_buffer)
{
    iENTER;

    char  *cp;
    char  *pos = start;
    char   temp_buffer[MAX_INT32_LENGTH];
    int32_t len, copied;

    if (width > (sizeof(temp_buffer)-1)) FAILWITH(IERR_INVALID_ARG);
    if (!start)    FAILWITH(IERR_INVALID_ARG);
    if (value < 0) FAILWITH(IERR_INVALID_ARG);

    switch (width) {
    case 2:
        if (value > 99) FAILWITH(IERR_INVALID_ARG);
        break;
    case 4:
        if (value > 9999) FAILWITH(IERR_INVALID_ARG);
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    // let the c library do the "hard" work
    cp = _ion_itoa_10(value, temp_buffer, sizeof(temp_buffer));
    len = (int32_t)strlen(cp);

    // add the leading zero's here
    if (len < width) {
        if (pos + (width - len) > end_of_buffer) FAILWITH(IERR_BUFFER_TOO_SMALL);
        memset(pos, '0', (width - len));
        pos += (width - len);
    }

    // now write in the digits
    IONCHECK(_ion_timestamp_copy_to_buf(pos, cp, end_of_buffer, &copied));

    // we always write width values (unless there's an error)
    // so there's no return value here

    iRETURN;
}

iERR _ion_timestamp_copy_to_buf(char *dst, char *src, char *end_of_buffer, int *p_copied)
{
    iENTER;
    char *cp = src;
    
    // copy until end of src or end of buffer
    while (*cp && dst < end_of_buffer) {
        *dst++ = *cp++;
    }

    // if our output buffer is big enough we should get to the null 
    // terminator before we run out of buffer
    if (*cp) FAILWITH(IERR_BUFFER_TOO_SMALL);

    // tell them how much we actually copied
    *p_copied = (int)(cp - src);

    iRETURN;
}

// validates the given day against the given year and month -- assumes year and month are valid
static BOOL _ion_timestamp_is_valid_day(int year, int one_based_month, int day)
{
    BOOL is_leapyear;

    // make sure within the right bounds
    if (day < 1 || day > 31) {
        return FALSE;
    }

    if (one_based_month < 1 || one_based_month > 12) {
        return FALSE;
    }

    // now check the day value ... closely
    is_leapyear = _ion_timestamp_is_leap_year(year);
    int zero_based_month = one_based_month - 1;
    if (day > JULIAN_DAY_PER_MONTH[is_leapyear][zero_based_month]){
        return FALSE;
    }

    // we're golden!
    return TRUE;
}

// this expects a null terminated string
iERR ion_timestamp_parse(ION_TIMESTAMP *ptime, char *buffer, SIZE buf_length, SIZE *p_chars_used, decContext *pcontext)
{
    iENTER;

    char   *cp = buffer;
    char   *end_of_buffer = buffer + buf_length;
    char   *pni, *dst;
    char    temp[DECQUAD_String];
    char   *end_of_temp = temp + DECQUAD_String;
    BOOL    is_negative;

    int     precision = 0;
    int     year = -1, month = -1, day = -1;
    int     hours = -1, minutes = -1;
    int     seconds = -1;
    decQuad fraction;
    int     offset_hours, offset_mins, offset = -99999;

    if (!ptime)         FAILWITH(IERR_INVALID_ARG);
    if (!buffer)        FAILWITH(IERR_INVALID_ARG);
    if (buf_length < 1) FAILWITH(IERR_INVALID_ARG);

    // zero out the passed in time buffer
    IONCHECK(_ion_timestamp_initialize(ptime));

    // first check for a "null.timestamp"
    if (*buffer == 'n') {
        // first see if there are enough characters at all
        if (cp + ION_TIMESTAMP_NULL_IMAGE_LEN >= end_of_buffer) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        // now see if they are the right characters
        pni = ION_TIMESTAMP_NULL_IMAGE;
        while (*pni) {
            if (*pni++ != *cp++) FAILWITH(IERR_INVALID_TIMESTAMP);
        }

        // so it's null and we have to have read IERR_INVALID_TIMESTAMP chars
        precision = ION_TS_NULL;
    }
    else {
        precision = ION_TS_YEAR;

        // first we extract the year, month and day
        // we don't care here what the values are just yet
        // since when put them altogether the called routines
        // will check the for us (and consider leap years and such)
        IONCHECK(_ion_timestamp_parse_int(&year,  4, 0, cp, end_of_buffer));
        if (year == 0) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        cp += 4;
        if (cp >= end_of_buffer) {
            // if this just 4 digits there better be a year - this is a system failure case
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        else if (*cp == 'T') {
            cp++;        
            month = day = 1;
            offset = 0;
            goto end_of_days;
        }
        else if (*cp != '-') {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        cp++;  // eat the '-'

        SET_FLAG_ON(precision, ION_TS_MONTH);
        IONCHECK(_ion_timestamp_parse_int(&month, 2, 0, cp, end_of_buffer));
        cp += 2;
        if (cp >= end_of_buffer) {
            // you can't stop at just yyyy-mm you have to have a 'T' or a day
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        else if (*cp == 'T') {
            cp++;
            day = 1;
            offset = 0;
            goto end_of_days;
        }
        else if (*cp != '-') {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        cp++;

        SET_FLAG_ON(precision, ION_TS_DAY);
        IONCHECK(_ion_timestamp_parse_int(&day, 2,  0, cp, end_of_buffer));
        cp += 2;

        // we may not have anything more, so we'll "zero" out
        // all the time values and set them as we see them
        hours = 0;
        minutes = 0;
        seconds = 0;
        decQuadZero(&fraction);

        if (cp >= end_of_buffer || *cp != 'T')  goto end_of_time;
        cp++; // eat the T - we have time

        // it's ok to have yyyy-mm-ddT as input (the 'T' is optional in this case)
        if (cp >= end_of_buffer)  goto end_of_time;

        // since we have a time - get the value(s) out
        SET_FLAG_ON(precision, ION_TT_BIT_MIN);
        IONCHECK(_ion_timestamp_parse_int(&hours, 2, 0, cp, end_of_buffer));
        cp += 2;
        if (cp >= end_of_buffer || *cp != ':')  FAILWITH(IERR_INVALID_TIMESTAMP);
        cp++; // eat the ':'
        IONCHECK(_ion_timestamp_parse_int(&minutes, 2, 0, cp, end_of_buffer));
        cp += 2;

        if (cp >= end_of_buffer || *cp != ':')  goto end_of_time;
        cp++;  // we don't need the colon any longer

        // and we have seconds - so here we transfer them to a local
        // buffer and let the decimal package do the rest of the work
        SET_FLAG_ON(precision, ION_TS_SEC);
        IONCHECK(_ion_timestamp_parse_int(&seconds, 2, 0, cp, end_of_buffer));
        cp += 2;

        if (cp >= end_of_buffer || *cp != '.')  goto end_of_time;

        // we have a fractional seconds in it
        SET_FLAG_ON(precision, ION_TS_FRAC);
        temp[0] = '0';
        temp[1] = '.';
        int fractional_digits_read = 0;
        for (dst=temp+2,cp++; (cp < end_of_buffer) && isdigit(*cp); dst++, cp++, fractional_digits_read++) {
            if (dst >= end_of_temp) FAILWITH(IERR_INVALID_TIMESTAMP);
            *dst = *cp;
        }
        if (fractional_digits_read == 0) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        if (cp > end_of_buffer) FAILWITH(IERR_INVALID_TIMESTAMP);

        *dst = 0; // null terminate the string (it's why we copied it after all)
        // TODO timestamp fraction as ION_DECIMAL to support full precision?
        IONCHECK(_ion_decimal_from_string_helper(temp, pcontext, NULL, &fraction, NULL));


end_of_time:
        // check for an offset - we'll start with it as zero in all cases anyway
        if (cp >= end_of_buffer) {
            if (IS_FLAG_ON(precision, ION_TT_BIT_MIN)) FAILWITH(IERR_INVALID_TIMESTAMP);
            SET_FLAG_OFF(precision, ION_TT_BIT_TZ);
            offset = 0;
        }
        else if (*cp == 'Z') {
            if ((precision & ION_TS_SUB_DATE) == 0) {
                // none of the sub-date precision flags are set, this is illegal
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            // zulu, the "official" not-a-timezone timezone
            SET_FLAG_ON(precision, ION_TT_BIT_TZ);
            offset = 0;
            // we don't need the 'Z' any longer
            cp++;
        }
        else if (*cp == '+' || *cp == '-') {
            if ((precision & ION_TS_SUB_DATE) == 0) {
                // none of the sub-date precision flags are set, this is illegal
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            SET_FLAG_ON(precision, ION_TT_BIT_TZ);
            // handle the sign
            is_negative = (*cp == '-');
            cp++;

            IONCHECK(_ion_timestamp_parse_int(&offset_hours, 2, ':', cp, end_of_buffer));
            cp += 3;
            if (offset_hours >= 24) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }

            if (cp >= end_of_buffer) {
                // no minutes
                offset_mins = 0;
            }
            else {
                IONCHECK(_ion_timestamp_parse_int(&offset_mins, 2, 0, cp, end_of_buffer));
                cp += 2;
                if (offset_mins >= 60) {
                    FAILWITH(IERR_INVALID_TIMESTAMP);
                }
            }
            offset = offset_hours * 60 + offset_mins;

            if (is_negative) {
                if (offset == 0) {
                    // negative 0 offset - i.e. no offset (but GMT otherwise)
                    SET_FLAG_OFF(precision, ION_TT_BIT_TZ);
                }
                else {
                    offset = -offset;
                }
            }
        }

    }

end_of_days:
    // check for one of our valid timestamp termination characters
    switch (*cp) {
    case NEW_LINE_1:
    case NEW_LINE_2:
    case NEW_LINE_3:
    case   0:
    case ' ': case '\t': case '\n': case '\r':
    case ',': case  '"': case '\'':
    case '(': case ')': 
    case '[': case ']': 
    case '{': case '}':
    case '/':
        *p_chars_used = (SIZE)(cp - buffer);    // TODO - this needs 64bit care
        break;
    default:
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }

    // now we have put it all together into a real timestamp
    IONCHECK(_ion_timestamp_initialize(ptime));
    ptime->precision = precision;
    if (precision != ION_TS_NULL)
    {
        ptime->tz_offset = offset;
        ptime->year      = year;
        if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MONTH)) {
            if (month < 1 || month > 12) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            ptime->month     = month;
        }
        else {
            ptime->month     = 1;
        }
        if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_DAY)) {
            if (!_ion_timestamp_is_valid_day(year, month, day)) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            ptime->day       = day;
        }
        else {
            ptime->day       = 1;
        }
        if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MIN)) {
            if (hours < 0 || hours > 23) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            if (minutes < 0 || minutes > 59) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            ptime->hours     = hours;
            ptime->minutes   = minutes;
        }
        else {
            ptime->hours     = 0;
            ptime->minutes   = 0;
        }
        if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_SEC)) {
            if (seconds < 0 || seconds > 59) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            ptime->seconds   = seconds;
        }
        else {
            ptime->seconds   = 0;
        }
        if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_FRAC)) {
            decQuadCopy(&ptime->fraction, &fraction);  // TODO timestamp fraction as ION_DECIMAL to support full precision?
        }
        // No need to zero ptime->fraction here -- that is taken care of by _ion_timestamp_initialize.
    }

    iRETURN;
}

iERR _ion_timestamp_parse_int(int *p_value, int32_t width, int terminator, char *cp, char *end_of_buffer)
{
    iENTER;
    int value = 0;

    if (!p_value) FAILWITH(IERR_INVALID_ARG);
    if ((end_of_buffer - cp) < (width + ((terminator > 0) ? 1 : 0))) FAILWITH(IERR_INVALID_TIMESTAMP);

    while (width--) {
        if (!isdigit(*cp)) FAILWITH(IERR_INVALID_TIMESTAMP);
        value *= 10;
        value += *cp - '0';
        cp++;
    }

    if (terminator && *cp != terminator) FAILWITH(IERR_INVALID_TIMESTAMP);

    // gotta pass it back up, for sure
    *p_value = value;
    iRETURN;
}

iERR ion_timestamp_for_year(ION_TIMESTAMP *ptime, int year)
{
    iENTER;

    if (!ptime) FAILWITH(IERR_INVALID_ARG);
    if (year  < 1 || year  > 9999) FAILWITH(IERR_INVALID_ARG);

    // note that by initializing the value we destroy the timezone
    IONCHECK(_ion_timestamp_initialize(ptime));
    ptime->year      = year;
    // here we have a precision to seconds and not timezone
    SET_FLAG_ON(ptime->precision, ION_TS_YEAR);
    SET_FLAG_OFF(ptime->precision, ION_TT_BIT_TZ);

    iRETURN;
}

iERR ion_timestamp_for_month(ION_TIMESTAMP *ptime, int year, int month)
{
    iENTER;

    IONCHECK(ion_timestamp_for_year(ptime, year));
    if (month < 1 || month >   12) FAILWITH(IERR_INVALID_ARG);

    ptime->month     = month;
    // here we have a precision to seconds and not timezone
    SET_FLAG_ON(ptime->precision, ION_TS_MONTH);

    iRETURN;
}

iERR ion_timestamp_for_day(ION_TIMESTAMP *ptime, int year, int month, int day)
{
    iENTER;
    
    IONCHECK(ion_timestamp_for_month(ptime, year, month));
    
    // make sure the day aligns with the calendar month
    if (!_ion_timestamp_is_valid_day(year, month, day)) {
        FAILWITH(IERR_INVALID_ARG);
    }
    
    // we're good - store it all away
    ptime->day       = day;
    // here we have a precision to seconds and not timezone
    SET_FLAG_ON(ptime->precision, ION_TS_DAY);

    iRETURN;
}

iERR ion_timestamp_for_minute(ION_TIMESTAMP *ptime,
        int year, int month, int day, int hours, int minutes)
{
    iENTER;

    IONCHECK(ion_timestamp_for_day(ptime, year, month, day));
    if (hours   < 0 || hours   > 24) FAILWITH(IERR_INVALID_ARG);
    if (minutes < 0 || minutes > 59) FAILWITH(IERR_INVALID_ARG);
    ptime->hours = hours;
    ptime->minutes = minutes;
    SET_FLAG_ON(ptime->precision, ION_TS_MIN);

    iRETURN;
}

iERR ion_timestamp_for_second(ION_TIMESTAMP *ptime,
        int year, int month, int day, int hours, int minutes, int seconds)
{
    iENTER;

    IONCHECK(ion_timestamp_for_minute(ptime, year, month, day, hours, minutes));
    if (seconds < 0 || seconds > 59) FAILWITH(IERR_INVALID_ARG);   // that's right, no leap seconds here !
    ptime->seconds = seconds;
    SET_FLAG_ON(ptime->precision, ION_TS_SEC);

    iRETURN;
}

iERR ion_timestamp_for_fraction(ION_TIMESTAMP *ptime,
        int year, int month, int day, int hours, int minutes, int seconds,
        decQuad *p_fraction, decContext *pcontext)
{
    iENTER;
    decQuad decResult, decQuad_1;

    IONCHECK(ion_timestamp_for_second(ptime, year, month, day, hours, minutes, seconds));

    if (p_fraction != NULL) {
        if (decQuadIsSigned(p_fraction))  FAILWITHMSG(IERR_INVALID_ARG, "fractional seconds can't be negative");

        decQuadFromInt32(&decQuad_1, 1);

        // Numeric value comparison: 1.0 == 1
        decQuadCompare(&decResult, p_fraction, &decQuad_1, pcontext);
        // Make sure fraction is < 1
        if (!decQuadIsSigned(&decResult)) {
            FAILWITHMSG(IERR_INVALID_ARG, "fraction seconds can't be greater than 1");
        }

        // Make sure fraction is not true zero
        if (decQuadIsZero(p_fraction) && decQuadGetExponent(p_fraction) > -1) {
            FAILWITHMSG(IERR_INVALID_ARG, "fraction seconds must have significant digit. 0 or 0. is not valid.");
        }
        decQuadCopy(&ptime->fraction, p_fraction);
        SET_FLAG_ON(ptime->precision, ION_TS_FRAC);
    }

    iRETURN;
}

iERR ion_timestamp_has_local_offset(ION_TIMESTAMP *ptime, BOOL *p_has_local_offset)
{
    iENTER;

    if (!ptime || !p_has_local_offset) FAILWITH(IERR_INVALID_ARG);

    *p_has_local_offset = IS_FLAG_ON(ptime->precision, ION_TT_BIT_TZ);

    iRETURN;
}


iERR ion_timestamp_get_local_offset(ION_TIMESTAMP *ptime, int *p_local_offset)
{
    iENTER;

    if (!ptime || !p_local_offset) FAILWITH(IERR_INVALID_ARG);

    if (HAS_TZ_OFFSET(ptime)) {
        *p_local_offset = ptime->tz_offset;
    }
    else {
        // TODO maintain the invariant that tz_offset==0 in this case
        *p_local_offset = 0;
    }

    iRETURN;
}


iERR ion_timestamp_unset_local_offset(ION_TIMESTAMP *ptime)
{
    iENTER;

    if (!ptime) FAILWITH(IERR_INVALID_ARG);

    SET_FLAG_OFF(ptime->precision, ION_TT_BIT_TZ);
    ptime->tz_offset = 0;

    iRETURN;
}

iERR ion_timestamp_set_local_offset(ION_TIMESTAMP *ptime, int offset_minutes)
{
    iENTER;

    if (!ptime) FAILWITH(IERR_INVALID_ARG);
    if (offset_minutes <= -24*60) FAILWITH(IERR_INVALID_ARG);
    if (offset_minutes >=  24*60) FAILWITH(IERR_INVALID_ARG);

    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MIN)) {
        SET_FLAG_ON(ptime->precision, ION_TT_BIT_TZ);
        ptime->tz_offset = offset_minutes;
    }

    iRETURN;
}

iERR ion_timestamp_get_thru_year(ION_TIMESTAMP *ptime,
        int *p_year)
{
    iENTER;

    if (!ptime)              FAILWITH(IERR_INVALID_ARG);
    if (p_year)     *p_year = ptime->year;

    iRETURN;
}

iERR ion_timestamp_get_thru_month(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month)
{
    iENTER;

    IONCHECK(ion_timestamp_get_thru_year(ptime, p_year));
    if (p_month)    *p_month = ptime->month;

    iRETURN;
}

iERR ion_timestamp_get_thru_day(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day)
{
    iENTER;

    IONCHECK(ion_timestamp_get_thru_month(ptime, p_year, p_month));
    if (p_day)      *p_day = ptime->day;

    iRETURN;
}

iERR ion_timestamp_get_thru_minute(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day, int *p_hours, int *p_minutes)
{
    iENTER;

    IONCHECK(ion_timestamp_get_thru_day(ptime, p_year, p_month, p_day));
    if (p_hours)     *p_hours = ptime->hours;
    if (p_minutes)   *p_minutes = ptime->minutes;
    iRETURN;
}

iERR ion_timestamp_get_thru_second(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day, int *p_hours, int *p_minutes, int *p_seconds)
{
    iENTER;

    IONCHECK(ion_timestamp_get_thru_minute(ptime, p_year, p_month, p_day, p_hours, p_minutes));
    if (p_seconds)     *p_seconds = ptime->seconds;

    iRETURN;
}

iERR ion_timestamp_get_thru_fraction(ION_TIMESTAMP *ptime,
        int *p_year, int *p_month, int *p_day, int *p_hours, int *p_minutes, int *p_seconds,
        decQuad *p_fraction)
{
    iENTER;

    IONCHECK(ion_timestamp_get_thru_second(ptime, p_year, p_month, p_day, p_hours, p_minutes, p_seconds));
    if (p_fraction)  decQuadCopy(p_fraction, &ptime->fraction);

    iRETURN;
}

BOOL _ion_timestamp_is_leap_year(int y)
{
    BOOL is_leap_year = FALSE;

    // if it's not divisible by 4 - it's not a leap year
    if (!(y & 0x3)) {
        // it is divisible by 4 so ...
        if ((y % 100) == 0) {
            // if it's divisible by 100, it's probably not a leap year
            // unless it's divisble by 400 (woo hoo)
            // thanks to Aloysius Lilius for this bit of calendar magic
            is_leap_year = !(y % 400);
        }
        else {
            is_leap_year = TRUE;
        }
    }
    return is_leap_year;
}

iERR _ion_timestamp_to_utc(const ION_TIMESTAMP *ptime, ION_TIMESTAMP *pout) {
    iENTER;

    int8_t sign;
    uint16_t  min_shift, hour_shift = 0;
    int16_t offset;
    BOOL needs_day = FALSE;

    if (!ptime || !pout) FAILWITH(IERR_INVALID_ARG);

    offset = ptime->tz_offset;
    pout->year = ptime->year;
    pout->month = ptime->month;
    pout->day = ptime->day;
    pout->hours = ptime->hours;
    pout->minutes = ptime->minutes;
    pout->seconds = ptime->seconds;
    pout->precision = ptime->precision;
    pout->tz_offset = 0; // The result is in UTC.
    decQuadCopy(&pout->fraction, &ptime->fraction);

    if (!HAS_TZ_OFFSET(ptime)) SUCCEED();
    sign = (int8_t)((offset < 0) ? -1 : 1);
    min_shift = (uint16_t)(sign * offset);

    if (min_shift >= 60 * 24) {
        // Offsets must have a magnitude of less than one day.
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }
    if (min_shift >= 60) {
        hour_shift = (uint16_t)(min_shift / 60);
        min_shift = (uint16_t)(min_shift % 60);
    }

    pout->hours -= (sign * hour_shift);
    pout->minutes -= (sign * min_shift);

    if ((int16_t)pout->minutes < 0) {
        pout->hours -= 1;
        pout->minutes = (uint16_t)(pout->minutes + 60);
    }
    else if (pout->minutes > 59) {
        pout->hours += 1;
        pout->minutes = (uint16_t)(pout->minutes - 60);
    }

    if ((int16_t)pout->hours < 0) {
        pout->day -= 1;
        pout->hours = (uint16_t)(pout->hours + 24);
    }
    else if (pout->hours > 23) {
        pout->day += 1;
        pout->hours = (uint16_t)(pout->hours - 24);
    }
    else {
        SUCCEED();
    }

    if (pout->day < 1) {
        pout->month -= 1;
        // Defer assignment of new day until year is known.
        needs_day = TRUE;
    }
    else if (pout->day > JULIAN_DAY_PER_MONTH[_ion_timestamp_is_leap_year(pout->year)][pout->month - 1]) {
        pout->month += 1;
        pout->day = 1;
    }
    else {
        SUCCEED();
    }

    if (pout->month < 1) {
        pout->year -= 1;
        pout->month = 12;
    }
    else if (pout->month > 12) {
        pout->year += 1;
        pout->month = 1;
    }

    if (needs_day) {
        pout->day = (uint16_t)JULIAN_DAY_PER_MONTH[_ion_timestamp_is_leap_year(pout->year)][pout->month - 1];
    }

    if (pout->year < 1 || pout->year > 9999) {
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }

    iRETURN;
}

iERR ion_timestamp_equals(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2, BOOL *is_equal, decContext *pcontext)
{
    return _ion_timestamp_equals_helper(ptime1, ptime2, is_equal, pcontext, FALSE);
}

iERR ion_timestamp_instant_equals(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2, BOOL *is_equal, decContext *pcontext)
{
    return _ion_timestamp_equals_helper(ptime1, ptime2, is_equal, pcontext, TRUE);
}

iERR _ion_timestamp_equals_helper(const ION_TIMESTAMP *ptime1, const ION_TIMESTAMP *ptime2, BOOL *is_equal, decContext *pcontext, BOOL instant_only)
{
    iENTER;
    int             precision1, precision2;
    BOOL            decResult;
    decQuad         fraction_trimmed1, fraction_trimmed2;
    ION_TIMESTAMP   ptime1_compare, ptime2_compare;

    if (ptime1 == ptime2) {
        goto is_true;
    }
    if (ptime1 == NULL || ptime2 == NULL) {
        goto is_false;
    }

    if (instant_only) {
        // Fractional precision does not matter in this case. Trim any trailing zeros from the fractions, then compare.
        decQuadReduce(&fraction_trimmed1, &ptime1->fraction, pcontext);
        decQuadReduce(&fraction_trimmed2, &ptime2->fraction, pcontext);
        IONCHECK(ion_decimal_equals_quad(&fraction_trimmed1, &fraction_trimmed2, pcontext, &decResult));
        if (!decResult) goto is_false;
        IONCHECK(_ion_timestamp_initialize(&ptime1_compare));
        IONCHECK(_ion_timestamp_initialize(&ptime2_compare));
        IONCHECK(_ion_timestamp_to_utc(ptime1, &ptime1_compare));
        IONCHECK(_ion_timestamp_to_utc(ptime2, &ptime2_compare));
        if    ((ptime1_compare.year != ptime2_compare.year)
               || (ptime1_compare.month != ptime2_compare.month)
               || (ptime1_compare.day != ptime2_compare.day)
               || (ptime1_compare.hours != ptime2_compare.hours)
               || (ptime1_compare.minutes != ptime2_compare.minutes)
               || (ptime1_compare.seconds != ptime2_compare.seconds)
               || (ptime1_compare.tz_offset != ptime2_compare.tz_offset)
                ) {
            goto is_false;
        }
    }
    else {
        IONCHECK(ion_timestamp_get_precision(ptime1, &precision1));
        IONCHECK(ion_timestamp_get_precision(ptime2, &precision2));
        if (precision1 == ION_TS_FRAC && decQuadIsZero(&ptime1->fraction) && decQuadGetExponent(&ptime1->fraction) >= 0) {
            // Fractions with zero coefficient and >= zero exponent are ignored.
            precision1 = ION_TS_SEC;
        }
        if (precision2 == ION_TS_FRAC && decQuadIsZero(&ptime2->fraction) && decQuadGetExponent(&ptime2->fraction) >= 0) {
            precision2 = ION_TS_SEC;
        }
        if (precision1 != precision2) {
            goto is_false;
        }

        if (HAS_TZ_OFFSET(ptime1) ^ HAS_TZ_OFFSET(ptime2)) {
            goto is_false;
        }
        if (HAS_TZ_OFFSET(ptime1) && (ptime1->tz_offset != ptime2->tz_offset)) {
            goto is_false;
        }
        if (precision1 >= ION_TS_YEAR) {
            if (ptime1->year != ptime2->year) {
                goto is_false;
            }
        }
        else {
            FAILWITHMSG(IERR_INVALID_TIMESTAMP, "Found timestamp with less than year precision.");
        }
        if (precision1 >= ION_TS_MONTH && (ptime1->month != ptime2->month)) {
            goto is_false;
        }
        if (precision1 >= ION_TS_DAY && (ptime1->day != ptime2->day)) {
            goto is_false;
        }
        if (precision1 >= ION_TS_MIN && (ptime1->hours != ptime2->hours || ptime1->minutes != ptime2->minutes)) {
            goto is_false;
        }
        if (precision1 >= ION_TS_SEC && (ptime1->seconds != ptime2->seconds)) {
            goto is_false;
        }
        if (precision1 == ION_TS_FRAC) { // Then precision2 is also ION_TS_FRAC.
            IONCHECK(ion_decimal_equals_quad(&ptime1->fraction, &ptime2->fraction, pcontext, &decResult));
            if (!decResult) goto is_false;
        }
    }

is_true:
    *is_equal = TRUE;
    SUCCEED();

is_false:
    *is_equal = FALSE;
    SUCCEED();

    iRETURN;
}

/*
the binary serialization is:

type desc byte:
    high nibble == 
    low nibble == len (or 0xf for null)

timezone as var int 7
    -0 is no timezone
    otherwise the time offset in minutes

year    as var var_uint
month   as var var_uint
day     as var var_uint
hours   as var var_uint
minutes as var var_uint
seconds as var var_uint
fractional seconds as a decimal

note the length may preclude trailing values.

If the value is null not fields are included.
If the value is a date only the timezone, year, month, and day are included.
if the value includes time hours and minutes will also be present.
if the value includes seconds they will be present.
and all bytes included the are beyond seconds are a decimal fracional seconds value.
*/
       
iERR ion_timestamp_binary_read(ION_STREAM *stream, int32_t len, decContext *context, ION_TIMESTAMP *ptime)
{
    iENTER;
    int  b, offset;
    BOOL has_offset, is_negative;

    ASSERT(stream != NULL);
    ASSERT(len >= 0);
    ASSERT(context != NULL);
    ASSERT(ptime != NULL);

    IONCHECK(_ion_timestamp_initialize(ptime));

    if (len == 0) {
        // nothing else to do here - and the timestamp will be NULL
        SUCCEED();
    }

    // we read the first byte by hand to extract the sign
    // if it's negative we'll negate the Quad when it's done
    // we'll also have to shift in the other 7 bits
    if (!len--) FAILWITH(IERR_INVALID_BINARY);
    ION_GET(stream, b);
    if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
    is_negative = (b & 0x40);
    offset = b & 0x3F;
    if ((b & 0x80) == 0) { // check for the "end of var int bit" to see if we need to read a 2nd byte
      if (!len--) FAILWITH(IERR_INVALID_BINARY);
      ION_GET(stream, b);
      if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
      offset = (offset << 7) + (b & 0x7F);
      if ((b & 0x80) == 0) FAILWITH(IERR_INVALID_BINARY); // check for the "end of var int bit" to make sure we read the last byte
    }
    has_offset = TRUE;
    if (is_negative) {
        if (offset == 0) {
            // negative 0, we have no offset
            has_offset = FALSE;
        }
        else {
            offset = -offset;
        }
    }

    // right now precision s/b 0 - we'll put values into it as we read fields
    ptime->precision = 0;

    // let's see what's left for the decimal time portion
    if (len == 0) FAILWITH(IERR_INVALID_BINARY);

    // year is from 0001 to 9999
    // or 0x1 to 0x270F or 14 bits - 1 or 2 bytes
    if (!len--) FAILWITH(IERR_INVALID_BINARY);
    ION_GET(stream, b);
    if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
    ptime->year = b & 0x7F;
    if ((b & 0x80) == 0) {
        if (!len--) FAILWITH(IERR_INVALID_BINARY);
        ION_GET(stream, b);
        if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
        ptime->year = (ptime->year << 7) + (b & 0x7F);
        if ((b & 0x80) == 0) FAILWITH(IERR_INVALID_BINARY);
    }
    // If the precision ends at YEAR, month and day are implicitly 1. If it ends at MONTH, day is implicitly 1.
    ptime->month = 1;
    ptime->day = 1;
    ptime->precision = ION_TS_YEAR; // our lowest significant option
    if (len == 0) {
        has_offset = FALSE;
        goto timestamp_is_finished;
    }

    --len;
    ION_GET(stream, b);
    if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
    ptime->month = b & 0x7F;
    SET_FLAG_ON(ptime->precision, ION_TT_BIT_MONTH);
    if (len == 0) {
        has_offset = FALSE;
        goto timestamp_is_finished;
    }

    ION_GET(stream, b);
    len--;
    if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
    ptime->day = b & 0x7F;
    SET_FLAG_ON(ptime->precision, ION_TT_BIT_DAY);

    if (!_ion_timestamp_is_valid_day(ptime->year, ptime->month, ptime->day)) {
        FAILWITH(IERR_INVALID_BINARY);
    }

    if (len == 0) {
        has_offset = FALSE;
        goto timestamp_is_finished;
    }

    // now we look for hours and minutes
    ION_GET(stream, b);
    len--;
    if (b == EOF || len == 0) FAILWITH(IERR_INVALID_BINARY);
    ptime->hours = b & 0x7F;

    ION_GET(stream, b);
    len--;
    if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
    ptime->minutes = b & 0x7F;
    SET_FLAG_ON(ptime->precision, ION_TT_BIT_MIN);
    if (len == 0) goto timestamp_is_finished;

    ION_GET(stream, b);
    len--;
    if (b == EOF) FAILWITH(IERR_INVALID_BINARY);
    ptime->seconds = b & 0x7F;
    SET_FLAG_ON(ptime->precision, ION_TT_BIT_SEC);
    if (len == 0) goto timestamp_is_finished;

    // now we read in our actual "milliseconds since the epoch"
    IONCHECK(ion_binary_read_decimal(stream, len, context, &ptime->fraction, NULL)); // TODO make timestamp's fraction an ION_DECIMAL so it can support full precision?
    if (decQuadIsSigned(&ptime->fraction)) {
        if (decQuadIsZero(&ptime->fraction)) {
            // Negative-zero fractional seconds are normalized to positive-zero.
            decQuadCopyNegate(&ptime->fraction, &ptime->fraction);
        }
        else {
            // Any other negative besides negative-zero is an error.
            FAILWITH(IERR_INVALID_BINARY);
        }
    }
    if (decQuadGetExponent(&ptime->fraction) >= 0) {
        if (decQuadIsZero(&ptime->fraction)) goto timestamp_is_finished; // Fraction with zero coefficient and >= zero exponent is ignored.
        FAILWITH(IERR_INVALID_BINARY);
    }
    SET_FLAG_ON(ptime->precision, ION_TT_BIT_FRAC);
    goto timestamp_is_finished;

timestamp_is_finished:
    // now we set the "has timezone" bit
    if (has_offset) {
        SET_FLAG_ON(ptime->precision, ION_TT_BIT_TZ);
        ptime->tz_offset = (int16_t)-offset;
        IONCHECK(_ion_timestamp_to_utc(ptime, ptime));
        ptime->tz_offset = (int16_t)offset;
    }
    SUCCEED();

    iRETURN;
}
