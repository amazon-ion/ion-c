/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
// ion helper routines, these support utf8 conversion, generating and testing
// various constants (like symbol table values and the IVM)
//
// and anything else that doesn't have an obvious place to live
//

#include "ion_internal.h"
#include <string.h>

BOOL ion_helper_is_ion_version_marker(BYTE *buffer, SIZE len) 
{
    BOOL is_ion_version_marker = 
            len       >= ION_VERSION_MARKER_LENGTH
         && buffer[0] == ION_VERSION_MARKER[0]
         && buffer[1] == ION_VERSION_MARKER[1]
         && buffer[2] == ION_VERSION_MARKER[2]
         && buffer[3] == ION_VERSION_MARKER[3]
    ;

    // just to make sure all our definitions are lined up
    ASSERT(ION_VERSION_MARKER_LENGTH == 4);

    return is_ion_version_marker;
}

int ion_helper_get_tid_from_ion_type(ION_TYPE t) 
{
    int tid;

    switch ((intptr_t)t) {
    case (intptr_t)tid_NULL:      tid = TID_NULL;         break;
    case (intptr_t)tid_BOOL:      tid = TID_BOOL;         break;
    case (intptr_t)tid_INT:       tid = TID_POS_INT;      break; // TID_NEG_INT
    case (intptr_t)tid_FLOAT:     tid = TID_FLOAT;        break;
    case (intptr_t)tid_DECIMAL:   tid = TID_DECIMAL;      break;
    case (intptr_t)tid_TIMESTAMP: tid = TID_TIMESTAMP;    break;
    case (intptr_t)tid_SYMBOL:    tid = TID_SYMBOL;       break;
    case (intptr_t)tid_STRING:    tid = TID_STRING;       break;
    case (intptr_t)tid_CLOB:      tid = TID_CLOB;         break;
    case (intptr_t)tid_BLOB:      tid = TID_BLOB;         break;
    case (intptr_t)tid_LIST:      tid = TID_LIST;         break;
    case (intptr_t)tid_SEXP:      tid = TID_SEXP;         break;
    case (intptr_t)tid_STRUCT:    tid = TID_STRUCT;       break;
    default:            
        tid = TID_NONE;         
        break;
    }

    return tid;
}

ION_TYPE ion_helper_get_iontype_from_tid(int tid)
{
    ION_TYPE t = tid_none;

    switch (tid) {
    case TID_NULL:      t = tid_NULL;       break;  // 0
    case TID_BOOL:      t = tid_BOOL;       break;  // 1
    case TID_POS_INT:   t = tid_INT;        break;  // 2
    case TID_NEG_INT:   t = tid_INT;        break;  // 3
    case TID_FLOAT:     t = tid_FLOAT;      break;  // 4
    case TID_DECIMAL:   t = tid_DECIMAL;    break;  // 5
    case TID_TIMESTAMP: t = tid_TIMESTAMP;  break;  // 6
    case TID_SYMBOL:    t = tid_SYMBOL;     break;  // 7
    case TID_STRING:    t = tid_STRING;     break;  // 8
    case TID_CLOB:      t = tid_CLOB;       break;  // 9
    case TID_BLOB:      t = tid_BLOB;       break;  // 10 A
    case TID_LIST:      t = tid_LIST;       break;  // 11 B
    case TID_SEXP:      t = tid_SEXP;       break;  // 12 C
    case TID_STRUCT:    t = tid_STRUCT;     break;  // 13 D

    case TID_UTA:       t = tid_none;       break;  // 14 E
    case TID_UNUSED:    
        t = tid_none;       
        break;  // 15 F
    default:            
        t = tid_none;       
        break;
    }
    return t;
}

// UTF8 helper functions - should be in-lined if the compiler is competent

// these help convert from Java UTF-16 to Unicode Scalars (aka unicode code 
    // points (aka characters)) which are "32" bit values (really just 21 bits)
    // the DON'T check validity of their input, they expect that to have happened
    // already.  This is a perf issue since normally this check has been done
    // to detect that these routines should be called at all - no need to do it
    // twice.  
int32_t ion_makeUnicodeScalar(int32_t high_surrogate, int32_t low_surrogate) 
{
    int32_t unicodeScalar;

    unicodeScalar = (high_surrogate & ION_surrogate_value_mask) << ION_surrogate_utf32_shift;
    unicodeScalar |= (low_surrogate & ION_surrogate_value_mask);
    unicodeScalar += ION_surrogate_utf32_offset;
    
    ASSERT(unicodeScalar >=0 && unicodeScalar <= ION_max_unicode_scalar);

    return unicodeScalar;
}
int32_t ion_makeHighSurrogate(int32_t unicodeScalar) 
{
    int32_t c;

    ASSERT(unicodeScalar >=0 && unicodeScalar <= ION_max_unicode_scalar);

    c = unicodeScalar - ION_surrogate_utf32_offset;
    c >>= ION_surrogate_utf32_shift;
    c |= ION_high_surrogate_value;

    return c;
}

int32_t ion_makeLowSurrogate(int32_t unicodeScalar) 
{
    int32_t c;

    ASSERT(unicodeScalar >=0 && unicodeScalar <= ION_max_unicode_scalar);

    c = unicodeScalar - ION_surrogate_utf32_offset;
    c &= ION_surrogate_value_mask;
    c |= ION_low_surrogate_value;

    return c;
}

BOOL ion_isHighSurrogate(int32_t c) 
{
    BOOL is;
    is = (c & ION_specific_surrogate_mask) == ION_high_surrogate_value;
    return is;
}

BOOL ion_isLowSurrogate(int32_t c) 
{
    BOOL is;
    is = (c & ION_specific_surrogate_mask) == ION_low_surrogate_value;
    return is;
}

BOOL ion_isSurrogate(int32_t c) 
{
    BOOL is;
    is = (c & ION_either_surrogate_mask) == ION_high_surrogate_value;
    return is;
}

//
// base64 encoding helpers
//

void _ion_writer_text_write_blob_make_base64_image(int triple, char *output)
{
    //  +4 null terminator
    *(output + 4) = '\0';

    //  +3   6      3F     3F  from byte0
    *(output + 3) = _Ion_base64_chars[triple & 0x3F];

    //  +2  12     FFF    FC0  from byte0 + byte1
    triple >>= 6;
    *(output + 2) = _Ion_base64_chars[triple & 0x3F];

    //  +1  18   3FFFF  3F000  from byte1 + byte2
    triple >>= 6;
    *(output + 1) = _Ion_base64_chars[triple & 0x3F];

    //  +0  24  FFFFFF FC0000  from byte2
    triple >>= 6;
    *(output + 0) = _Ion_base64_chars[triple & 0x3F];
}


//
// escape sequence helpers
//

char *_ion_writer_get_control_escape_string(int c)
{
    char hex_buffer[5];
    char *image;

    switch (c) {
    case '\0':  image = "\\0"; break; //    \u0000  \0  alert NUL
    case '\a':  image = "\\a"; break; //    \u0007  \a  alert BEL
    case '\b':  image = "\\b"; break; //    \u0008  \b  backspace BS
    case '\t':  image = "\\t"; break; //    \u0009  \t  horizontal tab HT
    case '\n':  image = "\\n"; break; //    \u000A  \n  linefeed LF
    case '\f':  image = "\\f"; break; //    \u000C  \f  form feed FF 
    case '\r':  image = "\\r"; break; //    \u000D  \r  carriage return CR  
    case '\v':  image = "\\v"; break; //    \u000B  \v  vertical tab VT  
    case '\'':  image = "\\'"; break; //    single quote - if someone asks us to
    case '"':  image = "\\\""; break; //    double quote - escape either quote, fine
    default:
        hex_buffer[0] = '\\';
        hex_buffer[1] = 'x';
        hex_buffer[2] = _ion_hex_chars[(c >> 4) & 0xF];
        hex_buffer[3] = _ion_hex_chars[c & 0xF];
        hex_buffer[4] = 0;
        image = hex_buffer;
    }

    return image;
}

iERR _ion_str_fetch_unicode_scalar_at(ION_STRING *str, int offset, int *p_scalar, int *p_length)
{
    iENTER;

    int   c, c2, c3, c4;
    BYTE *pb, *end;
    int   length = 0;

    if (offset < 0 || offset >= str->length) {
        IONCHECK(IERR_INVALID_ARG);
    }

    pb = str->value + offset;
    end = str->value + str->length;

    c = *pb++;

    // first the common, easy, case - it's an ascii character
    // or an EOF "marker"
    if (ION_is_utf8_1byte_header(c)) {
        // this includes -1 (eof) and characters in the ascii char set
        length = 1;
    }
    else if (ION_is_utf8_2byte_header(c)) {
        // 2 byte unicode character >=128 and <= 0x7ff or <= 2047)
        // 110yyyyy 10zzzzzz
        if (pb >= end)                              goto bad_utf8;
        c2 = *pb++;
        if (ION_is_utf8_trailing_char_header(c2))   goto bad_utf8;
        c = ((c & 0x1f) << 6) | (c2 & 0x3f);
        length = 2;
    }
    else if (ION_is_utf8_3byte_header(c)) {
        // 3 byte unicode character >=2048 and <= 0xffff, <= 65535
        // 1110xxxx 10yyyyyy 10zzzzzz
        if (pb >= end)                              goto bad_utf8;
        c2 = *pb++;
        if (ION_is_utf8_trailing_char_header(c2))   goto bad_utf8;
        if (pb >= end)                              goto bad_utf8;
        c3 = *pb++;
        if (ION_is_utf8_trailing_char_header(c3))   goto bad_utf8;
        c = ((c & 0x0f) << 12) | ((c2 & 0x3f) << 6) | (c3 & 0x3f);
        length = 3;
    }
    else if (ION_is_utf8_4byte_header(c)) {
        // 4 byte unicode character > 65535 (0xffff) and <= 2097151 <= 10xFFFFF
        // 11110www 10xxxxxx 10yyyyyy 10zzzzzz
        if (pb >= end)                              goto bad_utf8;
        c2 = *pb++;
        if (ION_is_utf8_trailing_char_header(c2))   goto bad_utf8;
        if (pb >= end)                              goto bad_utf8;
        c3 = *pb++;
        if (ION_is_utf8_trailing_char_header(c3))   goto bad_utf8;
        if (pb >= end)                              goto bad_utf8;
        c4 = *pb++;
        if (ION_is_utf8_trailing_char_header(c4))   goto bad_utf8;

        c = ((c & 0x07) << 18) | ((c2 & 0x3f) << 12) | ((c3 & 0x3f) << 6) | (c4 & 0x3f);
        length = 4;
    }
    else if (c != -1) {
        // at this point anything except EOF is a bad UTF-8 character
        goto bad_utf8;
    }

    *p_scalar = c;
    *p_length = length;
    SUCCEED();

bad_utf8:
    FAILWITH(IERR_INVALID_UTF8);

    iRETURN;
}

//char *ion_type_to_str(ION_TYPE t)
const char *ion_type_to_str(ION_TYPE t)
{
    char *name = NULL;

    switch((intptr_t)t) {
    case (intptr_t)tid_none:      name = "tid_none";      break; // = -2,
    case (intptr_t)tid_EOF:       name = "tid_EOF";       break; // = -1,
    case (intptr_t)tid_NULL:      name = "tid_NULL";      break; // =  0,
    case (intptr_t)tid_BOOL:      name = "tid_BOOL";      break; // =  1,
    case (intptr_t)tid_INT:       name = "tid_INT";       break; // =  2,
    case (intptr_t)tid_FLOAT:     name = "tid_FLOAT";     break; // =  3,
    case (intptr_t)tid_DECIMAL:   name = "tid_DECIMAL";   break; // =  4,
    case (intptr_t)tid_TIMESTAMP: name = "tid_TIMESTAMP"; break; // =  5,
    case (intptr_t)tid_STRING:    name = "tid_STRING";    break; // =  6,
    case (intptr_t)tid_SYMBOL:    name = "tid_SYMBOL";    break; // =  7,
    case (intptr_t)tid_CLOB:      name = "tid_CLOB";      break; // =  8,
    case (intptr_t)tid_BLOB:      name = "tid_BLOB";      break; // =  9,
    case (intptr_t)tid_STRUCT:    name = "tid_STRUCT";    break; // = 10, /* 0xA */
    case (intptr_t)tid_LIST:      name = "tid_LIST";      break; // = 11, /* 0xB */
    case (intptr_t)tid_SEXP:      name = "tid_SEXP";      break; // = 12, /* 0xC */
    case (intptr_t)tid_DATAGRAM:  name = "tid_DATAGRAM";  break; // = 13  /* 0xD */
    default:
        return _ion_hack_bad_value_to_str((intptr_t)t, "Bad ION_TYPE");
    }
    return name;
}

const char *ion_error_to_str(iERR err)
{
    char *s;
    
    switch (err) {
	case IERR_NOT_IMPL:
        s = "IERR_NOT_IMPL"; 
        break;

    #define ERROR_CODE( name, val ) case name: s = #name; break;
    #include "ion_errors.h"

    default:
        return _ion_hack_bad_value_to_str((intptr_t)err, "Unknown error code");
    }
    return s;
}

const char *ion_writer_output_type_to_str(ION_WRITER_OUTPUT_TYPE t)
{
    switch (t) {
    case iWOT_UNKNOWN:     return "iWOT_UNKNOWN";     // 0
    case iWOT_UTF8:        return "iWOT_UTF8";        // 1
    case iWOT_BINARY:      return "iWOT_BINARY";      // 2
    case iWOT_PRETTY_UTF8: return "iWOT_PRETTY_UTF8"; // 3
    default:
        return _ion_hack_bad_value_to_str((intptr_t)t, "Bad ION_WRITER_OUTPUT_TYPE");
    }
}

const char *ion_symbol_table_type_to_str(ION_SYMBOL_TABLE_TYPE t)
{
    switch (t) {
    case ist_EMPTY:  return "ist_EMPTY";  // 0
    case ist_LOCAL:  return "ist_LOCAL";  // 1
    case ist_SHARED: return "ist_SHARED"; // 2
    case ist_SYSTEM: return "ist_SYSTEM"; // 3
    default:
        return _ion_hack_bad_value_to_str((intptr_t)t, "Bad ION_SYMBOL_TABLE_TYPE");
    }
}

#define HACK_BUFFER_COUNT 10 /* one really isn't enough */
static int  hack_buffer_next = 0;
static char hack_buffer_return_message[HACK_BUFFER_COUNT][MAX_MESSAGE_LENGTH + 1];

const char *_ion_hack_bad_value_to_str(intptr_t val, char *msg)
{
    char   hack_buffer_int[MAX_INT32_LENGTH];
    char  *hack_buffer_return, *hack_buffer;
    size_t int_len, msg_limit;

    ASSERT(sizeof(val) == sizeof(int));

    // assign a buffer
    // TODO: make this thread safe - in some other way - like thread local storage
    //       or by taking in user buffers
    hack_buffer_next = (hack_buffer_next == HACK_BUFFER_COUNT) ? 0 : hack_buffer_next + 1;
    hack_buffer_return = hack_buffer_return_message[hack_buffer_next];

    // format the int in our local int buffer
    hack_buffer = _ion_itoa_10((int32_t)val, hack_buffer_int, sizeof(hack_buffer_int));
    int_len = _ion_strnlen(hack_buffer, sizeof(hack_buffer_int));

    // msg_limit is the number of char's from msg we can fit into the return buffer
    // with the int and the 2 format chars ": " and the null terminator
    msg_limit = sizeof(MAX_MESSAGE_LENGTH) - int_len - 3;
    msg_limit = _ion_strnlen(msg,  msg_limit);

    // now put it together
    strncpy(hack_buffer_return, hack_buffer, sizeof(hack_buffer_return_message[0]));
    strncat(hack_buffer_return, ": ", sizeof(hack_buffer_return_message[0]));
    strncpy(hack_buffer_return, msg, msg_limit);

    // put in the terminator
    hack_buffer_return[msg_limit + int_len + 3] = 0;

    return hack_buffer_return;
}

char * _ion_itoa_10(int32_t val, char *dst_buf, size_t buf_length) 
{
    // sprintf(dest, "%d", val); - with sprintf we can't tell if we're running off the end of the buf

    // we're only using this on error messages and unknown symbol name generation
    // so we don't care at the moment.  later this may become more important and
    // be worth optimizing
    char    *cp, *start, *end;
    int32_t  next;
    int      digit, temp;

    cp = dst_buf;
    end = dst_buf + buf_length;
    if (val < 0) {
        if (cp >= end) goto overflow;
        *cp++ = '-';
        val = -val;
    }

    // record where the first digit goes
    if (val == 0) {
        if (cp >= end) goto overflow;
        *cp++ = '0';
        *cp = '\0';
    }
    else {
        start = cp;
        while (val > 0) {
            next = val / 10;
            digit = (int)(val - 10*next);
            if (cp >= end) goto overflow;
            *cp++ = digit + '0';
            val = next;
        }
        if (cp >= end) goto overflow;
        *cp = '\0';

        // reverse the digits
        while (start < --cp) {
            temp = *start;
            *start = *cp;
            *cp = temp;
            start++;
        }
    }
    return dst_buf;

overflow:
    assert(cp < end && "buffer overflow in local itoa!");
    return NULL; // this should force a null pointer exception in the caller
}

char *_ion_i64toa_10(int64_t val, char *dst_buf, size_t buf_length) 
{
    // sprintf(dest, "%dI64", val); - with sprintf we can't tell if we're running off the end of the buf

    // we're only using this on error messages and unknown symbol name generation
    // so we don't care at the moment.  later this may become more important and
    // be worth optimizing

    char    *cp, *start, *end;
    int64_t  next;
    int      digit, temp;

    cp = dst_buf;
    end = dst_buf + buf_length;
    if (val < 0) {
        if (cp >= end) goto overflow;
        *cp++ = '-';
        val = -val;
    }

    // record where the first digit goes
    if (val == 0) {
        if (cp >= end) goto overflow;
        *cp++ = '0';
        *cp = '\0';
    }
    else {
        start = cp;
        while (val > 0) {
            next = val / 10;
            digit = (int)(val - next);
            if (cp >= end) goto overflow;
            *cp++ = digit + '0';
            val = next;
        }
        if (cp >= end) goto overflow;
        *cp = '\0';

        // reverse the digits
        while (start < cp) {
            temp = *start;
            *start = *cp;
            *cp = temp;
            cp--;
            start++;
        }
    }

    return dst_buf;

overflow:
    assert(cp < end && "buffer overflow in local itoa!");
    return NULL; // this should force a null pointer exception in the caller
}

size_t _ion_strnlen(const char *str, const size_t maxlen) {
    const char *pos = memchr(str, '\0', maxlen);
    return pos != 0 ? pos - str : maxlen;
}

void ion_helper_breakpoint(void)
{
    _debug_on = !!_debug_on;
}

int ion_helper_enter(const char *filename, int line_number, long count)
{
    if (g_ion_debug_tracing) {
        filename = filename ? ion_helper_short_filename(filename) : "\"<missing file name>\"";
        fprintf(stderr, "ENTER, %s, %d, %ld\n",
            (const char *)filename,
            (int)line_number,
            (long)count
        );
    }
    return line_number;
}

iERR ion_helper_return(const char *filename, int line_number, long count, iERR err)
{
    if (g_ion_debug_tracing) {
        filename = filename ? ion_helper_short_filename(filename) : "\"<missing file name>\"";
        fprintf(stderr, "EXIT, %s, %d, %ld\n",
            (const char *)filename,
            (int)line_number,
            (long)count
        );
    }
    return err;
}

const char *ion_helper_short_filename(const char *filename)
{
    const char *cp, *last_slash;

    if (!filename) return NULL;

    for (cp = last_slash = filename; *cp; cp++) {
        if (*cp == '\\' || *cp == '/') {
            last_slash = cp + 1;
        }
    }

    return last_slash;
}