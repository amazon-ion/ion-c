/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/*
 *  code for the token scanner for IonC, this is the 2nd version
 *
 *  while the code for scanning is reasonably large and distinct
 *  the relationship between ion_reader_text and ion_scanner is
 *  pretty blurry. It might make more sense to merge the two
 *  together ... at some point.
 */

#include "ion.h"
#include "ion_internal.h"

// this macro is just to keep the lines of code shorter, the do-while 
// forces the need for a ';' and it executes exactly once
// still - use with care it depends on local variables and good behavior!
#define PUSH_VALUE_BYTE(x) do { if (remaining <= 0) { FAILWITH(IERR_TOKEN_TOO_LONG); } remaining--; *dst++ = MAKE_BYTE(x); } while(FALSE)

static inline BOOL _ion_scanner_is_control_character(int c)
{
    return c <= 0x1F && 0x00 <= c;
}

static inline BOOL _ion_scanner_is_newline(int c)
{
    return c == 0x0A || c == 0x0D;
}

static inline BOOL _ion_scanner_is_non_newline_whitespace(int c)
{
    return c == 0x09 || c == 0x0B || c == 0x0C; // Tab, vertical tab, and form feed, respectively.
}

static inline BOOL _ion_scanner_is_valid_plain_char(int c)
{
    return !_ion_scanner_is_control_character(c) || _ion_scanner_is_non_newline_whitespace(c);
}

static inline BOOL _ion_scanner_is_valid_long_char(int c)
{
    return !_ion_scanner_is_control_character(c) || _ion_scanner_is_newline(c) || _ion_scanner_is_non_newline_whitespace(c);
}

static inline iERR _ion_scanner_get_terminator_for_sub_type(ION_SUB_TYPE ist, int* terminator)
{
    iENTER;

    if (ist == IST_SYMBOL_QUOTED) {
        *terminator = '\'';
    } else if (ist == IST_STRING_PLAIN || ist == IST_CLOB_PLAIN) {
        *terminator = '"';
    } else if (ist == IST_STRING_LONG || ist == IST_CLOB_LONG) {
        *terminator = -1;
    } else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }

    iRETURN;
}

iERR _ion_scanner_initialize(ION_SCANNER *scanner, ION_READER *preader)
{
    iENTER;

    ASSERT(scanner);

    scanner->_stream = preader->istream;

    IONCHECK(_ion_reader_text_open_alloc_buffered_string(preader
        , preader->options.symbol_threshold
        , &(scanner->_value_image)
        , &(scanner->_value_buffer)
        , &(scanner->_value_buffer_length)
    ));

    scanner->_value_location = SVL_NONE;

    IONCHECK(_ion_scanner_reset(scanner));

    SUCCEED();

    iRETURN;
}

iERR _ion_scanner_reset(ION_SCANNER *scanner)
{
    iENTER;

    ASSERT(scanner);

    IONCHECK(_ion_scanner_reset_value(scanner));

    scanner->_line               = 1;
    scanner->_offset             = 0;
    scanner->_saved_offset       = 0;
    scanner->_unread_sub_type    = IST_NONE;

    SUCCEED();

    iRETURN;
}


iERR _ion_scanner_reset_value(ION_SCANNER *scanner)
{
    iENTER;

    ASSERT(scanner);

    ION_STRING_INIT(&scanner->_value_image);
    
    scanner->_value_location     = SVL_NONE;
    scanner->_value_start        = -1;
    scanner->_pending_bytes_pos  = scanner->_pending_bytes;
    scanner->_pending_bytes_end  = scanner->_pending_bytes;

    SUCCEED();

    iRETURN;
}

// Is this useful just to keep up the pattern (initialize, reset, close)
// or just a waste? Hard to say.
iERR ion_scanner_close(ION_SCANNER *scanner)
{
    // do we want to memset the scanner to null?
    scanner->_stream = NULL; // at least the stream, if anyone trys to use this later it will fail pretty quickly
    return IERR_OK;
}


iERR _ion_scanner_next(ION_SCANNER *scanner, ION_SUB_TYPE *p_ist)
{
    iENTER;

    if (scanner->_unread_sub_type != IST_NONE) {
        *p_ist = scanner->_unread_sub_type;
        scanner->_value_location = scanner->_unread_value_location;
        if (scanner->_value_location == SVL_VALUE_IMAGE) {
            scanner->_value_image.value = scanner->_value_buffer;
            scanner->_value_image.length = scanner->_unread_value_length;
        }
        scanner->_unread_sub_type = IST_NONE;
    }
    else {
        IONCHECK(_ion_scanner_next_actual(scanner, p_ist));
    }

    iRETURN;
}

iERR _ion_scanner_un_next(ION_SCANNER *scanner, ION_SUB_TYPE ist)
{
    iENTER;
    ASSERT(scanner->_unread_sub_type == IST_NONE);

    scanner->_unread_sub_type = ist;
    scanner->_unread_value_location = scanner->_value_location;
    scanner->_unread_value_length = scanner->_value_image.length;

    SUCCEED();

    iRETURN;
}

iERR _ion_scanner_next_actual(ION_SCANNER *scanner, ION_SUB_TYPE *p_ist)
{
    iENTER;
    ION_SUB_TYPE t = IST_NONE;
    int          c, c2;
    BOOL         is_triple_quote, is_match, is_null;
#ifdef DEBUG
    static long _token_counter = 0;

    _token_counter++;
    if (_token_counter == -1) { // 7238 || scanner->_has_marked_value) {
        ion_helper_breakpoint();
        _token_counter = _token_counter + 0;;
    }
#endif

    // read the first character of a token, tokens may be preceeded
    // by whitespace
    IONCHECK(_ion_scanner_read_past_whitespace(scanner, &c));

    // it's actually in the stream until it gets read into the value buffer
    // for punctuation sub types this doesn't matter (there's no
    // need to look at the value.
    // for symbols, strings, clobs, and blobs they'll be left in the stream
    // all others will be copied into value buffer
    // when someone copies the bytes from the symbol, string, blob, or clob
    // out of the stream and into their buffer (and we get to the end of the 
    // value) we'll mark is as no longer in the stream at that time
    //
    // but the common case is not, so we'll set this to "in string" when we know
    scanner->_value_location = SVL_NONE;
    scanner->_value_start  = ion_stream_get_position( scanner->_stream ) - 1; // -1 because we read past the byte
    
    switch (c) {
    case EOF:
        t = IST_EOF;
        break;
    case ':':
        // since we check for double colon using the peek function
        // (above), there's only 1 possibility now
        t = IST_SINGLE_COLON;    
        break;
    case '{':
        IONCHECK(_ion_scanner_read_char(scanner, &c2));
        if (c2 != '{') {
            IONCHECK(_ion_scanner_unread_char(scanner, c2));
            t = IST_STRUCT;
        }
        else {
            t = IST_DOUBLE_BRACE;
        }
        break;
    case '}':
        t = IST_CLOSE_SINGLE_BRACE; // we don't have enough context here to decide if this is a single of a double close brace unambiguously
        break;
    case '[':
        t = IST_LIST;
        break;
    case ']':
        t = IST_CLOSE_BRACKET;
        break;
    case '(':
        t = IST_SEXP;
        break;
    case ')':
        t = IST_CLOSE_PAREN;
        break;
    case ',':
        t = IST_COMMA;
        break;
    case '\'':
        IONCHECK(_ion_scanner_peek_two_single_quotes(scanner, &is_triple_quote));
        if (is_triple_quote) {
            t = IST_STRING_LONG;
        }
        else {
            t = IST_SYMBOL_QUOTED;
        }
        scanner->_value_location = SVL_IN_STREAM;
        break;

    case '+':
        IONCHECK(_ion_scanner_read_char(scanner, &c2));
        if (c2 == 'i') {
            IONCHECK(_ion_scanner_peek_keyword(scanner, "nf", &is_match));
            if (is_match) {
                t = IST_PLUS_INF;
                break;
            }
        }
        IONCHECK(_ion_scanner_unread_char(scanner, c2));
        // fall through to the extended symbol case
    case '<': case '>': case '*': case '=': case '^': case '&': case '|': 
    case '~': case ';': case '!': case '?': case '@': case '%': case '`': 
    case '#': case '.': case '/': 
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        t = IST_SYMBOL_EXTENDED;
        scanner->_value_location = SVL_IN_STREAM;
        break;
    case '"':
        t = IST_STRING_PLAIN;
        scanner->_value_location = SVL_IN_STREAM;
        break;
    case 'n':
        // for 'n' we check to see if this is "null" or a typed
        // null (such as null.int)
        IONCHECK(_ion_scanner_peek_for_null(scanner, &is_null, &c));
        if (is_null) {
            // see if the terminating character was a '.' which would preceed a type name
            if (c != '.') {
                IONCHECK(_ion_scanner_unread_char(scanner, c));
                t = IST_NULL_NULL;
            }
            else {
                IONCHECK(_ion_scanner_read_null_type(scanner, &t));
            }
            break;
        }
        // we also have to check for nan, since that's not a plain symbol
        IONCHECK(_ion_scanner_peek_keyword(scanner, "an", &is_match));
        if (is_match) {
            t = IST_NAN;
            break;
        }
        // let the non-null 'n' fall through to the plain symbol case
    case 'a': case 'b': case 'c': case 'd': case 'e':             // no 'f'
    case 'g': case 'h': case 'j': case 'i': case 'k': case 'l':
    case 'm':           case 'o': case 'p': case 'q': case 'r':   // no 'n'
    case 's':           case 'u': case 'v': case 'w': case 'x':   // not 't'
    case 'y': case 'z': 
    case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': 
    case 'G': case 'H': case 'J': case 'I': case 'K': case 'L':
    case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
    case 'S': case 'T': case 'U': case 'V': case 'W': case 'X': 
    case 'Y': case 'Z': 
    case '$': case '_':
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        t = IST_SYMBOL_PLAIN;
        scanner->_value_location = SVL_IN_STREAM;
        break;
    case 't':
        IONCHECK(_ion_scanner_peek_keyword(scanner, "rue", &is_match));
        if (is_match) {
            t = IST_BOOL_TRUE;
        }
        else {
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            t = IST_SYMBOL_PLAIN;
            scanner->_value_location = SVL_IN_STREAM;
        }
        break;
    case 'f': 
        IONCHECK(_ion_scanner_peek_keyword(scanner, "alse", &is_match));
        if (is_match) {
            t = IST_BOOL_FALSE;
        }
        else {
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            t = IST_SYMBOL_PLAIN;
            scanner->_value_location = SVL_IN_STREAM;
        }
        break;

    case '0': case '1': case '2': case '3': case '4': 
    case '5': case '6': case '7': case '8': case '9':
        IONCHECK(_ion_scanner_read_possible_number(scanner, c, 1, &t));
        // note _ion_scanner_read_possible_number sets: scanner->_value_location = SVL_VALUE_IMAGE;
        break;
    case '-':
        // see if we have a real number or what might be an extended symbol
        IONCHECK(_ion_scanner_read_char(scanner, &c2));
        if (c2 == 'i') {
            IONCHECK(_ion_scanner_peek_keyword(scanner, "nf", &is_match));
            if (is_match) {
                t = IST_MINUS_INF;
                break;
            }
        }
        // not "inf" so is it a number?
        if (IS_1_BYTE_UTF8(c2) && isdigit(c2)) {
            IONCHECK(_ion_scanner_read_possible_number(scanner, c2, -1, &t));
            scanner->_value_location = SVL_VALUE_IMAGE;
        }
        else {
            // otherwise it must be an extended symbol and we need to
            // put these characters back (the '-' and whatever else we read)
            IONCHECK(_ion_scanner_unread_char(scanner, c2));
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            t = IST_SYMBOL_EXTENDED;
            scanner->_value_location = SVL_IN_STREAM;
        }
        break;
    default:
        FAILWITH(IERR_INVALID_TOKEN); // "invalid character for token start"
    }

    *p_ist = t;
    iRETURN;
}

iERR _ion_scanner_next_distinguish_lob(ION_SCANNER *scanner, ION_SUB_TYPE *p_ist)
{
    iENTER;
    int          c;
    BOOL         is_triple_quote;
    ION_SUB_TYPE t = IST_ERROR;

    IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
    if (c == '"') {
        t = IST_CLOB_PLAIN;
    }
    else if (c == '\'') {
        IONCHECK(_ion_scanner_peek_two_single_quotes(scanner, &is_triple_quote));
        if (is_triple_quote) {
            t = IST_CLOB_LONG;
        }
        else {
            FAILWITH(IERR_BAD_BASE64_BLOB);
        }
    }
    else if (IS_1_BYTE_UTF8(c) == FALSE) {
        FAILWITH(IERR_BAD_BASE64_BLOB);
    }
    else if (_Ion_base64_value[c] >= 0) {
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        t = IST_BLOB;
    }
    else if (c == '}') {
        // immediate closeing braces says this is an empty blob
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == '}') {
            // we'll want the closing braces around to stop the base64 scan
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            t = IST_BLOB;
        }
        else {
            FAILWITH(IERR_BAD_BASE64_BLOB);
        }
    }
    else {
        FAILWITH(IERR_BAD_BASE64_BLOB);
    }
    scanner->_value_location = SVL_IN_STREAM;
    *p_ist = t;

    iRETURN;
}

iERR _ion_scanner_read_char(ION_SCANNER *scanner, int *p_char)
{
    iENTER;
    int c;

    ION_GET(scanner->_stream, c);
    scanner->_offset++;

    if (c == '\r' || c == '\n') {
        IONCHECK(_ion_scanner_read_char_newline_helper(scanner, &c));
    }

    *p_char = c;

    iRETURN;
}

iERR _ion_scanner_read_char_with_validation(ION_SCANNER* scanner, ION_SUB_TYPE ist, int* result)
{
    iENTER;

    int c;
    _ion_scanner_read_char(scanner, &c);

    BOOL is_valid;
    if (ist == IST_SYMBOL_QUOTED) {
        is_valid = TRUE;
    } else if (ist == IST_STRING_PLAIN || ist == IST_CLOB_PLAIN) {
        is_valid = _ion_scanner_is_valid_plain_char(c);
    } else if (ist == IST_STRING_LONG || ist == IST_CLOB_LONG) {
        is_valid = _ion_scanner_is_valid_long_char(c);
    } else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }

    if (!is_valid) {
        char error_message[ION_ERROR_MESSAGE_MAX_LENGTH];
        snprintf(error_message, ION_ERROR_MESSAGE_MAX_LENGTH, "Invalid character 0x%04X", c);
        FAILWITHMSG(IERR_INVALID_SYNTAX, error_message);
    } else {
        *result = c;
    }

    iRETURN;
}

iERR _ion_scanner_read_char_newline_helper(ION_SCANNER *scanner, int *p_char)
{
    iENTER;
    int c, newline;

    ASSERT(p_char && scanner);

    c = *p_char;
    if (c != '\r') {
        ASSERT(c == '\n');
        newline = NEW_LINE_1;
    }
    else {
        // it was \r - is there a \n?
        ION_GET(scanner->_stream, c);
        if (c == '\n') {
            newline = NEW_LINE_2;
        }
        else {
            // not a new line, we have to push it back
            newline = NEW_LINE_3;
            IONCHECK(ion_stream_unread_byte(scanner->_stream, c));
        }
    }

    // there are (currently) no states where it's necessary
    // to pre-read over more than a single new line, so we
    // only need 1 saved offset
    scanner->_saved_offset = scanner->_offset;
    scanner->_line++;
    scanner->_offset = 0;

    *p_char = newline;

    iRETURN;
}

iERR _ion_scanner_read_past_whitespace(ION_SCANNER *scanner, int *p_char)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case ION_unicode_byte_order_mark_utf8_start:
            IONCHECK(_ion_scanner_read_past_unicode_byte_order_mark(scanner, &c));
            if (c != ' ') goto actual_char;
            break;
        case '/':
            IONCHECK(_ion_scanner_read_past_comment(scanner, &c));
            if (c != ' ') goto actual_char;
            break;
        case '\0':
        case ' ':
        case '\t':
        case NEW_LINE_3: /* carraige return */
        case NEW_LINE_2: /* carraige return, newline  */
        case NEW_LINE_1: /* newline */
            break;
        default:
            goto actual_char;
        }
    }

actual_char:
    *p_char = c;

    iRETURN;
}

iERR _ion_scanner_read_past_lob_whitespace(ION_SCANNER *scanner, int *p_char)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case ION_unicode_byte_order_mark_utf8_start:
            IONCHECK(_ion_scanner_read_past_unicode_byte_order_mark(scanner, &c));
            if (c != ' ') goto actual_char;
            break;
        case '\0':
        case ' ':
        case '\t':
        case NEW_LINE_3: /* carraige return */
        case NEW_LINE_2: /* carraige return, newline  */
        case NEW_LINE_1: /* newline */
            break;
        default:
            goto actual_char;
        }
    }

actual_char:
    *p_char = c;

    iRETURN;
}

iERR _ion_scanner_read_past_unicode_byte_order_mark(ION_SCANNER *scanner, int *p_char)
{
    iENTER;
    int c;

    IONCHECK(_ion_scanner_read_char(scanner, &c));
    if (c == ION_unicode_byte_order_mark_utf8[1]) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == ION_unicode_byte_order_mark_utf8[2]) {
            *p_char = ' ';
            SUCCEED();
        }
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        c = ION_unicode_byte_order_mark_utf8[1];
    }
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    
    iRETURN;
}

iERR _ion_scanner_read_past_comment(ION_SCANNER *scanner, int *p_char)
{
    iENTER;
    int c;

    IONCHECK(_ion_scanner_read_char(scanner, &c));
    switch (c) {
    case '/':
        // we have a single line comment
        IONCHECK(_ion_scanner_read_to_one_line_comment(scanner));
        *p_char = ' ';
        break;
    
    case '*':
        // we have a multi-line comment (or at least not a single line comment)
        IONCHECK(_ion_scanner_read_to_end_of_long_comment(scanner));
        *p_char = ' ';
        break;

    default:
        // oops, we don't want to disturb this one, just throw it back
        // note we don't do anything to p_char since we want to leave it alone too
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        break;
    }

    iRETURN;
}

iERR _ion_scanner_read_to_one_line_comment(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        // these are escaped new lines, they act as nothing which
        // in this case is just whitespace we'll read past
        case NEW_LINE_3: /* carraige return */
        case NEW_LINE_2: /* carraige return, newline  */
        case NEW_LINE_1: /* newline */
        case SCANNER_EOF:
            goto end_of_comment;
        default:
            break;
        }
    }
end_of_comment:
    iRETURN;
}
 
iERR _ion_scanner_read_to_end_of_long_comment(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == '*') {
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '/') goto end_of_comment;
        }
        else if (c == SCANNER_EOF) {
            FAILWITH(IERR_UNEXPECTED_EOF);
        }
    }
end_of_comment:
    iRETURN;
}

iERR _ion_scanner_unread_char(ION_SCANNER *scanner, int c)
{
    iENTER;

    switch(c) {
    case NEW_LINE_1:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\n'));
        goto uncount_line;
    case NEW_LINE_2:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\n')); // remember - push is reverse order
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\r'));
        goto uncount_line;
    case NEW_LINE_3:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\r'));
        goto uncount_line;
    case EMPTY_ESCAPE_SEQUENCE1:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\n'));
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\\'));
        goto uncount_line;
    case EMPTY_ESCAPE_SEQUENCE2:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\n')); // remember - push is reverse order
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\r'));
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\\'));
        goto uncount_line;
    case EMPTY_ESCAPE_SEQUENCE3:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\r'));
        IONCHECK(ion_stream_unread_byte(scanner->_stream, '\\'));
        goto uncount_line;
    default:
        IONCHECK(ion_stream_unread_byte(scanner->_stream, c));
        scanner->_offset--;
        break;
    }
    SUCCEED();

uncount_line:
    _ion_scanner_unread_char_uncount_line(scanner);
    SUCCEED();

    iRETURN;
}

void _ion_scanner_unread_char_uncount_line(ION_SCANNER *scanner)
{
    scanner->_line--;
    scanner->_offset = scanner->_saved_offset;
}

iERR _ion_scanner_peek_double_colon(ION_SCANNER *scanner, BOOL *p_is_double_colon)
{
    iENTER;
    int  c;
    BOOL is_double_colon = FALSE;

    IONCHECK(_ion_scanner_read_past_whitespace(scanner, &c));
    if (c != ':') {
        IONCHECK(_ion_scanner_unread_char(scanner, c));
    }
    else {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c != ':') {
            IONCHECK(_ion_scanner_unread_char(scanner, c));
        }
        else {
            is_double_colon = TRUE;
        }
    }
    *p_is_double_colon = is_double_colon;

    iRETURN;
}

iERR _ion_scanner_peek_two_single_quotes(ION_SCANNER *scanner, BOOL *p_rest_of_triple_quote_found)
{
    iENTER;
    int  c;
    BOOL rest_of_triple_quote_found = FALSE;
    
    IONCHECK(_ion_scanner_read_char(scanner, &c));
    if (c == '\'') {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == '\'') {
            rest_of_triple_quote_found = TRUE;
            goto found;
        }
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        c = '\'';  // restore c so that we when we undo just below it's the right character
    }
    IONCHECK(_ion_scanner_unread_char(scanner, c));

found:
    *p_rest_of_triple_quote_found = rest_of_triple_quote_found;
    iRETURN;
}

iERR _ion_scanner_peek_for_null(ION_SCANNER *scanner, BOOL *p_is_null, int *p_char)
{
    iENTER;
    int  c;
    BOOL is_terminator;

    // we're looking for an the "ull" (and a possible ".") following the "n"
    // we saw at the beginning of our token

    IONCHECK(_ion_scanner_read_char(scanner, &c));
    if (c == 'u') {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == 'l') {
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == 'l') {
                IONCHECK(_ion_scanner_read_char(scanner, &c));
                // it's either a valid unquoted symbol character (in which case this
                // isn't "null") or the dot (in which case it is) or something else
                // is which case we'll call it a null and let the symbol reader sort it out
                IONCHECK(_ion_scanner_is_value_terminator(scanner, c, &is_terminator));
                if (is_terminator || c == '.') {
                    *p_is_null = TRUE;
                    *p_char = c;
                    SUCCEED();
                }
                IONCHECK(_ion_scanner_unread_char(scanner, c));
                c = 'l';
            }
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            c = 'l';
        }
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        c = 'u';
    }
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    *p_is_null = FALSE;
    SUCCEED();

    iRETURN;
}

iERR _ion_scanner_read_null_type(ION_SCANNER *scanner, ION_SUB_TYPE *p_ist)
{
    iENTER;
    int           c;
    char          unread_buffer[MAX_TYPE_NAME_LEN + 1], *unread_pos = unread_buffer;  // +1 for null terminator
    SIZE          len, remaining = MAX_TYPE_NAME_LEN + 1;
    ION_SUB_TYPE  ist;
    BOOL          is_terminator;

    // we're looking for an the "nf" following the "i" and the
    // sign we saw at the beginning of our token

    while (remaining--) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c < 'a' || c > 'z') {
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            break;
        }
        *unread_pos++ = MAKE_BYTE(c);
    }
    len = unread_pos - unread_buffer;
    if (len < MIN_TYPE_NAME_LEN) goto not_a_typename;
    IONCHECK(_ion_scanner_is_value_terminator(scanner, c, &is_terminator));
    if (!is_terminator) goto not_a_typename;

    *unread_pos = '\0';
    ist = _ion_scanner_check_typename(unread_buffer, len);
    if (ist == NULL) {
        goto not_a_typename;
    }

    *p_ist = ist;

    SUCCEED();

not_a_typename:
    FAILWITH(IERR_INVALID_TOKEN);

    iRETURN;
}

ION_SUB_TYPE _ion_scanner_check_typename(char *buf, int len)
{
    if (len < MIN_TYPE_NAME_LEN || len > MAX_TYPE_NAME_LEN) return NULL;


    
    // check the string against type names or fail
    switch(buf[0]) {
    case 'b':
        if (len != 4) return 0;
	    if (strncmp("bool", buf, 4) == 0) return IST_NULL_BOOL;
	    if (strncmp("blob", buf, 4) == 0) return IST_NULL_BLOB;
	    break;
    case 'c':
        if (len != 4) return 0;
	    if (strncmp("clob", buf, 4) == 0) return IST_NULL_CLOB;
	    break;
    case 'd':
        if (len != 7) return 0;
	    if (strncmp("decimal", buf, 7) == 0) return IST_NULL_DECIMAL;
	    break;
    case 'f':
        if (len != 5) return 0;
	    if (strncmp("float", buf, 5) == 0) return IST_NULL_FLOAT;
	    break;
    case 'i':
        if (len != 3) return 0;
	    if (strncmp("int", buf, 3) == 0) return IST_NULL_INT;
	    break;
    case 'l':
        if (len != 4) return 0;
	    if (strncmp("list", buf, 4) == 0) return IST_NULL_LIST;
	    break;
    case 'n':
        if (len != 4) return 0;
	    if (strncmp("null", buf, 4) == 0) return IST_NULL_NULL;
	    break;
    case 's':
        if (len == 6) {
    	    if (strncmp("string", buf, 6) == 0) return IST_NULL_STRING;
	        if (strncmp("struct", buf, 6) == 0) return IST_NULL_STRUCT;
	        if (strncmp("symbol", buf, 6) == 0) return IST_NULL_SYMBOL;
        }
        else if (len == 4) {
            if (strncmp("sexp", buf, 4) == 0) return IST_NULL_SEXP;
        }
	    break;
    case 't':
        if (len != 9) return 0;
        if (strncmp("timestamp", buf, 9) == 0) return IST_NULL_TIMESTAMP;
	    break;
    default:
        break;
    }
    return 0;
}

// c is an already read-ahead character
iERR _ion_scanner_is_value_terminator(ION_SCANNER *scanner, int c, BOOL *p_is_terminator)
{
    iENTER;
    ION_TERM_TYPE maybe;
    int           c2;
    BOOL          is_terminator = FALSE;

    if (c < 0) {
        is_terminator = TRUE;
    }
    else {
        maybe = _Ion_value_terminators[c];
        if (maybe == ION_TT_MAYBE) {
            IONCHECK(_ion_scanner_read_char(scanner, &c2));
            if (c < 0) {
                maybe = IS_NEWLINE_SEQUENCE(c2);
            }
            else {
                maybe = _Ion_value_terminators2[c2];
            }
            IONCHECK(_ion_scanner_unread_char(scanner, c2));
        }
        is_terminator = (maybe == ION_TT_YES);
    }
    *p_is_terminator = is_terminator;
    SUCCEED();

    iRETURN;
}

iERR _ion_scanner_peek_keyword(ION_SCANNER *scanner, char *tail, BOOL *p_is_match)
{
    iENTER;
    int   c, match_c;
    char *cp = tail;
    BOOL  is_match = FALSE;

    // we're looking for an the "nf" following the "i" and the
    // sign we saw at the beginning of our token

    while ((match_c = *cp++) != '\0') {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c != match_c) {
            cp--; // we didn't match this char, so we don't unread it (we unread c which we just read)
            goto unread_tail;
        }
    }

    // we may have a match (if we have a terminator next)
    // we'll peek ahead and see if we have that terminator
    IONCHECK(_ion_scanner_read_char(scanner, &c));
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    IONCHECK(_ion_scanner_is_value_terminator(scanner, c, &is_match));

unread_tail:
    if (!is_match) {
        // no luck so we unread 
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        while (cp > tail) {
            cp--;
            c = *cp;
            IONCHECK(_ion_scanner_unread_char(scanner, c));
        }
    }

    *p_is_match = is_match;
    SUCCEED();

    iRETURN;
}


/** This is called when we are in the middle of reading a value or
 *  are at the beginning of a value and the user call next().
 *
 *  this scans and does not fully validate the value. It should
 *  leave the input stream at the same point simply consuming the
 *  value normally would.
 *
 *  this routine is more generous about parsing. Basically it just
 *  recognized possible escape sequences in strings, container start
 *  and end tokens (including lobs), and comments. It just searches
 *  for the desired end character which should mark the end of
 *  this value. (strings include all quoted sequences)
 */
iERR _ion_scanner_skip_value_contents(ION_SCANNER *scanner, ION_SUB_TYPE ist)
{
    iENTER;

    assert(scanner);

    // we should never be skipping an unread value, we haven't
    // actually processed it yet
    ASSERT(scanner->_unread_sub_type == IST_NONE);

    // if it's not in the stream the bytes have already been consumed
    // WAS:if (scanner->_value_location != SVL_IN_STREAM) {
    //    SUCCEED();
    //}
    if ((scanner->_value_location == SVL_IN_STREAM) 
     || (ist == IST_SEXP) 
     || (ist == IST_LIST) 
     || (ist == IST_STRUCT)
        ) {
        // see what type of value we are finding the end of
        if (ist == IST_STRING_PLAIN) {
            IONCHECK(_ion_scanner_skip_plain_string(scanner));
        }
        else if (ist == IST_STRING_LONG) {
            IONCHECK(_ion_scanner_skip_long_string(scanner));
        }
        else if (ist == IST_CLOB_PLAIN) {
            IONCHECK(_ion_scanner_skip_plain_clob(scanner));
        }
        else if (ist == IST_CLOB_LONG) {
            IONCHECK(_ion_scanner_skip_long_clob(scanner));
        }
        else if (ist == IST_BLOB) {
            IONCHECK(_ion_scanner_skip_blob(scanner));
        }
        else if (ist == IST_SEXP) {
            IONCHECK(_ion_scanner_skip_sexp(scanner));
        }
        else if (ist == IST_LIST) {
            IONCHECK(_ion_scanner_skip_list(scanner));
        }
        else if (ist == IST_STRUCT) {
            IONCHECK(_ion_scanner_skip_struct(scanner));
        }
        else  if (ist == IST_EOF) {
            /* do nothing, but eof is fine */
        }
        else {
            FAILWITH(IERR_PARSER_INTERNAL);
        }
    }
    else {
        SUCCEED();
    }

    // the value should be gone now
    scanner->_value_location = SVL_NONE;

    iRETURN;
}

iERR _ion_scanner_skip_plain_string(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case '"':
            SUCCEED();
        case '\\':
            // we just ignore the char after the escape, which is enough to
            // handle escaped chars correctly (including escaped / and ")
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            break;
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);
        default:
            break;
        }
    }

    iRETURN;
}

iERR _ion_scanner_skip_long_string(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        // find the end of the current part of the lonng
        // string
        IONCHECK(_ion_scanner_skip_one_long_string(scanner));

        // see if there is another one adjacent to this
        // which would be part of the same value. we do this
        // by peeking ahead for a triple quote
        IONCHECK(_ion_scanner_read_past_whitespace(scanner, &c));
        if (c == '\'') {
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '\'') {
                IONCHECK(_ion_scanner_read_char(scanner, &c));
                if (c == '\'') {
                    // we found the triple quote, loop around to consume
                    // the characters that are quoted and try for another
                    continue;
                }
                IONCHECK(_ion_scanner_unread_char(scanner, c));
                c = '\'';
            }
            IONCHECK(_ion_scanner_unread_char(scanner, c));
            c = '\'';
        }
        // the next token wasn't a triple quote, so we're actually done
        // and here we don't care if it happend to be an EOF
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        SUCCEED();
    }

    iRETURN;
}

iERR _ion_scanner_skip_one_long_string(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case '\'':
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c != '\'') break;
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c != '\'') break;
            SUCCEED();            
        case '\\':
            // we just ignore the char after the escape, which is enough to
            // handle escaped chars correctly (including escaped / and ")
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            break;
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);
        default:
            break;
        }
    }

    iRETURN;
}

iERR _ion_scanner_skip_single_quoted_string(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case '\'':
            SUCCEED();
        case '\\':
            // we just ignore the char after the escape, which is enough to
            // handle escaped chars correctly (including escaped / and ")
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            break;
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);
        default:
            break;
        }
    }

    iRETURN;
}

iERR _ion_scanner_skip_unknown_lob(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        // we should see a double closing curly brace next
        IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
        switch (c) {
        case '\'':
            // note that a valid triple quoted string looks like an empty
            // single quoted string, followed by a single quoted string,
            // followed by another empty single quoted string
            IONCHECK(_ion_scanner_skip_single_quoted_string(scanner));
            break;
        case '\"':
            IONCHECK(_ion_scanner_skip_plain_clob(scanner));
            break;
        case '}':
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '}') {
                SUCCEED();
            }
            break;
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);
        default:
            break;
        }
    }

    iRETURN;
}

iERR _ion_scanner_skip_plain_clob(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    // consume the string part of the value
    IONCHECK(_ion_scanner_skip_plain_string(scanner));
    
    // we should see a double closing curly brace next
    IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
    if (c == '}') {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == '}') {
            SUCCEED();
        }
    }
    if (c == EOF) {
        FAILWITH(IERR_UNEXPECTED_EOF);
    }
    else {
        FAILWITH(IERR_INVALID_SYNTAX);
    }

    iRETURN;
}

iERR _ion_scanner_skip_long_clob(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    // consume the string part of the value
    IONCHECK(_ion_scanner_skip_long_string(scanner));
    
    // we should see a double closing curly brace next
    IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
    if (c == '}') {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == '}') {
            SUCCEED();
        }
    }
    if (c == EOF) {
        FAILWITH(IERR_UNEXPECTED_EOF);
    }
    else {
        FAILWITH(IERR_INVALID_SYNTAX);
    }


    iRETURN;
}

iERR _ion_scanner_skip_blob(ION_SCANNER *scanner)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case '}':
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c != '}') break;
            SUCCEED();            
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);
        default:
            break;
        }
    }

    iRETURN;
}

iERR _ion_scanner_skip_sexp(ION_SCANNER *scanner)
{
    iENTER;

    IONCHECK(_ion_scanner_skip_container(scanner, ')'));

    iRETURN;
}

iERR _ion_scanner_skip_list(ION_SCANNER *scanner)
{
    iENTER;

    IONCHECK(_ion_scanner_skip_container(scanner, ']'));

    iRETURN;
}

iERR _ion_scanner_skip_struct(ION_SCANNER *scanner)
{
    iENTER;

    IONCHECK(_ion_scanner_skip_container(scanner, '}'));

    iRETURN;
}

iERR _ion_scanner_skip_container(ION_SCANNER *scanner, int close_char)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_scanner_read_past_whitespace(scanner, &c));
just_another_char: // yes this is evil
        switch (c) {
        case '"':
            IONCHECK(_ion_scanner_skip_plain_string(scanner));
            break;
        case '\'':
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '\'') {
                IONCHECK(_ion_scanner_read_char(scanner, &c));
                if (c == '\'') {
                    IONCHECK(_ion_scanner_skip_one_long_string(scanner));
                }
                else {
                    goto just_another_char; // very evil - but I don't want to have to unread end reread this char
                }
            }
            else {
                IONCHECK(_ion_scanner_skip_single_quoted_string(scanner));
            }
            break;
        case '{':
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '{') {
                IONCHECK(_ion_scanner_skip_unknown_lob(scanner));
            }
            else if (c == '}') {
                // do nothing, we've just finished an empty struct
            }
            else {
                IONCHECK(_ion_scanner_skip_container(scanner, '}'));
            }
            break;
        case '[':
            IONCHECK(_ion_scanner_skip_container(scanner, ']'));
            break;
        case '(':
            IONCHECK(_ion_scanner_skip_container(scanner, ')'));
            break;
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);
        default:
            if (c == close_char) {
                SUCCEED();
            }
            break;
        }
    }

    iRETURN;
}

iERR _ion_scanner_read_cached_bytes(ION_SCANNER *scanner, BYTE *buf, SIZE len, SIZE *p_bytes_written)
{
    iENTER;
    BYTE *pb = buf, *sb = scanner->_pending_bytes_pos;

    ASSERT(buf);
    ASSERT(len > 0);
    ASSERT(p_bytes_written);

    while (scanner->_pending_bytes_pos < scanner->_pending_bytes_end && len--) {
        *pb++ = *scanner->_pending_bytes_pos++;
    }

    // see if we emptied the pending bytes out, if so reset the ptrs
    if (scanner->_pending_bytes_pos >= scanner->_pending_bytes_end) {
        scanner->_pending_bytes_pos = scanner->_pending_bytes_end = scanner->_pending_bytes;
    }

    *p_bytes_written = pb - buf;

    SUCCEED();

    iRETURN;
}

iERR _ion_scanner_read_as_string(ION_SCANNER *scanner
                               , BYTE        *buf
                               , SIZE         len
                               , ION_SUB_TYPE ist
                               , SIZE        *p_bytes_written
                               , BOOL        *p_eos_encountered
) {
    iENTER;
    BYTE       *dst = buf;
    SIZE        remaining = len, written;
    BOOL        triple_quote_found, eos_encountered = FALSE;

    ASSERT(scanner);
    ASSERT(buf);
    ASSERT(len > 0);
    ASSERT(p_bytes_written);
    ASSERT(p_eos_encountered);
    ASSERT(scanner->_value_location == SVL_IN_STREAM);

    if (ist == IST_SYMBOL_PLAIN) {
        IONCHECK(_ion_scanner_read_as_symbol(scanner, dst, remaining, &written));
        dst += written;
        remaining -= written;
        eos_encountered = TRUE;
    }
    else if (ist == IST_SYMBOL_EXTENDED) {
        IONCHECK(_ion_scanner_read_as_extended_symbol(scanner, dst, remaining, &written));
        dst += written;
        remaining -= written;
        eos_encountered = TRUE;
    }
    else {
        int terminator;
        IONCHECK(_ion_scanner_get_terminator_for_sub_type(ist, &terminator));

        // we loop over read as string at least once, but more if this is a long clob (which
        // is zero or more triple quoted string (but we know we have at least one)
        for (;;) {
            IONCHECK(_ion_scanner_read_as_string_to_quote(scanner, dst, remaining, ist, &written, &eos_encountered));
            dst += written;
            remaining -= written;
            if (!eos_encountered) {
                // we ran out of space in the buffer before we hit the end of the string
                break;
            }
            if (terminator != -1) {
                // we only loop for a triple quoted strings, this one is a plain clob so it's not possible
                break;
            }
            // for a triple quoted string there might be another segment waiting
            IONCHECK(_ion_scanner_peek_for_next_triple_quote(scanner, &triple_quote_found));
            if (!triple_quote_found) {
                // we'll only loop around again to read another triple quoted string
                // if we actually find another one
                break;
            }
        }
        if (eos_encountered) {
            if (ist == IST_CLOB_PLAIN || ist  == IST_CLOB_LONG) {
                // with a clob the string reader does not understand it might
                // be in a clob, so it doesn't consume or check for the
                // closing braces, so we have to do that here (since we know
                // it's a clob)
                IONCHECK(_ion_scanner_read_lob_closing_braces(scanner));
            }
        }
    }

    *p_bytes_written = len - remaining;
    // we check eos *after* setting the written length as we will be pushing a null
    // at the end of the value (which will change the "remaining" length, but only on eos
    if (eos_encountered) {
        // we took it out of the stream but we don't know where the chars went 
        // if the caller is passing in the value buffer they set this back to SVL_VALUE_IMAGE
        scanner->_value_location = SVL_NONE;
        PUSH_VALUE_BYTE('\0'); // we null terminate the last (typically only) buffer of the string
    }
    *p_eos_encountered = eos_encountered;

    iRETURN;
}

iERR _ion_scanner_read_as_string_to_quote(ION_SCANNER *scanner, BYTE *buf, SIZE len, ION_SUB_TYPE ist, SIZE *p_bytes_written, BOOL *p_eos_encountered)
{
    iENTER;
    ION_STREAM *stream = scanner->_stream;
    BOOL        is_triple_quote, triple_quote_terminator = FALSE, eos_encountered = FALSE;
    BYTE       *dst = buf;
    SIZE        remaining = len, written;
    int         c, c2;

    ASSERT(scanner);
    ASSERT(stream);
    ASSERT(buf);
    ASSERT(len > 0);
    ASSERT(p_bytes_written);
    ASSERT(p_eos_encountered);
    ASSERT(scanner->_value_location == SVL_IN_STREAM);

    int terminator;
    IONCHECK(_ion_scanner_get_terminator_for_sub_type(ist, &terminator));
    if (terminator == -1) {
        triple_quote_terminator = TRUE;
        terminator = '\'';
    }

    // first we have to check for uncopied utf8 bytes that might have been left
    // behind in a partial copy (where the original read didn't have a big enough
    // buffer to hold the utf8)
    if (scanner->_pending_bytes_end > scanner->_pending_bytes) {
        IONCHECK(_ion_scanner_read_cached_bytes(scanner, buf, remaining, &written));
        remaining -= written;
        if (remaining < 1) {
            *p_bytes_written = written;
            *p_eos_encountered = FALSE;  // we can't tell here this if there are chars remaining or not
            SUCCEED();
        }
        dst += written;
    }

    // until we encounter the terminator read, interpret escape sequences, 
    // interpret utf8, write utf8 char out, count bytes written
    // the terminator is single quote, double quote, triple quote
    while (remaining > 0) {
        IONCHECK(_ion_scanner_read_char_with_validation(scanner, ist, &c));
        switch (c) {
        case EOF:
            FAILWITH(IERR_UNEXPECTED_EOF);

        case NEW_LINE_1: // for the various forms of end of line
        case NEW_LINE_2:
        case NEW_LINE_3:
            if (!triple_quote_terminator) {
                FAILWITH(IERR_NEW_LINE_IN_STRING);
            }
            c = NEW_LINE_1; // All end of line forms are normalized to LF within quoted text.
            break;

        case '\'':
            if (terminator == c) {
                if (triple_quote_terminator) {
                    IONCHECK(_ion_scanner_peek_two_single_quotes(scanner, &is_triple_quote));
                    if (is_triple_quote) {
                        eos_encountered = TRUE;
                        goto end_of_string;
                    }
                }
                else {
                    eos_encountered = TRUE;
                    goto end_of_string;
                }
            }
            break;
            
        case '\"':
            if (terminator == c) {
                eos_encountered = TRUE;
                goto end_of_string;
            }
            break;

        case '\\':
            IONCHECK(_ion_scanner_read_escaped_char(scanner, ist, &c));
            if (IS_EMPTY_ESCAPE_SEQUENCE(c)) goto dont_write_char;
            if (ion_isLowSurrogate(c)) {
                // a loose low surrogate is an invalid character
                // TODO: we may want to ignore this - hmmm
                FAILWITH(IERR_INVALID_UTF8);
            }
            else if (ion_isHighSurrogate(c)) {
                IONCHECK(_ion_scanner_read_char(scanner, &c2));
                if (c2 == '\\') {
                    IONCHECK(_ion_scanner_read_escaped_char(scanner, ist, &c2));
                }
                if (!ion_isLowSurrogate(c2)) FAILWITH(IERR_INVALID_UTF8);
                c = ion_makeUnicodeScalar(c, c2);
            }
            break;
        default:
            if (IS_1_BYTE_UTF8(c)) break;
            if (ion_isLowSurrogate(c)) {
                // a loose low surrogate is an invalid character
                // TODO: we may want to ignore this - hmmm
                FAILWITH(IERR_INVALID_UTF8);
            }
            else if (ion_isHighSurrogate(c)) {
                IONCHECK(_ion_scanner_read_char(scanner, &c2));
                if (c2 == '\\') {
                    IONCHECK(_ion_scanner_read_escaped_char(scanner, ist, &c2));
                }
                if (!ion_isLowSurrogate(c2)) FAILWITH(IERR_INVALID_UTF8);
                c = ion_makeUnicodeScalar(c, c2);
            }
            else {
                // HACK HACK TODO - this handles utf8 bytes but doesn't check them, which we should be doing
                PUSH_VALUE_BYTE(c);
                continue;
            }
            // do we need to check anything else here?
            break;
        }

        // here we write the char to the output buffer either the easy way or the hard way
        if (IS_1_BYTE_UTF8(c)) {
            PUSH_VALUE_BYTE(c);
        }
        else {
            IONCHECK(_ion_scanner_encode_utf8_char(scanner, c, dst, remaining, &written));
            remaining -= written;
            dst += written;
        }

dont_write_char:
        continue;
    }

end_of_string:
    *p_bytes_written = len - remaining;
    *p_eos_encountered = eos_encountered;
    iRETURN;
}

iERR _ion_scanner_read_as_symbol(ION_SCANNER *scanner, BYTE *dst, SIZE len, SIZE *p_bytes_written)
{
    iENTER;
    ION_STREAM *stream = scanner->_stream;
    SIZE        remaining = len;
    int         c;

    ASSERT(scanner);
    ASSERT(stream);
    ASSERT(dst);
    ASSERT(len > 0);
    ASSERT(p_bytes_written);
    ASSERT(scanner->_value_location == SVL_IN_STREAM);


    // if it's not a quoted symbol we just read it in here (note the
    // symbol termination condition is different than the string read
    // can handle: read, we *don't* interpret escape sequences, 
    // we *don't* interpret utf8, count bytes written
    // the terminator is any non-basic symbol char
    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case EOF:
            goto end_of_symbol;

        case EMPTY_ESCAPE_SEQUENCE3:
        case EMPTY_ESCAPE_SEQUENCE2:
        case EMPTY_ESCAPE_SEQUENCE1:
            break;

        default:
            if (!IS_1_BYTE_UTF8(c) || !IS_BASIC_SYMBOL_CHAR(c)) {
                goto end_of_symbol;
            }
            // here we write the char to the output buffer always the easy way 
            PUSH_VALUE_BYTE(c);
            break;
        }
    }

end_of_symbol:
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    *p_bytes_written = len - remaining;
    iRETURN;
}

iERR _ion_scanner_read_as_extended_symbol(ION_SCANNER *scanner, BYTE *buf, SIZE len, SIZE *p_bytes_written)
{
    iENTER;
    BYTE       *dst = buf;
    SIZE        remaining = len;
    int         c;

    ASSERT(scanner);
    ASSERT(buf);
    ASSERT(len > 0);
    ASSERT(p_bytes_written);
    ASSERT(scanner->_value_location == SVL_IN_STREAM);

    // until we encounter the terminator read we just copy the
    // remaining bytes until we encounter a non operator char, then we're done
    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        switch (c) {
        case EOF:
            goto end_of_symbol;
            
        case EMPTY_ESCAPE_SEQUENCE3:
        case EMPTY_ESCAPE_SEQUENCE2:
        case EMPTY_ESCAPE_SEQUENCE1:
            break;
        default:
            if (!IS_1_BYTE_UTF8(c) || !IS_OPERATOR_CHAR(c)) {
                goto end_of_symbol;
            }
            // here we write the char to the output buffer; always the easy way 
            PUSH_VALUE_BYTE(c);
            break;
        }
    }

end_of_symbol:
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    *p_bytes_written = len - remaining;
    iRETURN;
}

// for non-ascii characters this converts to utf8 and writes them out
// this also handles the special forms of new line
iERR _ion_scanner_encode_utf8_char(ION_SCANNER *scanner, int c, BYTE *buf, SIZE remaining, SIZE *p_bytes_written)
{
    iENTER;
    BYTE *pb = buf;
    SIZE  written = 0;

    // if there isn't enough room in the caller buffer we temporarily write
    // into the scanners small byte buffer, we can read the rest out of there later
    if (remaining < ION_utf8_max_length) {
        pb = scanner->_pending_bytes_pos = scanner->_pending_bytes;
    }

    // check for the various special values, like CR LF end of line
    if (c < 0) {
        switch (c) {
        case NEW_LINE_3: /* carraige return */
            *pb++ = '\r';
            break;

        case NEW_LINE_2: /* carraige newline return */
            *pb++ = '\n';
            *pb++ = '\r';
            break;

        case NEW_LINE_1: /* newline */
            *pb++ = '\n';
            break;

        case EMPTY_ESCAPE_SEQUENCE3: /* slash carraige return */
        case EMPTY_ESCAPE_SEQUENCE2: /* slash carraige newline return */
        case EMPTY_ESCAPE_SEQUENCE1: /* slash newline */
            break;

        default:
            FAILWITH(IERR_INVALID_UTF8);
        }
    }
    else if (c <= ION_utf8_1byte_max) {
        // 1 byte unicode character >=   0 and <= 0xff or <= 127)
        // 0yyyyyyy
        *pb++ = c;
    }
    else if (c <= ION_utf8_2byte_max) {
        // 2 byte unicode character >=128 and <= 0x7ff or <= 2047)
        // 5 + 6 == 11 bits
        // 110yyyyy 10zzzzzz
        *pb++ = ION_utf8_2byte_header | (c >> 6);
        *pb++ = ION_utf8_trailing_header | (c & ION_utf8_trailing_bits_mask);
    }
    else if (c <= ION_utf8_3byte_max) {
        // 3 byte unicode character >=2048 and <= 0xffff, <= 65535
        // 4 + 6 + 6 == 16 bits
        // 1110xxxx 10yyyyyy 10zzzzzz
        *pb++ = ION_utf8_3byte_header | (c >> 12);
        *pb++ = ION_utf8_trailing_header | ((c >> 6) & ION_utf8_trailing_bits_mask);
        *pb++ = ION_utf8_trailing_header | (c & ION_utf8_trailing_bits_mask);
    }
    else if (c <= ION_utf8_4byte_max) {
        // 4 byte unicode character > 65535 (0xffff) and <= 2097151 <= 10xFFFFF
        // 3 + 3*6 == 21 bits
        // 11110www 10xxxxxx 10yyyyyy 10zzzzzz
        *pb++ = ION_utf8_4byte_header | (c >> 18);
        *pb++ = ION_utf8_trailing_header | ((c >> 12) & ION_utf8_trailing_bits_mask);
        *pb++ = ION_utf8_trailing_header | ((c >> 6) & ION_utf8_trailing_bits_mask);
        *pb++ = ION_utf8_trailing_header | (c & ION_utf8_trailing_bits_mask);
    }
    else {
        FAILWITH(IERR_INVALID_UTF8);
    }

    // now if we were writing in the scanners buffer copy as much
    // as the caller asked for, the rest remains in the buffer for later
    // and compute the written byte count in either case
    if (remaining < ION_utf8_max_length) {
        written = remaining;
        while (remaining--) {
            *buf++ = *scanner->_pending_bytes_pos++;
        }
        scanner->_pending_bytes_end = pb;
    }
    else {
        written = pb - buf;
    }
    *p_bytes_written = written;

    iRETURN;
}

iERR _ion_scanner_read_escaped_char(ION_SCANNER *scanner, ION_SUB_TYPE ist, int *p_char)
{
    iENTER;
    int c;

    ASSERT(scanner);
    ASSERT(p_char);

    IONCHECK(_ion_scanner_read_char(scanner, &c));

    switch (c) {
    case '0':        //    \u0000  \0  alert NUL
        c = '\0';
        break;
    case NEW_LINE_1:
        c = EMPTY_ESCAPE_SEQUENCE1;
		break;
    case NEW_LINE_2:
        c = EMPTY_ESCAPE_SEQUENCE2;
		break;
    case NEW_LINE_3:
        c = EMPTY_ESCAPE_SEQUENCE3;
		break;
    case 'a':        //    \u0007  \a  alert BEL
        c = '\a';
        break;
    case 'b':        //    \u0008  \b  backspace BS
        c = '\b';
        break;
    case 't':        //    \u0009  \t  horizontal tab HT
        c = '\t';
        break;
    case 'n':        //    \ u000A  \ n  linefeed LF
        c = '\n';
        break;
    case 'f':        //    \u000C  \f  form feed FF 
        c = '\f';
        break;
    case 'r':        //    \ u000D  \ r  carriage return CR  
        c = '\r';
        break;
    case 'v':        //    \u000B  \v  vertical tab VT  
        c = '\v';
        break;
    case '"':        //    \u0022  \"  double quote  
        c = '"';
        break;
    case '\'':       //    \u0027  \'  single quote  
        c = '\'';
        break;
    case '?':        //    \u003F  \?  question mark  
        c = '?';
        break;
    case '\\':       //    \u005C  \\  backslash  
        c = '\\';
        break;
    case '/':        //    \u002F  \/  forward slash nothing  \NL  escaped NL expands to nothing
        c = '/';
        break;
    case 'x':        //    any  \xHH  2-digit hexadecimal unicode character equivalent to \ u00HH
        IONCHECK(_ion_scanner_read_hex_escape_value(scanner, 2, &c));
        break;
    case 'u':        //    any  \ uHHHH  4-digit hexadecimal unicode character
        if (ist == IST_CLOB_PLAIN || ist == IST_CLOB_LONG) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        IONCHECK(_ion_scanner_read_hex_escape_value(scanner, 4, &c));
        break;
    case 'U':        //    any  \ UHHHHHHHH  8-digit hexadecimal unicode character, note max unicode value avoids -1
        if (ist == IST_CLOB_PLAIN || ist == IST_CLOB_LONG) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        IONCHECK(_ion_scanner_read_hex_escape_value(scanner, 8, &c));
        break;
    default:
        FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
    }

    *p_char = c;
    iRETURN;
}

// this expects the caller to gaurantee that there are hex_len character in pb
iERR _ion_scanner_read_hex_escape_value(ION_SCANNER *scanner, int hex_len, int *p_hexchar)
{
    iENTER;
    int c, d, hexchar = 0;

    while( hex_len-- ) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (!IS_1_BYTE_UTF8(c)) FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
        d = _ion_hex_character_value[c];
        // d < 0 happens on overflow
        if (d < 0) FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
        hexchar = hexchar * 16 + d;
    }

    if (hexchar < 0 || hexchar > ION_max_unicode_scalar) {
        FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
    }

    *p_hexchar = hexchar;
    iRETURN;
}

iERR _ion_scanner_peek_for_next_triple_quote(ION_SCANNER *scanner, BOOL *p_triple_quote_found)
{
    iENTER;
    ION_STREAM *stream = scanner->_stream;
    int         c;

    ASSERT(scanner);
    ASSERT(stream);

    // we use lob rules for skipping whitespace after the value
    IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
    if (c == '\'') {
        // and we expect to find two more of them right next to one another
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (c == '\'') {
            // and we expect to find two more of them right next to one another
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '\'') {
                // and we did, so we're done
                *p_triple_quote_found = TRUE;
                SUCCEED();
            }
        }
        IONCHECK(_ion_scanner_unread_char(scanner, c));
        c = '\'';
    }
    IONCHECK(_ion_scanner_unread_char(scanner, c));

    *p_triple_quote_found = FALSE;
 
    iRETURN;
}

iERR _ion_scanner_read_lob_closing_braces(ION_SCANNER *scanner)
{
    iENTER;
    ION_STREAM *stream = scanner->_stream;
    int         c;

    ASSERT(scanner);
    ASSERT(stream);

    // we use lob rules for skipping whitespace after the value
    IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
    if (c != '}') {
        FAILWITH(IERR_INVALID_LOB_TERMINATOR);
    }
    // and we expect to find two of them right next to one another
    IONCHECK(_ion_scanner_read_char(scanner, &c));
    if (c != '}') {
        FAILWITH(IERR_INVALID_LOB_TERMINATOR);
    }

    iRETURN;
}

iERR _ion_scanner_read_as_base64(ION_SCANNER *scanner, BYTE *buf, SIZE len, SIZE *p_bytes_written, BOOL *p_eos_encountered)
{
    iENTER;
    BOOL        eos_encountered = FALSE;
    BYTE       *dst = buf;
    SIZE        remaining = len, written, output_length;
    int         c, b64_value, b64_block;
    int         padding = 0;

    ASSERT(scanner);
    ASSERT(buf);
    ASSERT(len > 0);
    ASSERT(p_bytes_written);
    ASSERT(p_eos_encountered);
    ASSERT(scanner->_value_location == SVL_IN_STREAM);


    // first we have to check for uncopied utf8 bytes that might have been left
    // behind in a partial copy (where the original read didn't have a big enough
    // buffer to hold the full 3 bytes we get from 1 base64 block of 4 characters)
    if (scanner->_pending_bytes_end > scanner->_pending_bytes) {
        IONCHECK(_ion_scanner_read_cached_bytes(scanner, buf, remaining, &written));
        remaining -= written;
        if (remaining < 1) {
            *p_bytes_written = written;
            *p_eos_encountered = FALSE; // we haven't seen the closing curlies yet
            SUCCEED();
        }
        dst += written;
    }

    // now we start processing the character from the input stream
    //
    //  the basic plan is to read 4 actual character (ignoring
    //  whitespace and such, then convert those 4 into 1-3 output
    //  bytes, either into the callers buf or the pending bytes
    //  buffer

    while (remaining) {
        // this doesn't help perf, but whitespace is allowed so there's not 
        // much to do about it (and i'm not overly concerned about the perf 
        // of converting base64 text since it should an unusual case)
        IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
        // this is the point valid time to see a closeing curly bracket
        if (c == '}') {
            IONCHECK(_ion_scanner_read_char(scanner, &c));
            if (c == '}') {
                // and it better be two of them
                eos_encountered = TRUE;
                break;
            }
            FAILWITH(IERR_BAD_BASE64_BLOB);
        }
        if (!IS_1_BYTE_UTF8(c)) FAILWITH(IERR_BAD_BASE64_BLOB);
        b64_block = _Ion_base64_value[c];
        if (b64_block < 0) FAILWITH(IERR_BAD_BASE64_BLOB);

        // character 2 of 4
        IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
        if (!IS_1_BYTE_UTF8(c) || ((b64_value = _Ion_base64_value[c]) < 0)) FAILWITH(IERR_BAD_BASE64_BLOB);
        b64_block <<= 6;
        b64_block |= b64_value;

        // character 3 of 4 (we may hit a trailer at this point)
        IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
        if (!IS_1_BYTE_UTF8(c) || ((b64_value = _Ion_base64_value[c]) < 0)) {
            // if it's not a valid base64 character - it better be a trailing char ('=')
            if (c != ION_BASE64_TRAILING_CHAR) {
                FAILWITH(IERR_BAD_BASE64_BLOB);
            }
            padding = 2;
            // don't care what this is, we'll never read these bits on output 
            b64_value = 0; // but we don't want it to be -1
        }
        b64_block <<= 6;
        b64_block |= b64_value;

        // character 4 of 4 (we may hit a trailer at this point as well)
        IONCHECK(_ion_scanner_read_past_lob_whitespace(scanner, &c));
        if (!IS_1_BYTE_UTF8(c) || ((b64_value = _Ion_base64_value[c]) < 0)) {
            // if it's not a valid base64 character - it better be a trailing char ('=')
            if (c != ION_BASE64_TRAILING_CHAR) {
                FAILWITH(IERR_BAD_BASE64_BLOB);
            }
            if (padding == 0) {
                padding = 1;
            }
            //else {
            //   if (padding == 2) it's still 2
            //}
            // don't care what this is, we'll never read these bits on output 
            b64_value = 0; // but we don't want it to be -1
        }
        else {
            // when we don't have a pad char (i.e. we have a valid char) make sure 
            // we didn't already see see a pad 
            if (padding == 2) FAILWITH(IERR_BAD_BASE64_BLOB);
        }
        b64_block <<= 6;
        b64_block |= b64_value;

        // figure out if there's enough remaining space to write these bytes
        // and if there is claim we did
        output_length = 3 - padding;

        // now actually move the 1-3 bytes into the output buffer
        // note that this expects the bytes to be shifted whether they
        // are present or not, that is the value is high bit justified.

        // we first move as many as we can into the caller buffer
        while (output_length-- && remaining--) {
            *dst++ = (b64_block & 0xff0000) >> 16;
            b64_block <<= 8;
        }

        // and if there's anything left we move it into the scanners temp
        // pending value buffer and bail
        if (output_length > 0) {
            ASSERT(scanner->_pending_bytes_pos == scanner->_pending_bytes);
            dst = scanner->_pending_bytes_pos = scanner->_pending_bytes;
            while (output_length--) {
                *dst++ = (b64_block & 0xff0000) >> 16;
                b64_block <<= 8;
            }
            scanner->_pending_bytes_end = dst;
            break; // break out of the outer while (remaining) loop
        }
    }

    // we've copied all we can, now we just tell the caller what happened
    *p_bytes_written = len - remaining;
    *p_eos_encountered = eos_encountered;
    if (eos_encountered) {
        // we took it out of the stream but we don't know where the chars went 
        // if the caller is passing in the value buffer they set this back to SVL_VALUE_IMAGE
        scanner->_value_location = SVL_NONE;
    }

    iRETURN;
}

// when this is called if we read a '-' sign will be -1 otherwise +1
// c will be the first actual digit (and it must be a digit to get here)
// at this point we might still see an int, a hex int, a binary int, a float, a double, a decimal or a timestamp
// we'll copy bytes into _value_buffer as we process them. they'll be nice and need
// (and not hold page buffers);

iERR _ion_scanner_read_possible_number(ION_SCANNER *scanner, int c, int sign, ION_SUB_TYPE *p_ist)
{
    iENTER;
    ION_SUB_TYPE    t = IST_NONE;
    BYTE           *dst = scanner->_value_buffer;
    SIZE            remaining_before, remaining = scanner->_value_buffer_length;
    BOOL            is_zero;

    ASSERT(isdigit(c));

    // at this point we're moving the value bytes into the value buffer
    // whether it is valid of not, but in either case it won't be in
    // the input stream any longer
    scanner->_value_location = SVL_NONE;

    // push the optionally read sign and the first digit
    if (sign == -1) {
        PUSH_VALUE_BYTE('-');
    }
    PUSH_VALUE_BYTE(c);
    is_zero = (c == '0'); // we need to save this to complain later if someone includes unnessesary leading 0's <sigh>

    IONCHECK(_ion_scanner_read_char(scanner, &c));

    // if we have an x we have a hexadecimal int
    if (c == 'x' || c == 'X') {
        PUSH_VALUE_BYTE(c);
        IONCHECK(_ion_scanner_read_hex_int(scanner, &dst, &remaining));
        t = (sign == -1) ? IST_INT_NEG_HEX : IST_INT_POS_HEX;
    }
    else if (c == 'b' || c == 'B') {
        PUSH_VALUE_BYTE(c);
        IONCHECK(_ion_scanner_read_binary_int(scanner, &dst, &remaining));
        t = (sign == -1) ? IST_INT_NEG_BINARY : IST_INT_POS_BINARY;
    }
    else {
        // we'll use this to check for a 4 digit year if this is a timestamp, 
        // and for the leading zero test (if digits > 1 and a leading 0)
        // the -1 is because we already pushed the first digit on the output buffer
        remaining_before = remaining + 1;  

        // if the char is a digit, it's the leading digits of a decimal, float or the year of a timestamp
        // or the entire decimal integer - so we'll read those digits in until we hit a char that will
        // let us distinguish which it is
        if (IS_1_BYTE_UTF8(c) && (isdigit(c) || c == '_')) {
            if (isdigit(c)) {
                PUSH_VALUE_BYTE(c);
                IONCHECK(_ion_scanner_read_digits_with_underscores(scanner, &dst, &remaining, &c, TRUE));
            }
            else { // c == '_'
                IONCHECK(_ion_scanner_read_digits_with_underscores(scanner, &dst, &remaining, &c, FALSE));
                if ((remaining_before - remaining) == 1) { // Didn't find any more digits after the underscore.
                    FAILWITHMSG(IERR_INVALID_TOKEN_CHAR, "Illegal underscore in number.");
                }
            }
        }

        // we read all the leading digits - we check for timestamp first
        if (c == '-' || c == 'T') {
            // it is a '-' or a 'T' so this has to be a timestamp
            if (sign == -1 || ((remaining_before - remaining) != 4)) {
                // no negative timestamps and a year is 4 digits long
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            IONCHECK(_ion_scanner_read_timestamp(scanner, c, &dst, &remaining, &t));
            // note that read timestamp doesn't overread
        }
        else {
            // so might a decimal point (if decimal, float, or double) before we hit
                            
            // it there was a leading zero we shouldn't have any other digits but we might have a '.'
            // NB that even if it is a '.', it hasn't been pushed yet/
            if (is_zero && (remaining_before - remaining) > 1) {
                // there is more than 1 digit but there is a leading 0
                FAILWITH(IERR_INVALID_LEADING_ZEROS);
            }
            
            // if it's decimal point, read the digits after the decimal, then look
            // for the exponent
            if (c == '.') {
                PUSH_VALUE_BYTE(c);
                IONCHECK(_ion_scanner_read_digits_with_underscores(scanner, &dst, &remaining, &c, FALSE));
                // with a decimal point we'll presume this is a a decimal unless we see a 'e'
                t = IST_DECIMAL;
            }
            else {
                // otherwise we'll presume this is an int until we've checked for an 'e' or 'd'
                t = (sign == -1) ? IST_INT_NEG_DECIMAL : IST_INT_POS_DECIMAL;
            }

            if (c == 'd' || c == 'D') {
                PUSH_VALUE_BYTE(c);
                IONCHECK(_ion_scanner_read_exponent(scanner, &dst, &remaining, &c));
                // ahh, this is a decimal *with* a 'd'
                t = IST_DECIMAL_D;
            }
            else if (c == 'e' || c == 'E') {
                PUSH_VALUE_BYTE(c);
                IONCHECK(_ion_scanner_read_exponent(scanner, &dst, &remaining, &c));
                // it's a float 64 with a 'e'
                t = IST_FLOAT_64;
            }
            // POSSIBLITY: else if (c == 'f' || c == 'F') {
            // POSSIBLITY:     PUSH_VALUE_BYTE(c);
            // POSSIBLITY:     IONCHECK(_ion_scanner_read_exponent(scanner, &dst, &remaining, &c));
            // POSSIBLITY:     t = IST_FLOAT_32;
            // POSSIBLITY: }
            
            // in all these cases we have read 1 character too far
            IONCHECK(_ion_scanner_unread_char(scanner, c));
        }
    }

    // we have a good_value, set up the value state to reflect this
    PUSH_VALUE_BYTE('\0');
    scanner->_value_location     = SVL_VALUE_IMAGE;
    scanner->_value_image.value  = scanner->_value_buffer;
    scanner->_value_image.length = scanner->_value_buffer_length - remaining - 1; // we don't count the null terminator we also pushed onto the end of the value string
    *p_ist = t;
    SUCCEED();

    iRETURN;
}

iERR _ion_scanner_read_radix_int(ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, int *p_char, ION_INT_RADIX radix, BOOL underscore_allowed)
{
    iENTER;
    int   c, remaining = *p_remaining;
    BYTE *dst = *p_dst;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (!IS_1_BYTE_UTF8(c)) {
            break;
        }
        if (c == '_') {
            if (!underscore_allowed) {
                FAILWITHMSG(IERR_INVALID_TOKEN_CHAR, "Illegal underscore in number.");
            }
            underscore_allowed = FALSE;
            continue; // Do not append the underscore.
        }
        if (!IS_RADIX_CHAR(c, radix)) {
            break;
        }
        PUSH_VALUE_BYTE(c);
        underscore_allowed = TRUE;
    }

    if (dst != *p_dst && !underscore_allowed) {
        FAILWITHMSG(IERR_INVALID_TOKEN_CHAR, "Illegal underscore in number.")
    }

    *p_char = c;
    *p_remaining = remaining;
    *p_dst = dst;

    iRETURN;
}

iERR _ion_scanner_read_hex_int(ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining)
{
    iENTER;
    int c;
    IONCHECK(_ion_scanner_read_radix_int(scanner, p_dst, p_remaining, &c, ION_INT_HEX, FALSE));
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    iRETURN;
}

iERR _ion_scanner_read_binary_int(ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining)
{
    iENTER;
    int c;
    IONCHECK(_ion_scanner_read_radix_int(scanner, p_dst, p_remaining, &c, ION_INT_BINARY, FALSE));
    IONCHECK(_ion_scanner_unread_char(scanner, c));
    iRETURN;
}

iERR _ion_scanner_read_digits(ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, int *p_char)
{
    iENTER;
    BYTE *dst      = *p_dst;
    SIZE remaining = *p_remaining;
    int  c;

    for (;;) {
        IONCHECK(_ion_scanner_read_char(scanner, &c));
        if (!IS_1_BYTE_UTF8(c) || !isdigit(c)) {
            break;
        }
        PUSH_VALUE_BYTE(c);
    }

    *p_char = c;
    *p_remaining = remaining;
    *p_dst = dst;

    iRETURN;
}

iERR _ion_scanner_read_digits_with_underscores(ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, int *p_char, BOOL underscore_allowed)
{
    iENTER;
    IONCHECK(_ion_scanner_read_radix_int(scanner, p_dst, p_remaining, p_char, ION_INT_DECIMAL, underscore_allowed));
    iRETURN;
}

iERR _ion_scanner_read_exponent(ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, int *p_char)
{
    iENTER;
    BYTE *dst      = *p_dst;
    SIZE remaining = *p_remaining;
    int  c;

    // the first char following the 'e' ('d' or other) is either a sign or a digit
    IONCHECK(_ion_scanner_read_char(scanner, &c));
    if (!IS_1_BYTE_UTF8(c)) {
        goto past_exponent;
    }
    if (isdigit(c) || c == '-' || c == '+') {
        PUSH_VALUE_BYTE(c);
    }
    else {
        goto past_exponent;
    }

    // from here on out it's only digits, we'll let the digit scanner do it
    IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));

past_exponent:
    *p_char = c;
    *p_remaining = remaining;
    *p_dst = dst;

    iRETURN;
}

iERR _ion_scanner_read_timestamp(ION_SCANNER *scanner, int c, BYTE **p_dst, SIZE *p_remaining, ION_SUB_TYPE *p_ist )
{
    iENTER;
    ION_SUB_TYPE t                      = IST_TIMESTAMP_YEAR;
    BYTE        *dst                    = *p_dst;
    SIZE         remaining              = *p_remaining;
    SIZE         remaining_before;
    BOOL         valid_termination_char = FALSE;
    BOOL         has_time               = FALSE;


    // at this point we have a 4 digit year (or a fail)
    // we should have already checked for the negative sign before we were called
    if (remaining != (scanner->_value_buffer_length - 4)) {
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }

    // so the year has at least a month following it (before a timezone offset)
    if (c == '-') {
        PUSH_VALUE_BYTE(c);

        // read month
        remaining_before = remaining;
        IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
        if ((remaining_before - 2) != remaining) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        t = IST_TIMESTAMP_MONTH;

        if (c == '-') {
            PUSH_VALUE_BYTE(c);

            // read a day
            remaining_before = remaining;
            IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
            if ((remaining_before - 2) != remaining) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            t = IST_TIMESTAMP_DAY;
        }
    }

    // now we're past the date, do we have a timestamp
    if (c == 'T') {
        PUSH_VALUE_BYTE(c);
        
        // read hour
        remaining_before = remaining;
        IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
        if (remaining_before == remaining) {
            // not a digit after the 'T', so: no hours, no time zone offset
            goto check_timestamp_terminator;
        }
        if ((remaining_before - 2) != remaining) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }

        // we have a time value, so we'll have to make sure we have a timezone specifier (below)
        has_time = TRUE;

        // if we have hours we have to have minutes
        if (c != ':') {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        PUSH_VALUE_BYTE(c);

        remaining_before = remaining;
        IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
        if ((remaining_before - 2) != remaining) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }
        t = IST_TIMESTAMP_TIME;

        // now we might have seconds
        if (c == ':') {
            PUSH_VALUE_BYTE(c);

            remaining_before = remaining;
            IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
            if ((remaining_before - 2) != remaining) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
            t = IST_TIMESTAMP_WITH_SECS;

            // we might even have decimal (fractional seconds)
            if (c == '.') {
                PUSH_VALUE_BYTE(c);
                IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
            }
            t = IST_TIMESTAMP_WITH_FRAC_SECS;
        }
    }

    if (c == 'Z' || c == 'z') {
        PUSH_VALUE_BYTE(c);
        //we don't do anything more with the 'z', it's been pushed onto the value
        // but we will be checking the termination character (just after the 'z')
        IONCHECK(_ion_scanner_read_char(scanner, &c));
    }
    else if (c == '+' || c == '-') {
        PUSH_VALUE_BYTE(c);
        // read hour
        remaining_before = remaining;
        IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
        if ((remaining_before - 2) != remaining) {
            FAILWITH(IERR_INVALID_TIMESTAMP);
        }

        // on a timezone offset if we might have minutes
        if (c == ':') {
            PUSH_VALUE_BYTE(c);

            remaining_before = remaining;
            IONCHECK(_ion_scanner_read_digits(scanner, &dst, &remaining, &c));
            if ((remaining_before - 2) != remaining) {
                FAILWITH(IERR_INVALID_TIMESTAMP);
            }
        }
    }
    else if (has_time == TRUE) {
        // we have a time value (at least hours) but not timezone offset - that's a bad timestamp
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }

check_timestamp_terminator:
    // if there wasn't a timezone offset, we'll need this character again later
    IONCHECK(_ion_scanner_unread_char(scanner, c));

    IONCHECK(_ion_scanner_is_value_terminator(scanner, c, &valid_termination_char));
    if (!valid_termination_char) {
        FAILWITH(IERR_INVALID_TIMESTAMP);
    }

    *p_dst       = dst;
    *p_remaining = remaining;
    *p_ist       = t;

    iRETURN;
}

