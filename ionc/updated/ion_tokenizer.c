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

/*

 the tokenizer cracks the input stream into tokens for
 the ion_parser.  It has a lookahead buffer where tokens
 are pushed.  Tokens have types, start and end positions
 and support up to _TOKEN_LOOKAHEAD_MAX lookahead.

 The tokenizer may also hold a copy of itself for resetting
 which is used in some circumstances - such as when the
 user asked for a count of children.  In this case we have
 to scan ahead and then reset outselves.  It's a very
 expensive operation.

 (currently child count is *not* supported in the C interface
 as such the "save state" code may not be necessary.)

 */

#include "ion_internal.h"
#include <memory.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>


iERR ion_make_tokenizer(hOWNER owner, ION_READER_OPTIONS *p_options, ION_INPUT_STREAM *in, ION_TOKENIZER **p_ret)
{
    iENTER;
    ION_TOKENIZER *tokenizer = NULL;
    int buffer_size, ii;
    BYTE *buffer;

    ASSERT(owner != NULL && in != NULL);

    tokenizer = ion_alloc_with_owner(owner, sizeof(ION_TOKENIZER));
    if (!tokenizer) FAILWITH(IERR_NO_MEMORY);

    memset(tokenizer, 0, sizeof(ION_TOKENIZER));

    tokenizer->_owner = owner;
    tokenizer->_r = in;

    // if this is "handled" stream wrap it with the tokenizers
    // handler so that we can intercept the block transitions
    if (in->fn_block_hander != NULL) {
        tokenizer->_handler = in->fn_block_hander;
        tokenizer->_handler_state = in->handler_state;
        in->fn_block_hander = _ion_tokenizer_block_wrapper;
        in->handler_state = tokenizer;
    }

    tokenizer->_token_lookahead_queue[0]._token = -1; 

    tokenizer->_line = 1;
    tokenizer->_new_line_char = p_options->new_line_char;

    // we need to save this in case we have to clone this tokenizer
    // at which point we won't have the caller around to help us out
    buffer_size = (p_options->chunk_threshold > p_options->symbol_threshold)
                    ? p_options->chunk_threshold
                    : p_options->symbol_threshold;
    for (ii=0; ii<_TOKEN_LOOKAHEAD_MAX; ii++) {
        buffer = (BYTE *)ion_alloc_with_owner(owner, buffer_size);
        if (!buffer) FAILWITH(IERR_NO_MEMORY);
        tokenizer->_token_buffers[ii] = buffer;
    }
    tokenizer->_token_buffer_size = buffer_size;

    *p_ret = tokenizer;
    iRETURN;
}

iERR  ion_tokenizer_close(ION_TOKENIZER *tokenizer)
{
    iENTER;

    memset( tokenizer, 0, sizeof( *tokenizer ));
    SUCCEED();

    iRETURN;
}


// ------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------
// character stream support routines


// tokenizer_read handles new line counting on top of the basic
// stream.  This adds overhead on the character reading, which is
// our inner loop.  Whether this is significant or not remains
// to be seen.
iERR _ion_tokenizer_read_char(ION_TOKENIZER *tokenizer, int *p_char)
{
    iENTER;
    int c;

    ION_READ2(tokenizer->_r, c);
    tokenizer->_offset++;

    if (c == tokenizer->_new_line_char) {
        // there are (currently) no states where it's necessary
        // to pre-read over more than a single new line, so we
        // only need 1 saved offset
        tokenizer->_saved_offset = tokenizer->_offset;
        tokenizer->_line++;
        tokenizer->_offset = 0;
    }

    *p_char = c;
    SUCCEED();

    iRETURN;
}

// this reads over 0 or more byte order marks it is called 
// whenever that input character matches the start of a 
// byte order mark that character will have already been read
iERR _ion_tokenizer_read_puncutation_char_helper(ION_TOKENIZER *tokenizer, int *p_char)
{
    iENTER;

    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, p_char));
        if (*p_char != ION_unicode_byte_order_mark_utf8[1]) {
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, *p_char));
            *p_char = ION_unicode_byte_order_mark_utf8[0];
            SUCCEED();
        }

        IONCHECK(_ion_tokenizer_read_char(tokenizer, p_char));
        if (*p_char != ION_unicode_byte_order_mark_utf8[2]) {
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, *p_char));
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, ION_unicode_byte_order_mark_utf8[1]));
            *p_char = ION_unicode_byte_order_mark_utf8[0];
            SUCCEED();
        }

        // read the byte following the now consumed byte order mark
        IONCHECK(_ion_tokenizer_read_char(tokenizer, p_char));
        if (*p_char != ION_unicode_byte_order_mark_utf8[0]) {
            SUCCEED();
        }
    }

    iRETURN;
}

iERR _ion_tokenizer_read_puncutation_char(ION_TOKENIZER *tokenizer, int *p_char)
{
    iENTER;

    IONCHECK(_ion_tokenizer_read_char(tokenizer, p_char));
    if (*p_char == ION_unicode_byte_order_mark_utf8[0]) {
        IONCHECK(_ion_tokenizer_read_puncutation_char_helper(tokenizer, p_char));
    }
    
    iRETURN;
}

iERR _ion_tokenizer_unread_char(ION_TOKENIZER *tokenizer, int c)
{
    iENTER;

    if (c == tokenizer->_new_line_char) {
        tokenizer->_line--;
        tokenizer->_offset = tokenizer->_saved_offset;
    }

    IONCHECK(ion_input_stream_unread(tokenizer->_r, c));

    iRETURN;
}

// this is an "optimized" read and unread it doesn't
// bother with new line counting and the offset since
// we return at the same point in the steam as when
// we were called
iERR _ion_tokenizer_peek_char(ION_TOKENIZER *tokenizer, int *p_char)
{
    iENTER;
    int c;
    
    ION_READ2(tokenizer->_r, c);
    IONCHECK(ion_input_stream_unread(tokenizer->_r, c));

    *p_char = c;

    iRETURN;
}

// the "mark" routines are used to save the contents of tokens
// whose input image needs to be returned later.  This includes
// int, decimal, float, timestamp, symbol and string.  We don't
// bother with (or can't) "mark" null, bool, blob, clob, or
// any of the container types (since in the tokenizer containers
// are really just the start and end characters)

// since the parser only needs a 1 or 2 token lookahead BUT it
// does the 2 deep lookahead after a string or symbol (looking
// for a colon or double color) we need 2 save buffers.

// one can be the streams "mark" but if scan ahead to the next
// token we have to save the marked value 

iERR _ion_tokenizer_start_mark_at_current(ION_TOKENIZER *tokenizer, int curr_char)
{
    iENTER;
    int c;
    
    IONCHECK(ion_input_stream_unread(tokenizer->_r, curr_char));
    IONCHECK(_ion_tokenizer_start_mark_at_next(tokenizer));
    ION_READ2(tokenizer->_r, c);

    iRETURN;
}

iERR _ion_tokenizer_start_mark_at_next(ION_TOKENIZER *tokenizer) 
{
    iENTER;
    int next = tokenizer->_token_lookahead_next_available;

    IONCHECK(ion_input_stream_set_mark_buffer(tokenizer->_r, tokenizer->_token_buffers[next], tokenizer->_token_buffer_size));
    IONCHECK(ion_input_stream_start_mark(tokenizer->_r));

    iRETURN;
}

// WARNING: this is playing a sneeky trick it's putting the mark
//          information about the just parsed value into the token
//          queue's NEXT token slot expecting the token to get
//          queued shortly.  Today this is the case.
iERR _ion_tokenizer_end_mark_at_current(ION_TOKENIZER *tokenizer, int curr_char) 
{
    iENTER;
    int c;

    IONCHECK(ion_input_stream_unread(tokenizer->_r, curr_char));
    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));
    ION_READ2(tokenizer->_r, c);

    iRETURN;
}

iERR _ion_tokenizer_end_mark_at_next(ION_TOKENIZER *tokenizer) 
{
    iENTER;
    ION_INPUT_STREAM *s = tokenizer->_r;

    IONCHECK(ion_input_stream_end_mark(s));
    IONCHECK(ion_input_stream_get_mark_length(s, &tokenizer->_marked_length));
    IONCHECK(ion_input_stream_get_mark_start(s, &tokenizer->_marked_start));

    tokenizer->_has_marked_value = TRUE;

    iRETURN;
}

iERR _ion_tokenizer_clear_end_mark(ION_TOKENIZER *tokenizer)
{
    iENTER;
    ION_INPUT_STREAM *s = tokenizer->_r;

    IONCHECK(ion_input_stream_clear_end_mark(s));
    tokenizer->_has_marked_value = FALSE;

    iRETURN;
}

/*
iERR _ion_tokenizer_clear_mark(ION_TOKENIZER *tokenizer)
{
    iENTER;
    ION_INPUT_STREAM *s = tokenizer->_r;

    IONCHECK(ion_input_stream_clear_mark(s));
    tokenizer->_has_marked_value = FALSE;

    iRETURN;
}
*/

int _ion_tokenizer_marked_length(ION_TOKENIZER *tokenizer) 
{
    ASSERT( tokenizer && tokenizer->_has_marked_value );
    return tokenizer->_marked_length;
}

BYTE *_ion_tokenizer_marked_start(ION_TOKENIZER *tokenizer) 
{
    ASSERT( tokenizer && tokenizer->_has_marked_value );
    return tokenizer->_marked_start;
}


// ------------------------------------------------------------------------------------

iERR _ion_tokenizer_enqueue_token(ION_TOKENIZER *tokenizer, int token)
{
    iENTER;
    int   next = tokenizer->_token_lookahead_next_available;
    int   len;
    BYTE *start;

    tokenizer->_token_lookahead_queue[next]._token = token;
    tokenizer->_tokens_queued++;

    if (tokenizer->_has_marked_value) {
        // so we marked some data as it went by, go get the reference to it
        IONCHECK(ion_input_stream_get_mark_length(tokenizer->_r, &len));
        IONCHECK(ion_input_stream_get_mark_start(tokenizer->_r, &start));
        IONCHECK(ion_input_stream_clear_mark(tokenizer->_r));
        tokenizer->_token_lookahead_queue[next]._value_start = start;
        tokenizer->_token_lookahead_queue[next]._value_length = len;
        tokenizer->_token_lookahead_queue[next]._has_escape_characters = tokenizer->_has_escape_characters;
    }
    else {
        // here we're going to fake the two tokens we didn't spend time
        // saving from the input stream - maybe we should ??
        switch (token) {
        case TOKEN_COMMA:
            tokenizer->_token_lookahead_queue[next]._value_start = _ion_comma_byte;
            tokenizer->_token_lookahead_queue[next]._value_length = 1;
            break;
        case TOKEN_DOT:
            tokenizer->_token_lookahead_queue[next]._value_start = _ion_dot_byte;
            tokenizer->_token_lookahead_queue[next]._value_length = 1;
            break;
        default:
            tokenizer->_token_lookahead_queue[next]._value_start = NULL;
            tokenizer->_token_lookahead_queue[next]._value_length = 0;
            break;
        }
        tokenizer->_token_lookahead_queue[next]._has_escape_characters = FALSE;
    }

    next++;
    if (next == _TOKEN_LOOKAHEAD_MAX) next = 0;
    if (next == tokenizer->_token_lookahead_current) {
        FAILWITH(_ion_tokenizer_internal_error(
                      tokenizer
                    ,"token queue overflow"
                 )
        );
    }
    tokenizer->_token_lookahead_next_available = next;

    iRETURN;        
}

iERR _ion_tokenizer_dequeue_token(ION_TOKENIZER *tokenizer)
{
    iENTER;

    if (tokenizer->_token_lookahead_current == tokenizer->_token_lookahead_next_available) 
    {
        FAILWITH(_ion_tokenizer_internal_error(
                      tokenizer
                    ,"token queue underflow"
                 )
        );
    }
    tokenizer->_token_lookahead_current++;
        
    if (tokenizer->_token_lookahead_current == _TOKEN_LOOKAHEAD_MAX)
    {
        tokenizer->_token_lookahead_current = 0;
    }

    tokenizer->_tokens_consumed++;

    iRETURN;
}

int _ion_tokenizer_get_queue_count(ION_TOKENIZER *tokenizer)
{
    int count = tokenizer->_token_lookahead_next_available - tokenizer->_token_lookahead_current;
    if (count < 0) {
        count += _TOKEN_LOOKAHEAD_MAX;
    }
    return count;
}

int _ion_tokenizer_queue_position(ION_TOKENIZER *tokenizer, int lookahead) 
{
    int count, pos;

    count = _ion_tokenizer_get_queue_count(tokenizer);

    if (lookahead >= count) return TOKEN_EOF;

    // now that we know it's in the queue figure out where this token is
    pos = tokenizer->_token_lookahead_current + lookahead;
    if (pos >= _TOKEN_LOOKAHEAD_MAX) 
    {
        pos -= _TOKEN_LOOKAHEAD_MAX;
    }

    return pos;
}

//--------------------------------------------------------------------------------------------------

int _ion_tokenizer_peek_token(ION_TOKENIZER *tokenizer, int lookahead) 
{
    int pos = _ion_tokenizer_queue_position(tokenizer, lookahead);
    int token = tokenizer->_token_lookahead_queue[pos]._token;
    return token;
}

int _ion_tokenizer_current_token(ION_TOKENIZER *tokenizer) 
{
    int token = _ion_tokenizer_peek_token(tokenizer, 0);
    return token;
}

// returns any pending token image as a string. It converts escaped 
// characters into correct utf8 sequences (if necessary) This is destructive, 
// once it's a string it doesn't go back to a byte array
iERR ion_tokenizer_get_image_as_string(ION_TOKENIZER *tokenizer, int lookahead, ION_STRING *pstr) 
{
    iENTER;

    int pos = _ion_tokenizer_queue_position(tokenizer, lookahead);
    int len = tokenizer->_token_lookahead_queue[pos]._value_length;
    BYTE *value, *src, *end, *dst, *dst_end;
    int   c, c2;

    ION_STRING_INIT(pstr);

    if (len < 1) {
        SUCCEED();
    }

    value = tokenizer->_token_lookahead_queue[pos]._value_start;

    // this is expensive so we only want to do it if it's really needed
    if (tokenizer->_token_lookahead_queue[pos]._has_escape_characters) {
        // convert the string into the lookaside buffer
        src = value;
        dst = tokenizer->_token_buffers[pos];
        dst_end = dst + tokenizer->_token_buffer_size;
        end = src + len; 
        while (src < end) {
            c = *src++;
            if (c == '\\') {
                IONCHECK(_ion_tokenizer_scan_escaped_char(src, end, &src, &c));
            }
            if (c == EMPTY_ESCAPE_SEQUENCE) {
                // slash followed by new line (so we eat the new line)
                continue;
            }
            if (ION_IS_ASCII_CHARACTER(c)) {
                *dst++ = c;
            }
            else if (ion_isLowSurrogate(c)) {
                // a loose low surrogate is an invalid character
                // TODO: we may want to ignore this - hmmm
                FAILWITH(IERR_INVALID_UTF8);
            }
            else {
                if (ion_isHighSurrogate(c)) {
                    if (src >= end) FAILWITH(IERR_INVALID_UTF8);
                    c2 = *src++;
                    if (c2 == '\\') {
                        IONCHECK(_ion_tokenizer_scan_escaped_char(src, end, &src, &c2));
                    }
                    if (!ion_isLowSurrogate(c2)) FAILWITH(IERR_INVALID_UTF8);
                    c = ion_makeUnicodeScalar(c, c2);
                }
                // this may be an overly aggressive test, but we 
                // we should never run into this limit anyway since
                // the escaped images are always longer than the
                // utf-8 sequence anyway.
                if (dst + ION_UNT8_MAX_BYTE_LENGTH >= dst_end) {
                    FAILWITH(_ion_tokenizer_bad_escape_sequence(tokenizer));
                }
                IONCHECK(_ion_tokenizer_encode_utf8_char(c, dst, &dst));
            }
        }

        // put this back as this is where the data will be later as well
        value = tokenizer->_token_buffers[pos];
        len = (int)(dst - value);   // TODO - this needs 64bit care
        tokenizer->_token_lookahead_queue[pos]._value_start = value;
        tokenizer->_token_lookahead_queue[pos]._value_length = len;

        // we only need to convert this once
        tokenizer->_token_lookahead_queue[pos]._has_escape_characters = FALSE;
    }

    pstr->value  = value;
    pstr->length = len;

    iRETURN;
}

iERR ion_tokenizer_get_image_raw(ION_TOKENIZER *tokenizer, int lookahead, ION_STRING *pstr) 
{
    iENTER;
    int pos = _ion_tokenizer_queue_position(tokenizer, lookahead);
    int len = tokenizer->_token_lookahead_queue[pos]._value_length;

    ION_STRING_INIT(pstr);

    if (len > 0) {
        pstr->value = tokenizer->_token_lookahead_queue[pos]._value_start;
        pstr->length = len;
    }
    SUCCEED();

    iRETURN;
}

// we need the base 64 encoded and clob variants of this
//iERR ion_tokenizer_get_bytes_from_blob(ION_TOKENIZER *tokenizer, int lookahead, ION_STRING *pstr) 
//iERR ion_tokenizer_get_bytes_from_clob(ION_TOKENIZER *tokenizer, int lookahead, ION_STRING *pstr) 


//--------------------------------------------------------------------------------------------------


iERR ion_tokenizer_consume_token(ION_TOKENIZER *tokenizer) 
{
    iENTER;
    int token;

    if (_ion_tokenizer_debug) {
        IONCHECK(ion_tokenizer_lookahead(tokenizer, 0, &token));
        _ion_tokenizer_debug_out(" consume ", ion_tokenizer_get_token_name(token), tokenizer->_line, tokenizer->_offset);
    }

    IONCHECK(_ion_tokenizer_dequeue_token(tokenizer));

    iRETURN;
}


//--------------------------------------------------------------------------------------------------
    

iERR _ion_tokenizer_fill_queue(ION_TOKENIZER *tokenizer)
{
    iENTER;
    int t = -1;
    int c, c2;

    // this resets the "saved token" state after this reset the 
    // tokenizer won't have any string to return
    tokenizer->_has_marked_value = FALSE;
    tokenizer->_has_escape_characters = FALSE;
    IONCHECK(ion_input_stream_clear_mark(tokenizer->_r));
        
    // now we start reading characters to recognize another token
    // and possibly build up a new token value
    for (;;) {
        IONCHECK(_ion_tokenizer_read_puncutation_char(tokenizer, &c));
        switch (c) {
        case -1: 
            t = TOKEN_EOF;
            goto enqueue;
        case ' ':
        case '\t':
        case '\n': case '\r': // new line normalization is NOT handled in read_char
        case '\0': // treat null's a whitespace for C strings
            break;
        case '/':
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
            if (c2 == '/') {
                IONCHECK(_ion_tokenizer_read_single_line_comment(tokenizer));
            } else if (c2 == '*') {
                IONCHECK(_ion_tokenizer_read_block_comment(tokenizer));
            }
            else {
                IONCHECK(_ion_tokenizer_unread_char(tokenizer, c2));
                IONCHECK(_ion_tokenizer_read_symbol_extended(tokenizer, c, &t));
                goto enqueue;
            }
            break;
        case ':':
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
            if (c2 != ':') {
                IONCHECK(_ion_tokenizer_unread_char(tokenizer, c2));
                t = TOKEN_COLON;    
            }
            else {
                t = TOKEN_DOUBLE_COLON;
            }
            goto enqueue;
        case '{':
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
            if (c2 != '{') {
                IONCHECK(_ion_tokenizer_unread_char(tokenizer, c2));
                t = TOKEN_OPEN_BRACE;    
            }
            else {
                t = TOKEN_OPEN_DOUBLE_BRACE;
            }
            goto enqueue;
        case '}':
            t = TOKEN_CLOSE_BRACE;
            goto enqueue;
        case '[':
            t = TOKEN_OPEN_SQUARE;
            goto enqueue;
        case ']':
            t = TOKEN_CLOSE_SQUARE;
            goto enqueue;
        case '(':
            t = TOKEN_OPEN_PAREN;
            goto enqueue;
        case ')':
            t = TOKEN_CLOSE_PAREN;
            goto enqueue;
        case ',':
            t = TOKEN_COMMA;
            goto enqueue;
        case '.':
            t = TOKEN_DOT;
            goto enqueue;
        case '\'':
            IONCHECK(_ion_tokenizer_read_quoted_symbol(tokenizer, c, &t));
            goto enqueue;
        case '<': case '>': case '*': case '=': case '^': case '&': case '|': 
        case '~': case ';': case '!': case '?': case '@': case '%': case '`': 
            IONCHECK(_ion_tokenizer_read_symbol_extended(tokenizer, c, &t));
            goto enqueue;
        case '"':
            IONCHECK(_ion_tokenizer_read_quoted_string(tokenizer, c, &t));
            goto enqueue;
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': 
        case 'g': case 'h': case 'j': case 'i': case 'k': case 'l':
        case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
        case 's': case 't': case 'u': case 'v': case 'w': case 'x': 
        case 'y': case 'z': 
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': 
        case 'G': case 'H': case 'J': case 'I': case 'K': case 'L':
        case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
        case 'S': case 'T': case 'U': case 'V': case 'W': case 'X': 
        case 'Y': case 'Z': 
        case '$': case '_':
            IONCHECK(_ion_tokenizer_read_symbol(tokenizer, c, &t));
            goto enqueue;
        case '0': case '1': case '2': case '3': case '4': 
        case '5': case '6': case '7': case '8': case '9':
            IONCHECK(_ion_tokenizer_read_number(tokenizer, c, &t));
            goto enqueue;
        case '-':
        case '+':
            // see if we have a real number or what might be an extended symbol
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, c2)); // now we have our own c2
            if (isdigit(c2)) {
                IONCHECK(_ion_tokenizer_read_number(tokenizer, c, &t));
            }
            else {
                IONCHECK(_ion_tokenizer_read_symbol_extended(tokenizer, c, &t));
            }
            goto enqueue;
        default:
            FAILWITH(_ion_tokenizer_error(
                          tokenizer
                        , IERR_INVALID_TOKEN
                        ,"invalid character for token start"
                     )
            );
        }
    }

enqueue:
    // really we just enqueue this, we don't care about it otherwise
    IONCHECK(_ion_tokenizer_enqueue_token(tokenizer, t));

    iRETURN;
}


//--------------------------------------------------------------------------------------------------


iERR ion_tokenizer_is_really_double_brace(ION_TOKENIZER *tokenizer, BOOL *p_is_double_brace)
{
    iENTER;
    BOOL is_double;
    int  token, c2;

    // if there isn't a brace on the stack, it can't be a double brace
    IONCHECK(ion_tokenizer_lookahead(tokenizer, 0, &token));
    if (token != TOKEN_CLOSE_BRACE) {
        is_double = FALSE;
        SUCCEED();
    }

    // we have 1 brace so we peek at the next character
    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));

    // either we eat the second half of the double brace
    // or we unread it and we don't have a "real" double brace
    if (c2 != '}')
    {
        IONCHECK(_ion_tokenizer_unread_char(tokenizer, c2));
        is_double = FALSE;
    }
    else {
        is_double = TRUE;
    }

    *p_is_double_brace = is_double;

    iRETURN;
}


//--------------------------------------------------------------------------------------------------


iERR ion_tokenizer_lob_lookahead(ION_TOKENIZER *tokenizer, int *p_char) 
{
    iENTER;
    int c;

    do {
        IONCHECK(_ion_tokenizer_read_puncutation_char(tokenizer, &c));
        if (c < 0) break;
    } while (IS_WHITESPACE_CHAR(c));

    // we don't consume it, we just look at it
    IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));

    *p_char = c;

    iRETURN;
}

iERR ion_tokenizer_lookahead(ION_TOKENIZER *tokenizer, int distance, int *p_token)
{
    iENTER;
    int count = _ion_tokenizer_get_queue_count(tokenizer);;

#ifdef DEBUG
    if (distance < 0 || distance >= _TOKEN_LOOKAHEAD_MAX) {
        FAILWITH(_ion_tokenizer_error(
                     tokenizer
                   , IERR_LOOKAHEAD_OVERFLOW
                   ,"invalid lookahead distance"
                 )
        );
    }
#endif

    // if we don't have enough tokens pre-fetched
    // push some into the queue until we do
    while (distance >= count) {
        IONCHECK(_ion_tokenizer_fill_queue(tokenizer));
        count = _ion_tokenizer_get_queue_count(tokenizer);
    }
    // peek at our pre-fetched token
    *p_token = _ion_tokenizer_peek_token(tokenizer, distance);

    iRETURN;
}


//--------------------------------------------------------------------------------------------------


iERR ion_tokenizer_scan_base64_value(ION_TOKENIZER *tokenizer) 
{
    iENTER;

    int c, len = 0;
    int filler_len = 0;

    IONCHECK(_ion_tokenizer_start_mark_at_next(tokenizer));

    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if (c < 0 || c > 127) {
            FAILWITH(_ion_tokenizer_error(
                         tokenizer
                       , IERR_BAD_BASE64_BLOB
                       ,"invalid character in base64 encoded blob"
                     )
            );
        }
        if (IS_WHITESPACE_CHAR(c)) {
            continue;
        }
        if (!IS_BASE64_CHAR(c)) {
            break;
        }
        len++;
    }

    while (c == ION_BASE64_TRAILING_CHAR) {
        filler_len++;
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    }

    if ( filler_len > 3 ) {
        FAILWITH(_ion_tokenizer_error(
                     tokenizer
                   , IERR_BAD_BASE64_BLOB
                   ,"too many trailing '='s"
                 )
        );
    }
    if ( ((filler_len + len) & 0x3) != 0 ) { // if they're using the filler char the len should be divisible by 4
        FAILWITH(_ion_tokenizer_error(
                     tokenizer
                   , IERR_BAD_BASE64_BLOB
                   ,"base64 encoded length must be evenly divisible by 4"
                 )
        );

    }

    IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));

    iRETURN;
}


//--------------------------------------------------------------------------------------------------


   
iERR _ion_tokenizer_read_single_line_comment(ION_TOKENIZER *tokenizer)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        switch (c) {
        case '\n':
        case -1:
            SUCCEED();
        default:
            break;
        }
    }

    iRETURN;
}

iERR _ion_tokenizer_read_block_comment(ION_TOKENIZER *tokenizer)
{
    iENTER;
    int c;

    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        switch (c) {
        case '*':
            // read back to back '*'s until you hit a '/' and terminate the comment
            // or you see a non-'*'; in which case you go back to the outer loop.
            // this just avoids the read-unread pattern on every '*' in a line of '*' 
            // commonly found at the top and bottom of block comments
            for (;;) {
                IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
                if (c == '/') SUCCEED();
                if (c != '*') break;
            }
            break;
        case -1:
            FAILWITH(_ion_tokenizer_error(
                          tokenizer
                        , IERR_INVALID_TOKEN
                        ,"invalid character for token start"
                     )
            );
        default:
            break;
        }
    }

    iRETURN;
}

   
iERR _ion_tokenizer_read_symbol(ION_TOKENIZER *tokenizer, int c, int *p_symbol)
{
    iENTER;

    IONCHECK(_ion_tokenizer_start_mark_at_current(tokenizer, c));
        
    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        switch (c) {
        case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': 
        case 'g': case 'h': case 'j': case 'i': case 'k': case 'l':
        case 'm': case 'n': case 'o': case 'p': case 'q': case 'r':
        case 's': case 't': case 'u': case 'v': case 'w': case 'x': 
        case 'y': case 'z': 
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': 
        case 'G': case 'H': case 'J': case 'I': case 'K': case 'L':
        case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
        case 'S': case 'T': case 'U': case 'V': case 'W': case 'X': 
        case 'Y': case 'Z': 
        case '$': case '_':
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
            //NOT NEEDED: tokenizer->_saved_symbol.append((char)c);
            break;
        default:
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
            goto loop_done;
        }
    }
loop_done:

    // we don't want the character that bumped us out, but we've 
    // unread it already so we're in the right spot already

    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));

    *p_symbol = TOKEN_SYMBOL1;

    iRETURN;
}

iERR _ion_tokenizer_read_quoted_symbol(ION_TOKENIZER *tokenizer, int c, int *p_symbol)
{
    iENTER;
    BOOL has_escape = FALSE;

    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));

    if (c == '\'') {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if (c == '\'') {
            IONCHECK(_ion_tokenizer_read_quoted_long_string(tokenizer, p_symbol));
            SUCCEED();
        }
        IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
        goto return_symbol2;
    }
        
    // the position should always be correct here
    // since there's no reason to lookahead into a
    // quoted symbol
    IONCHECK(_ion_tokenizer_start_mark_at_current(tokenizer, c));
        
    for (;;) {
        switch (c) {
        case -1: 
            FAILWITH(_ion_tokenizer_unexpected_eof(tokenizer));
        case '\'':
            // here we don't +1 because we are on the closing quote
            IONCHECK(_ion_tokenizer_end_mark_at_current(tokenizer, c));
            goto return_symbol2;
        case '\\':
            has_escape = TRUE;
            IONCHECK(_ion_tokenizer_read_escaped_char(tokenizer, &c, &c));
            break;
        }
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    }

return_symbol2:
    tokenizer->_has_escape_characters = has_escape;
    *p_symbol = TOKEN_SYMBOL2;
    iRETURN;
}

iERR _ion_tokenizer_read_symbol_extended(ION_TOKENIZER *tokenizer, int c, int *p_symbol)
{
    iENTER;

    IONCHECK(_ion_tokenizer_start_mark_at_current(tokenizer, c ));

    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if ((c < 0) || !IS_OPERATOR_CHAR(c)) {
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
            break;
        }
    }

    // we don't want the character that bumped us out but we "unread" it already
    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));
    *p_symbol = TOKEN_SYMBOL3;
    iRETURN;
}

iERR _ion_tokenizer_read_escaped_char(ION_TOKENIZER *tokenizer, int *p_char, int *p_last_byte)
{
    iENTER;
    int c;

    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    *p_last_byte = c;

    switch (c) {
    case '0':        //    \u0000  \0  alert NUL
        c = '\0';
        break;
    case '\n':       // slash-new line the new line eater
        c = EMPTY_ESCAPE_SEQUENCE;
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
        IONCHECK(_ion_tokenizer_read_hex_escape_value(tokenizer, 2, &c, p_last_byte));
        break;
    case 'u':        //    any  \ uHHHH  4-digit hexadecimal unicode character  
        IONCHECK(_ion_tokenizer_read_hex_escape_value(tokenizer, 4, &c, p_last_byte));
        break;
    case 'U':        //    any  \ UHHHHHHHH  8-digit hexadecimal unicode character  
        IONCHECK(_ion_tokenizer_read_hex_escape_value(tokenizer, 8, &c, p_last_byte));
        break;
    default:
        FAILWITH(_ion_tokenizer_bad_escape_sequence(tokenizer));
    }
    *p_char = c;
    iRETURN;
}

iERR _ion_tokenizer_read_hex_escape_value(ION_TOKENIZER *tokenizer, int len, int *p_hexchar, int *p_last_byte)
{
    iENTER;
    int d, c = -1, hexchar = 0;

    while (len > 0) {
        len--;

        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if (c < 0) FAILWITH(_ion_tokenizer_bad_escape_sequence(tokenizer));

        d = _ion_hex_character_value[c];
        if (d < 0) FAILWITH(_ion_tokenizer_bad_escape_sequence(tokenizer));
        hexchar = hexchar * 16 + d;
    }
    *p_last_byte = c;

    if (ion_isSurrogate(hexchar))
    {
        FAILWITH(_ion_tokenizer_bad_character(tokenizer));
    }

    *p_hexchar = hexchar;
    iRETURN;
}


iERR _ion_tokenizer_read_quoted_string(ION_TOKENIZER *tokenizer, int c, int *p_token)
{
    iENTER;
    BOOL has_big_char = FALSE;
    BOOL has_escape = FALSE;
    int  next_char = -1;
    

    // the position should always be correct here
    // since there's no reason to lookahead into a
    // quoted symbol
    IONCHECK(_ion_tokenizer_start_mark_at_next(tokenizer));

    for(;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        switch (c) {
        case -1:
            FAILWITH(_ion_tokenizer_unexpected_eof(tokenizer));
        case '\n': 
            FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        case '"':
            next_char = c;
            goto loop_done;
        case '\\':
            has_escape = TRUE;
            IONCHECK(_ion_tokenizer_read_escaped_char(tokenizer, &c, &next_char));
            break;
        default:
            if (c > TOKEN_BIG_CHARACTER_THRESHOLD) has_big_char = TRUE;
            break;
        }
    }

loop_done:
    IONCHECK(_ion_tokenizer_end_mark_at_current(tokenizer, next_char));
    tokenizer->_has_escape_characters = has_escape;
    *p_token = has_big_char ? TOKEN_STRING1 : TOKEN_STRING3; 
    iRETURN;
}

iERR _ion_tokenizer_read_quoted_long_string(ION_TOKENIZER *tokenizer, int *p_token)
{
    iENTER;
    int  c;
    BOOL has_escape = FALSE;
    BOOL has_big_char = FALSE;
        
    // the position should always be correct here
    // since there's no reason to lookahead into a
    // quoted symbol
    IONCHECK(_ion_tokenizer_start_mark_at_next(tokenizer));

    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        switch (c) {
        case -1: 
            FAILWITH(_ion_tokenizer_unexpected_eof(tokenizer));
        case '\'':
            IONCHECK(_ion_tokenizer_end_mark_at_current(tokenizer, c));

            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
            if (c == '\'') {
                IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
                if (c == '\'') {
                    goto loop_done;
                }
                IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
                c = '\'';  // restore c so that we when we undo just below it's the right character
            }
            IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
            IONCHECK(_ion_tokenizer_clear_end_mark(tokenizer));
            break;
        case '\\':
            has_escape = TRUE;
            IONCHECK(_ion_tokenizer_read_escaped_char(tokenizer, &c, &c));
            break;
        }
        if (c > TOKEN_BIG_CHARACTER_THRESHOLD) has_big_char = TRUE;
    }

loop_done:
    tokenizer->_has_escape_characters = has_escape;
    *p_token = has_big_char ? TOKEN_STRING2 : TOKEN_STRING4;
    iRETURN;
}

    
iERR _ion_tokenizer_read_number(ION_TOKENIZER *tokenizer, int c, int *p_token)
{
    iENTER;
    BOOL has_sign;
    BOOL starts_with_zero;
    int  t, c2, len, year;
        
    // this reads int, float, decimal and timestamp strings
    // anything staring with a +, a - or a digit
    //case '0': case '1': case '2': case '3': case '4': 
    //case '5': case '6': case '7': case '8': case '9':
    //case '-': case '+':

    // we've already read the first character so we have
    // to back it out in saving our starting position
    has_sign = ((c == '-') || (c == '+'));
    IONCHECK(_ion_tokenizer_start_mark_at_current(tokenizer, c));
       
    // first leading digit - to look for hex and
    // to make sure that there is at least 1 digit (or
    // this isn't really a number
    if (has_sign) {
        // if there is a sign character, we just consume it
        // here and get whatever is next in line
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    }
    if (!isdigit(c)) {
        // if it's not a digit, this isn't a number
        // the only non-digit it could have been was a
        // sign character, and we'll have read past that
        // by now
        t = TOKEN_ERROR;
        SUCCEED();
    }

    // the first digit is a special case
    starts_with_zero = (c == '0');
    if (starts_with_zero) {
        // if it's a leading 0 check for a hex value
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
        if (c2 == 'x' || c2 == 'X') {
            IONCHECK(_ion_tokenizer_read_hex_value(tokenizer, has_sign, &t));
            goto succeed;
        }
        // not a next value, back up and try again
        IONCHECK(_ion_tokenizer_unread_char(tokenizer, c2));
    }
        
    // leading digits
    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if (!isdigit(c)) break;
    }
        
    if (c == '.') {
        // so it's probably a float of some sort (unless we change our minds below)
        // but ... if it started with a 0 we only allow that when it's
        // only the single digit (sign not withstanding)
        if (starts_with_zero) {
            IONCHECK(_ion_tokenizer_end_mark_at_current(tokenizer, c));
            if (_ion_tokenizer_marked_length(tokenizer) != (has_sign ? 2 : 1) ) {
                FAILWITH(_ion_tokenizer_bad_token(tokenizer));
            }
            IONCHECK(_ion_tokenizer_clear_end_mark(tokenizer));
        }

        // now it's a decimal or a float
        // read the "fraction" digits
        for (;;) {
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
            if (!isdigit(c)) break;
        }
        t = TOKEN_DECIMAL;
    }
    else if ((c == '-') || (c == 'T')) {
        // this better be a timestamp and it starts with a 4 digit 
        // year followed by a dash and no leading sign
        if (has_sign) FAILWITH(_ion_tokenizer_bad_token(tokenizer));

        IONCHECK(_ion_tokenizer_end_mark_at_current(tokenizer, c));
        if (_ion_tokenizer_marked_length(tokenizer) != TOKEN_TIMESTAMP_YEAR_LENGTH) {
            FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        }
        IONCHECK(_ion_tokenizer_read_timestamp_get_year(tokenizer, &year));

        // we clear it after reading the year since we really read the year 
        // out of the marked area (and you can't do that if you've cleared it)
        IONCHECK(_ion_tokenizer_clear_end_mark(tokenizer));

        IONCHECK(_ion_tokenizer_read_timestamp(tokenizer, c, year, &t));
        goto succeed;
    }
    else {
        // so it's probably an int (unless we change our minds below)
        // but ... if it started with a 0 we only allow that when it's
        // only the single digit (sign not withstanding)
        if (starts_with_zero) {
            IONCHECK(_ion_tokenizer_end_mark_at_current(tokenizer, c));
            len = _ion_tokenizer_marked_length(tokenizer);
            if (len != (has_sign ? 2 : 1)) {
                FAILWITH(_ion_tokenizer_bad_token(tokenizer));
            }
            IONCHECK(_ion_tokenizer_clear_end_mark(tokenizer));
        }
        t = TOKEN_INT;
    }
        
    // see if we have an exponential as in 2d+3
    if (c == 'e' || c == 'E') {
        t = TOKEN_FLOAT;
        IONCHECK(_ion_tokenizer_read_exponent(tokenizer, &c));   // the unused lookahead char
    }
    else if (c == 'd' || c == 'D') {
        t = TOKEN_DECIMAL;
        IONCHECK(_ion_tokenizer_read_exponent(tokenizer, &c));
    }
        
    // all forms of numeric need to stop someplace rational
    // note that -1 (eof) is a fine terminator as well
    if ((c >= 0) && !ION_IS_VALUE_TERMINATOR(c)) {
        FAILWITH(_ion_tokenizer_bad_token(tokenizer));
    }
        
    // we read off the end of the number, so put back
    // what we don't want, but what ever we have is an int
    IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));

succeed:
    *p_token = t;

    iRETURN;
}



    // this returns the lookahead character it didn't use so the caller
    // can unread it
iERR _ion_tokenizer_read_exponent(ION_TOKENIZER *tokenizer, int *p_char)
{
    iENTER;
    int c;

    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));

    if (c == '-' || c == '+') {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    }

    while (isdigit(c)) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    }

    if (c == '.') {
        // read the trailing portion  --- TODO: really?
        while (isdigit(c)) {
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        }
    }

    *p_char = c;
    iRETURN;
}


iERR _ion_tokenizer_read_timestamp_get_year(ION_TOKENIZER *tokenizer, int *p_year)
{
    iENTER;

    int   year = 0;
    int   c, ii, len;
    BYTE *pb;

    pb  = _ion_tokenizer_marked_start(tokenizer);
    len = _ion_tokenizer_marked_length(tokenizer);

    ASSERT( pb != NULL && len >= TOKEN_TIMESTAMP_YEAR_LENGTH);

    pb = pb + len - TOKEN_TIMESTAMP_YEAR_LENGTH;

    for (ii=0; ii<TOKEN_TIMESTAMP_YEAR_LENGTH; ii++) {
        c = pb[ii];
        year *= 10;
        year += c - '0'; // we already checked for digits before we got here
    }

    *p_year = year;
    SUCCEED();
    iRETURN;
}


BOOL _ion_tokenizer_read_timestamp_is_leap_year(int year) 
{
    BOOL is_leap = FALSE;
    int  topdigits;

    if ((year & 0x3) == 0) {
        is_leap = TRUE; // divisible by 4 generally is a leap year
        topdigits = (year / 100);
        if (year - (topdigits * 100) == 0) {
            if ((topdigits & 0x3) == 0) {
                is_leap = TRUE; // but it's still a leap year if we divide by 400 evenly
            }
            else {
                is_leap = FALSE; // but mostly not on even centuries
            }
        }
    }
    return is_leap;
}

iERR _ion_tokenizer_read_timestamp(ION_TOKENIZER *tokenizer, int c, int year, int *p_symbol)
{
    iENTER;
    int c1, c2;
    int month, tmp;

    ASSERT ((c == '-') || (c == 'T'));
    
    // check c for '-' vs 'T'
    if (c == 'T') {
        // we have yearT - just read the termination char and finish
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        goto end_of_days;
    }

    // read month as 2 digits into c1 and c2
    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
    if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));

    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
    if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
    month = (c1 - '0') * 10 + (c2 - '0');
    if (month < 1 || month > 12) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        
    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
    if (c == 'T') {
        // we have year-monthT - just read the termination char and finish
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        goto end_of_days;
    }
    if (c != '-') FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        
    // read day
    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
    if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
    if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));

    // now we validate the month values
    switch (c1) {
    case '0':
        if (c2 < '1' || c2 > '9') FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        break;
    case '1':
        if (c2 < '0' || c2 > '9') FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        break;
    case '2':
        if (c2 < '0' || c2 > '9') FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        // I guess we do try to figure out leap years here
        if (c2 == '9' && month == 2 && !_ion_tokenizer_read_timestamp_is_leap_year(year)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        break;
    case '3':
        if (month == 2) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        if (c2 > '1') FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        if (c2 == '0') break;
        // c2 == '1'
        switch (month) {
        case  2: // feb
        case  4: // apr 
        case  6: // jun
        case  9: // sept
        case 11: // nov 
            FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        default:
            break;
        }
        break;
    default:
        FAILWITH(_ion_tokenizer_bad_token(tokenizer));
    }

    // look for the 't', otherwise we're done (and happy about it)
    if (c == 'T') {
        
        // hour
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
        if (!isdigit(c1)) {
            // as the time is optional after the 'T' check for normal 
            // (valid) termination characters and if found, we're done
            if (c1 < 0 || ION_IS_VALUE_TERMINATOR(c1)) {
                c = c1;  // end_of_days unreads whatever it is we read past, so restore c1 to c for this
                goto end_of_days;
            }
            // any other non-digit is not valid input
            FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        }
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
        if (!isdigit(c2)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        tmp = (c1 - '0')*10 + (c2 - '0');
        if (tmp < 0 || tmp > 23) FAILWITH(_ion_tokenizer_bad_token(tokenizer));

        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if (c != ':') FAILWITH(_ion_tokenizer_bad_token(tokenizer));
            
        // minutes
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
        if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
        if (!isdigit(c2)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        tmp = (c1 - '0')*10 + (c2 - '0');
        if (tmp < 0 || tmp > 59) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if (c == ':') {
            // seconds are optional 
            // and first we'll have the whole seconds
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
            if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
            if (!isdigit(c2)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
            tmp = (c1 - '0')*10 + (c2 - '0');
            if (tmp < 0 || tmp > 59) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
            IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
            if (c == '.') {
                // then the optional fractional seconds
                do {
                    IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
                } while (isdigit(c));
            }
        }

        // since we have a time, we have to have a timezone of some sort 
            
        // the timezone offset starts with a '+' '-' 'Z' or 'z'
        if (c == 'z' || c == 'Z') {
             // read ahead since we'll check for a valid ending in a bit
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        }
        else  if (c != '+' && c != '-') {
             // some sort of offset is required with a time value
             // if it wasn't a 'z' (above) then it has to be a +/- hours { : minutes }
             FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        }
        else {
             // then ... hours of time offset
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
             if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
             if (!isdigit(c2)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             tmp = (c1 - '0')*10 + (c2 - '0');
             if (tmp < 0 || tmp > 23) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
             if (c != ':') {
                 // those hours need their minutesif it wasn't a 'z' (above) then it has to be a +/- hours { : minutes }
                 FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             }
             // and finally the *not* optional minutes of time offset
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c1));
             if (!isdigit(c1)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c2));
             if (!isdigit(c2)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             tmp = (c1 - '0')*10 + (c2 - '0');
             if (tmp < 0 || tmp > 59) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
             IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        }
    }

end_of_days:
    // make sure we ended on a reasonable "note"
    if ((c >= 0) && !ION_IS_VALUE_TERMINATOR(c)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));

    // now do the "paper work" to close out a valid value
    IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));

    *p_symbol = TOKEN_TIMESTAMP;

    iRETURN;
}

    
iERR _ion_tokenizer_read_hex_value(ION_TOKENIZER *tokenizer, BOOL has_sign, int *p_token)
{
    iENTER;
    int c;

    // TODO: why isn't this referenced !!
    has_sign;

    // read the hex digits
    for (;;) {
        IONCHECK(_ion_tokenizer_read_char(tokenizer, &c));
        if ((c < 0) || !IS_HEX_CHAR(c)) break;
    }
        
    // all forms of numeric need to stop someplace rational
    if ((c >= 0) && !ION_IS_VALUE_TERMINATOR(c)) FAILWITH(_ion_tokenizer_bad_token(tokenizer));
        
    IONCHECK(_ion_tokenizer_unread_char(tokenizer, c));
    IONCHECK(_ion_tokenizer_end_mark_at_next(tokenizer));

    *p_token = TOKEN_HEX;
    iRETURN;
}


//--------------------------------------------------------------------------------------------------


iERR ion_tokenizer_consume_token_as_string(ION_TOKENIZER *tokenizer, ION_STRING *pstr)
{
    iENTER;
    IONCHECK(ion_tokenizer_get_image_as_string(tokenizer, 0, pstr));
    IONCHECK(ion_tokenizer_consume_token(tokenizer));
    iRETURN;
}


//iERR ion_tokenizer_get_value_as_bytes(ION_TOKENIZER *tokenizer, ION_STRING *pstr) 
//{
//    iENTER;
//    BYTE *image;
//    int   len, curr;
//
//    ASSERT( _ion_tokenizer_get_queue_count(tokenizer) > 0 );
//
//    curr = tokenizer->_token_lookahead_current;
//
//    image = tokenizer->_token_lookahead_queue[curr]._value_start;
//    if ( image == NULL ) {
//        FAILWITH(IERR_INVALID_STATE);
//    }
//    len = tokenizer->_token_lookahead_queue[curr]._value_length;
//    
//    pstr->value = image;
//    pstr->length = len;
//
//    iRETURN;
//}

/*
// get value as string moves the current marked token value
// into a lookaside buffer while converting escape characters
// if the caller wants this preserved they'll need to copy
// the contents before getting the next token
iERR ion_tokenizer_get_value_as_string(ION_TOKENIZER *tokenizer, ION_STRING *pstr) 
{
    iENTER;
    BYTE *src, *end, *dst, *end_dst;
    int   len, curr, c;

    ASSERT( _ion_tokenizer_get_queue_count(tokenizer) > 0 );

    curr = tokenizer->_token_lookahead_current;

    src = tokenizer->_token_lookahead_queue[curr]._value_start;
    ASSERT( src != NULL );

    len = tokenizer->_token_lookahead_queue[curr]._value_length;
    end = src + len;

    dst = tokenizer->_token_buffers[curr];  // this may be the same as src
    end_dst = dst + tokenizer->_token_buffer_size;

    pstr->value = dst;
    pstr->length = -1;

    while (src < end) {
        c = *src++;
        if (c == '\\') {
            IONCHECK(_ion_tokenizer_scan_escaped_char(src, end, &src, &c));
        }
        if (c == EMPTY_ESCAPE_SEQUENCE) {
            // slash-new line (to eat the new line)
            continue;
        }
        if (ION_IS_ASCII_CHARACTER(c)) {
            *dst++ = c;
        }
        else {
            // this may be an overly aggressive test, but we 
            // we should never run into this limit anyway since
            // the escaped images are always longer than the
            // utf-8 sequence anyway.
            if (dst + ION_UNT8_MAX_BYTE_LENGTH >= end_dst) {
                FAILWITH(_ion_tokenizer_bad_escape_sequence(tokenizer));
            }
            IONCHECK(_ion_tokenizer_encode_utf8_char(c, dst, &dst));
        }
    }

    pstr->length = dst - pstr->value;

    iRETURN;
}
*/

iERR _ion_tokenizer_scan_escaped_char(BYTE *pb, BYTE *end, BYTE **p_next, int *p_char)
{
    iENTER;
    int c;

    ASSERT( pb < end );

    c = *pb++;

    switch (c) {
    case '0':        //    \u0000  \0  alert NUL
        c = '\0';
        break;
    case '\n':       // slash-new line the new line eater
        c = EMPTY_ESCAPE_SEQUENCE;
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
        if (pb + 2 > end) FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
        IONCHECK(_ion_tokenizer_copy_hex_escape_value(pb, 2, &c));
        pb += 2;
        break;
    case 'u':        //    any  \ uHHHH  4-digit hexadecimal unicode character  
        if (pb + 4 > end) FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
        IONCHECK(_ion_tokenizer_copy_hex_escape_value(pb, 4, &c));
        pb += 4;
        break;
    case 'U':        //    any  \ UHHHHHHHH  8-digit hexadecimal unicode character  
        if (pb + 8 > end) FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
        IONCHECK(_ion_tokenizer_copy_hex_escape_value(pb, 8, &c));
        pb += 8;
        break;
    default:
        FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
    }

    *p_next = pb;
    *p_char = c;
    iRETURN;
}

// this expects the caller to gaurantee that there are hex_len character in pb
iERR _ion_tokenizer_copy_hex_escape_value(BYTE *pb, int hex_len, int *p_hexchar)
{
    iENTER;
    int c, d, hexchar = 0;

    while( hex_len-- ) {
        c = *pb++;
        d = _ion_hex_character_value[c];
        // d < 0 happens on overflow
        if (d < 0) FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
        hexchar = hexchar * 16 + d;
    }

    if (ion_isSurrogate(hexchar))
    {
        FAILWITH(IERR_INVALID_ESCAPE_SEQUENCE);
    }

    *p_hexchar = hexchar;
    iRETURN;
}

// the caller is expected gaurantee room in the buffer for any 
// utf character sequence (1 to 4 bytes)
iERR _ion_tokenizer_encode_utf8_char(int c, BYTE *pb, BYTE **p_dst)
{
    iENTER;

    ASSERT( c >= 0 && c <= ION_max_unicode_scalar);
    ASSERT( ion_isSurrogate(c) == FALSE );

    // 1 byte unicode character >=   0 and <= 0xff or <= 127)
    // 0yyyyyyy
    if (c <= ION_utf8_1byte_max) {
        *pb++ = c;
    }

    // 2 byte unicode character >=128 and <= 0x7ff or <= 2047)
    // 5 + 6 == 11 bits
    // 110yyyyy 10zzzzzz
    else if (c <= ION_utf8_2byte_max) {
        *pb++ = ION_utf8_2byte_header | (c >> 6);
        *pb++ = ION_utf8_trailing_header | (c & ION_utf8_trailing_bits_mask);
    }

    // 3 byte unicode character >=2048 and <= 0xffff, <= 65535
    // 4 + 6 + 6 == 16 bits
    // 1110xxxx 10yyyyyy 10zzzzzz
    else if (c <= ION_utf8_3byte_max) {
        *pb++ = ION_utf8_3byte_header | (c >> 12);
        *pb++ = ION_utf8_trailing_header | ((c >> 6) & ION_utf8_trailing_bits_mask);
        *pb++ = ION_utf8_trailing_header | (c & ION_utf8_trailing_bits_mask);
    }

    // 4 byte unicode character > 65535 (0xffff) and <= 2097151 <= 10xFFFFF
    // 3 + 3*6 == 21 bits
    // 11110www 10xxxxxx 10yyyyyy 10zzzzzz
    else { // if (c <= ION_utf8_4byte_max) {
        *pb++ = ION_utf8_4byte_header | (c >> 18);
        *pb++ = ION_utf8_trailing_header | ((c >> 12) & ION_utf8_trailing_bits_mask);
        *pb++ = ION_utf8_trailing_header | ((c >> 6) & ION_utf8_trailing_bits_mask);
        *pb++ = ION_utf8_trailing_header | (c & ION_utf8_trailing_bits_mask);
     }

    *p_dst = pb;    

    SUCCEED();

    iRETURN;
}

int ion_tokenizer_keyword(ION_STRING *pstr)
{
    int len_offset, char_offset;
    char *cp, *cp_k;
    struct keyword_node**by_length, *candidate;

    if (!pstr || !pstr->value || !pstr->length) return KEYWORD_unknown;

    // first we deref our 2 way list by the initial character in the candidate string
    char_offset = *pstr->value - MIN_KEYWORD_CHAR1;
    if (char_offset < 0 || char_offset > MAX_KEYWORD_CHAR1 - MIN_KEYWORD_CHAR1) return KEYWORD_unknown;

    // if it's in rangde see if there is a list of candidates (by length)
    by_length = keywords_by_first_char[char_offset];
    if (by_length == NULL) return KEYWORD_unknown;

    // there is at least one with this starting character - see if the length of the
    // candidate string has a possility of matching
    len_offset = pstr->length - MIN_KEYWORD_LENGTH;
    if (len_offset < 0 || len_offset > MAX_KEYWORD_LENGTH - MIN_KEYWORD_LENGTH) return KEYWORD_unknown;

    // now check the 0 to 3 possibilities for keywords with this the
    // candidates length and starting character
    for (candidate = by_length[len_offset]; 
         candidate; 
         candidate = candidate->next
    ) {
        cp = (char *)pstr->value + 1; // we start with the 2nd character since the array handles the first one
        cp_k = candidate->keyword_tail;
        // I'm expecting strcmp to inlined and fast
        // most times we 
        if (memcmp(cp, cp_k, len_offset + MIN_KEYWORD_LENGTH - 1) == 0) {
            return candidate->keyword_id;
        }
    }
    return KEYWORD_unknown;
}

/**
 * Tokenizer for the Ion text parser in IonTextIterator. This
 * reads bytes and returns the interesting tokens it recognizes
 * or an error.  While, currently, this does UTF-8 decoding
 * as it goes that is unnecessary.  The main entry point is
 * lookahead(n) which gets the token type n tokens ahead (0
 * is the next token).  The tokens type, its starting offset
 * in the input stream and its ending offset in the input stream
 * are cached, so lookahead() can be called repeatedly with
 * little overhead.  This supports a 7 token lookahead and requires
 * a "recompile" to change this limit.  (this could be "fixed"
 * but seems unnecessary at this time - the limit is in 
 * IonTextTokenizer._TOKEN_LOOKAHEAD_MAX which is 1 larger than 
 * the size of the lookahead allowed)  Tokens are consumed by
 * a call to consumeToken, or the helper consumeTokenAsString.
 * The informational interfaces - getValueStart(), getValueEnd()
 * getValueAsString() can be used to get the contents of the
 * value once the caller has decided how to use it. 
 */

// WARNING: in the event the token is illegal this routine is not
//          thread safe since is uses a static character array to 
//          format the "name" of the unrecognized token

// TODO: make this thread safe

char *ion_tokenizer_get_token_name(int t) 
{
    // {invalid_token: x}
    #define GET_TOKEN_NAME_FORMAT_LENGTH 17
    #define GET_TOKEN_NAME_FORMAT_LENGTH_PART_1 16

    static char *invalid_format_head = "{invalid_token: ";
    static char *invalid_format_tail = "}";
    static char  invalid_buffer[17 + MAX_INT32_LENGTH + 1]; // length of head + tail + max int + 1

    char *invalid_buffer_ptr;

    ASSERT( strlen(invalid_format_head) == GET_TOKEN_NAME_FORMAT_LENGTH_PART_1 );
    ASSERT( (strlen(invalid_format_head) + strlen(invalid_format_tail)) == GET_TOKEN_NAME_FORMAT_LENGTH );

    switch (t) {
    case TOKEN_ERROR:        return "TOKEN_ERROR";
    case TOKEN_EOF:          return "TOKEN_EOF";
        
    case TOKEN_INT:          return "TOKEN_INT";
    case TOKEN_HEX:          return "TOKEN_HEX";
    case TOKEN_DECIMAL:      return "TOKEN_DECIMAL";
    case TOKEN_FLOAT:        return "TOKEN_FLOAT";
    case TOKEN_TIMESTAMP:    return "TOKEN_TIMESTAMP";
    case TOKEN_BLOB:         return "TOKEN_BLOB";
        
    case TOKEN_SYMBOL1:      return "TOKEN_SYMBOL1";
    case TOKEN_SYMBOL2:      return "TOKEN_SYMBOL2";
    case TOKEN_SYMBOL3:      return "TOKEN_SYMBOL3";
    case TOKEN_STRING1:      return "TOKEN_STRING1";
    case TOKEN_STRING2:      return "TOKEN_STRING2";
    case TOKEN_STRING3:      return "TOKEN_STRING3";
    case TOKEN_STRING4:      return "TOKEN_STRING4";
        
    case TOKEN_DOT:          return "TOKEN_DOT";
    case TOKEN_COMMA:        return "TOKEN_COMMA";
    case TOKEN_COLON:        return "TOKEN_COLON";
    case TOKEN_DOUBLE_COLON: return "TOKEN_DOUBLE_COLON";
      
    case TOKEN_OPEN_PAREN:   return "TOKEN_OPEN_PAREN";
    case TOKEN_CLOSE_PAREN:  return "TOKEN_CLOSE_PAREN";
    case TOKEN_OPEN_BRACE:   return "TOKEN_OPEN_BRACE";
    case TOKEN_CLOSE_BRACE:  return "TOKEN_CLOSE_BRACE";
    case TOKEN_OPEN_SQUARE:  return "TOKEN_OPEN_SQUARE";
    case TOKEN_CLOSE_SQUARE: return "TOKEN_CLOSE_SQUARE";
        
    case TOKEN_OPEN_DOUBLE_BRACE:  return "TOKEN_OPEN_DOUBLE_BRACE";
    case TOKEN_CLOSE_DOUBLE_BRACE: return "TOKEN_CLOSE_DOUBLE_BRACE";
    default:
        break;
    }

    // crude should be done some where common and useful
    strcpy(invalid_buffer, invalid_format_head);
    invalid_buffer_ptr = _ion_itoa_10(t, invalid_buffer + GET_TOKEN_NAME_FORMAT_LENGTH_PART_1, sizeof(invalid_buffer) - GET_TOKEN_NAME_FORMAT_LENGTH_PART_1);
    strcat(invalid_buffer_ptr, invalid_format_tail);

    return invalid_buffer_ptr;
}

// WARNING: this routine is NOT thread safe - it uses a 
//          static character array to return the input position
//          formatted as text

// TODO: make this thread safe

char *ion_tokenizer_input_position(ION_TOKENIZER *tokenizer) 
{
    // {line: x, offset: x}
    #define INPUT_POSITION_FORMAT_LENGTH 18
    #define INPUT_POSTITON_FORMAT_PART_1_LENGTH 7

    char *pos_1 = "{line: ";
    char *pos_2 = ", offset: ";
    char *pos_3 = "}";

    static char  buf[ 18 + (MAX_INT32_LENGTH * 2) + 1 ]; // len(pos 1 - 3) + 2 int's + 1

    char   *buf_ptr;
    size_t  len;

    ASSERT( strlen(pos_1) == INPUT_POSTITON_FORMAT_PART_1_LENGTH);
    ASSERT( (strlen(pos_1) + strlen(pos_2) + strlen(pos_3)) == INPUT_POSITION_FORMAT_LENGTH);

    strcpy( buf, pos_1 );
    buf_ptr = _ion_itoa_10( tokenizer->_line, (buf + INPUT_POSTITON_FORMAT_PART_1_LENGTH), sizeof(buf) - INPUT_POSTITON_FORMAT_PART_1_LENGTH);
    strcat( buf_ptr, pos_2 );
    len = strlen(buf_ptr);
    buf_ptr = _ion_itoa_10( tokenizer->_offset, buf_ptr + len, sizeof(buf) - len);
    strcat( buf_ptr, pos_3 );

    return buf_ptr;
}


//--------------------------------------------------------------------------------------------------


        

iERR _ion_tokenizer_bad_character(ION_TOKENIZER *tokenizer)
{
    int err = _ion_tokenizer_error(
                       tokenizer
                      ,IERR_INVALID_UTF8_CHAR
                      ,"invalid UTF-8 sequence encountered"
    );
    return err;
}

iERR _ion_tokenizer_unexpected_eof(ION_TOKENIZER *tokenizer)
{
    int err = _ion_tokenizer_error(
                       tokenizer
                      ,IERR_UNEXPECTED_EOF
                      ,"unexpected EOF encountered"
    );
    return err;
}

iERR _ion_tokenizer_bad_escape_sequence(ION_TOKENIZER *tokenizer)
{
    int err = _ion_tokenizer_error(
                       tokenizer
                      ,IERR_INVALID_ESCAPE_SEQUENCE
                      ,"bad escape character encountered"
    );
    return err;
}

iERR _ion_tokenizer_bad_token_start(ION_TOKENIZER *tokenizer, int c)
{
    int err = _ion_tokenizer_error(
                       tokenizer
                      ,IERR_INVALID_TOKEN_START
                      ,"bad character encountered at a token start"
    );

    // TODO: better formatting to use the 'c'
    c;

    return err;
}

iERR _ion_tokenizer_bad_token(ION_TOKENIZER *tokenizer)
{
    int err = _ion_tokenizer_error(
                       tokenizer
                      ,IERR_INVALID_TOKEN_CHAR
                      ,"a bad character was encountered in a token"
    );
    return err;
}

iERR _ion_tokenizer_internal_error(ION_TOKENIZER *tokenizer, char *message)
{
    int err = _ion_tokenizer_error(
                       tokenizer
                      ,IERR_INVALID_STATE
                      ,message
    );
    return err;
}

iERR _ion_tokenizer_error(ION_TOKENIZER *tokenizer, iERR err, char *message)
{
    char temp[100], *temp_ptr;

    // TODO: we need an error stack for return decent messages
    //       that will use the tokenizer

    // sprintf(temp, "error %d", (int)err);
    strcpy(temp, "error ");
    temp_ptr = _ion_itoa_10(err, temp + 6, sizeof(temp) - 6);

    _ion_tokenizer_debug_out(temp_ptr, message, tokenizer->_line, tokenizer->_offset); 

    return err;
}

void _ion_tokenizer_debug_out(char *op, char *token, int line, int offset)
{
    if (!_ion_tokenizer_debug) return;
    printf("debug: %s: %s at line %d, offset %d\n"
          , (char *)op, (char *)token, (int)line, (int)offset
    );
}

iERR _ion_tokenizer_block_wrapper(ION_STREAM *pstream) 
{
    iENTER;
    int   ii, len;
    BYTE *saved, *buf;

    ION_TOKENIZER *tokenizer = (ION_TOKENIZER *)pstream->handler_state;

    ASSERT (tokenizer != NULL);

    // we're about to shift the stream buffer forward so we need
    // to copy any saved values out of the steam's buffer into our
    // own buffers before moving to the next block

    // lookahead is a circular buffer, so we start at our current
    // entry (zero-th entry) and circle around until we hit the
    // next available (which will be used next - ie the end)
    ii = tokenizer->_token_lookahead_current;
    while (ii != tokenizer->_token_lookahead_next_available) {
        if ((len = tokenizer->_token_lookahead_queue[ii]._value_length) > 0) {
            saved = tokenizer->_token_lookahead_queue[ii]._value_start;
            ASSERT( saved != NULL );
            if (saved != tokenizer->_token_buffers[ii]) {
                ASSERT( len <= tokenizer->_token_buffer_size );
                buf = tokenizer->_token_buffers[ii];
                memcpy(buf, saved, len);
                tokenizer->_token_lookahead_queue[ii]._value_start = buf;
            }
        }
        ii++;
        if (ii >= _TOKEN_LOOKAHEAD_MAX) {
            ii = 0;
        }
    }

    // now we can move forward - restore the callers handlers state
    // call it and then set us back in place
    pstream->handler = tokenizer->_handler;
    pstream->handler_state = tokenizer->_handler_state;

    err = (*pstream->handler)(pstream);

    // we want to restore this before handing any error that might
    // have occured
    pstream->handler = _ion_tokenizer_block_wrapper;
    pstream->handler_state = tokenizer;
    if (err && err != IERR_EOF) {
        FAILWITH(err);
    }

    iRETURN;
}
