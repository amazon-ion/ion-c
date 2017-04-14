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
 *   header to support the ion scanner which is called by the ion reader for text
 *   includes declarations for the helper constanst and the ion_scanner_* functions
 */


#ifndef ION_SCANNER_H_
#define ION_SCANNER_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef int ION_PARSER_STATE;

typedef struct _ion_sub_type {
    char             *name;
    ION_TYPE          base_type;
    ION_PARSER_STATE  follow_state;
    uint16_t          flags;       // tags for whether this token is valid to follow a value in a particular container, or if it is a form of null
} ION_SUB_TYPE_STRUCT;


typedef struct _ion_sub_type *ION_SUB_TYPE;

#define PENDING_BYTES_MAX 4 /* 4 is the utf length, base64 needs just 3 */

/** the text scanner handles actual reading of bytes from the input
 *  stream and recognizing tokens, and converting values as necessary.
 *
 *  This is (currently) only a member of the ion_reader_text structure
 *  and not separately allocated.
 *
 *  It holds the input stream, tracks read characters, counts lines
 *  and handles special read and unread cases (such as unicode byte
 *  order mark and different end of line encodings).
 *
 */
typedef struct _ion_scanner
{
    /** this is a copy of the pointer to the ion_stream 
     *  it is handy to have in the ion_scanner routines so
     *  it is unnecessary to pass in the parser and have to
     *  dereference from reader to text parser to stream
     */
    ION_STREAM     *_stream;

    /** If the value had to be read into a buffer to be passed to the user it is stored here.
     *
     */
    ION_STRING      _value_image;
    BYTE           *_value_buffer;
    int             _value_buffer_length;

    /** this keeps track of whether the value is pending in the input stream
     *  (like string sometimes, blob and clob usually) or if it has been copied
     *  into the value image (like numeric types and symbols)
     *
     *  _value_location choices: SVL_NONE, SVL_VALUE_IMAGE, SVL_IN_STREAM
     *
     */
    int             _value_location;

    /** this is set after we have read the first non-whitespace character
     *  during scanner next(). If this is a value, it starts here.
     *
     */
    POSITION        _value_start;

    /** This small buffer is used to hold bytes used during base64 decoding. It is typically used
     *  when a base64 value cross an input page buffer.
     *
     *  or it holds pending bytes while writing out a utf8 character, which can be 4 bytes long
     *
     */
    BYTE            _pending_bytes[PENDING_BYTES_MAX];
    BYTE           *_pending_bytes_pos;
    BYTE           *_pending_bytes_end;

    /** An unread token. We only support 1 unread token. If
     *  the unread token is one which must be cached (in the
     *  text_readers value buffer) then it is expected to be
     *  _generally_ undisturbed by the callers. However the
     *  start location and the value length (when the value is
     *  in the value image string) are, so we have to save
     *  them as well.
     */
    ION_SUB_TYPE    _unread_sub_type;
    int             _unread_value_location; // when we unread we need to 
    SIZE            _unread_value_length;

    /** Used to keep track of the location (line number) of the current token. It's for debugging and error reporting.
     * @see _offset
     *
     */
    int             _line;                    //  = 1;

    /** Used to keep track of the location (column number) of the current token. It's for debugging purpose and error reporting.
     * @see _line
     */
    int             _offset;                  //  = 0;

    /** Internal temporary variable used to keep track of the column number.
     * There are (currently) no states where it's necessary
     * to pre-read over more than a single new line, so we
     * only need 1 saved offset
     *
     */
    int             _saved_offset;            //  = 0;

} ION_SCANNER;


#define NEW_LINE_3             -8 /* carraige return */
#define NEW_LINE_2             -7 /* carraige newline return */
#define NEW_LINE_1             -6 /* newline */
#define IS_NEWLINE_SEQUENCE(c)     (NEW_LINE_1 >= (c) && (c) >= NEW_LINE_3)

#define EMPTY_ESCAPE_SEQUENCE3 -5 /* slash carraige return */
#define EMPTY_ESCAPE_SEQUENCE2 -4 /* slash carraige newline return */
#define EMPTY_ESCAPE_SEQUENCE1 -3 /* slash newline */
#define IS_EMPTY_ESCAPE_SEQUENCE(c)      (EMPTY_ESCAPE_SEQUENCE1 >= (c) && (c) >= EMPTY_ESCAPE_SEQUENCE3)

#define IS_SCANNER_SPECIAL_WHITESPACE(c) (-2 >= (c) && (c) >= -8)

#define SCANNER_EOF             -1

#define IS_1_BYTE_UTF8(x)  (((int)(x) & ~0x7f) == 0)  /* We don't want the high bit set */
#define MAKE_BYTE(x)       ((BYTE)((int)(x) & 0xff))

#define IS_OPERATOR_CHAR(x) (_ion_is_operator_character[(x)])
GLOBAL BOOL  _ion_is_operator_character[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //   0 -   9
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  10 -  19
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  20 -  29
         0,  0,  0,  1,  0,  1,  0,  1,  1,  0,  //  30 -  39  // '!'=33, '#'=35, %'=37, '&'=38
         0,  0,  1,  1,  0,  1,  1,  1,  0,  0,  //  40 -  49  // '*'=42, '+'=43, -'=45, '.'=46, '/'=47
         0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  //  50 -  59  // ';'=59
         1,  1,  1,  1,  1,  0,  0,  0,  0,  0,  //  60 -  69  // '<'=60, '='=61, >'=62, '?'=63, '@'=64
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  70 -  79
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  80 -  89
         0,  0,  0,  0,  1,  0,  1,  0,  0,  0,  //  90 -  99  // '^'=94, '`'=96
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 100 - 109
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 110 - 119
         0,  0,  0,  0,  1,  0,  1,  0,  0,  0,  // 120 - 129  // '|'=124, '~'=126
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 130 - 139
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 140 - 149
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 150 - 159
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 160 - 169
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 170 - 179
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 180 - 189
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 190 - 199
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 200 - 209
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 210 - 219
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 220 - 229
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 230 - 239
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 240 - 249
         0,  0,  0,  0,  0,  0                   // 250 - 255
} 
#endif
;

#define IS_BASIC_SYMBOL_CHAR(x) (_ion_is_basic_symbol_character[(x)])
GLOBAL BOOL  _ion_is_basic_symbol_character[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //   0 -   9
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  10 -  19
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  20 -  29
         0,  0,  0,  0,  0,  0,  1,  0,  0,  0,  //  30 -  39  '$' 36
         0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  //  40 -  49  '0'-'1' 48-49
         1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  //  50 -  59  '2'-'9' 50-57
         0,  0,  0,  0,  0,  1,  1,  1,  1,  1,  //  60 -  69   A-Z is 65-90
         1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  //  70 -  79
         1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  //  80 -  89
         1,  0,  0,  0,  0,  1,  0,  1,  1,  1,  //  90 -  99   a-z is 97-122 (90 is Z), '_' is 95
         1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 100 - 109
         1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  // 110 - 119
         1,  1,  1,  0,  0,  0,  0,  0,  0,  0,  // 120 - 129
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 130 - 139
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 140 - 149
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 150 - 159
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 160 - 169
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 170 - 179
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 180 - 189
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 190 - 199
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 200 - 209
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 210 - 219
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 220 - 229
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 230 - 239
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 240 - 249
         0,  0,  0,  0,  0,  0                   // 250 - 255
} 
#endif
;


#define IS_HEX_CHAR(x) (_ion_hex_character_value[x] != -1)
GLOBAL BOOL  _ion_hex_character_value[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //   0 -   9
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  10 -  19
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  20 -  29
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  30 -  39 
        -1, -1, -1, -1, -1, -1, -1, -1,  0,  1,  //  40 -  49 // 48-57 '0'-'9'
         2,  3,  4,  5,  6,  7,  8,  9, -1, -1,  //  50 -  59
        -1, -1, -1, -1, -1, 10, 11, 12, 13, 14,  //  60 -  69 // 65-70 'A'-'F'
        15, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  70 -  79
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  80 -  89
        -1, -1, -1, -1, -1, -1, -1, 10, 11, 12,  //  90 -  99 // 97-102 'a'-'f'
        13, 14, 15, -1, -1, -1, -1, -1, -1, -1,  // 100 - 109
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 110 - 119
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 120 - 129
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 130 - 139
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 140 - 149
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 150 - 159
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 160 - 169
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 170 - 179
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 180 - 189
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 190 - 199
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 200 - 209
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 210 - 219
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 220 - 229
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 230 - 239
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 240 - 249
        -1, -1, -1, -1, -1, -1                   // 250 - 255
}
#endif
;

#define IS_BINARY_CHAR(x) (_ion_binary_character_value[x] != -1)
GLOBAL BOOL  _ion_binary_character_value[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //   0 -   9
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  10 -  19
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  20 -  29
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  30 -  39
        -1, -1, -1, -1, -1, -1, -1, -1,  0,  1,  //  40 -  49 // 48-40 '0' and '1'
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  50 -  59
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  60 -  69
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  70 -  79
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  80 -  89
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  90 -  99
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 100 - 109
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 110 - 119
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 120 - 129
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 130 - 139
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 140 - 149
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 150 - 159
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 160 - 169
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 170 - 179
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 180 - 189
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 190 - 199
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 200 - 209
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 210 - 219
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 220 - 229
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 230 - 239
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 240 - 249
        -1, -1, -1, -1, -1, -1                   // 250 - 255
}
#endif
;

typedef enum { ION_INT_HEX, ION_INT_BINARY } ION_INT_RADIX;

#define IS_RADIX_CHAR(x, r) ((r == ION_INT_BINARY && IS_BINARY_CHAR(x)) || (r == ION_INT_HEX && IS_HEX_CHAR(x)))

typedef enum { ION_TT_NO,   ION_TT_YES, ION_TT_MAYBE } ION_TERM_TYPE;
//#define ION_IS_MAYBE_VALUE_TERMINATOR(x)  (( _Ion_value_terminators[(x)] == ON_TT_MAYBE ) \x
//                                         || (_Ion_value_terminators[(x)] == ON_TT_YES))
//#define ION_IS_REALLY_VALUE_TERMINATOR(x1, x2) (( _Ion_value_terminators[(x1)] = ION_TT_MAYBE ) \x

//                                        ? _Ion_value_termintors2[(x2)]                   \x
//                                        : (_Ion_value_terminators[(x1)] == ON_TT_YES))
// see: iERR _ion_tokenizer_is_value_terminator(ION_TOKENIZER *t, int c, BOOL p_is_terminator);
//  '\n'  '\r' - also NEW_LINE1, NEW_LINE2 and NEW_LINE3
//  '\0' '\t'  ' ' 
//  '"' '\''  ':' ',' '/'
//  '(' ')'   '['  ']'   '{'  '}'       

GLOBAL ION_TERM_TYPE _Ion_value_terminators[256]
#ifdef INIT_STATICS
= {
      //   0              1             2             3             4                          6             7             8             9
      ION_TT_YES,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_YES,  //   0    9 '\0:0 '\t':9

      ION_TT_YES,    ION_TT_NO,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  10 -  19 '\n':10 '\r':13 (dos compat)
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  20   29

       ION_TT_NO,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,   ION_TT_YES,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_YES,  //  30 - 39 ' ':32, '"':34. '\'':39
      ION_TT_YES,   ION_TT_YES,    ION_TT_NO,    ION_TT_NO,   ION_TT_YES,   ION_TT_NO,    ION_TT_NO, ION_TT_MAYBE,    ION_TT_NO,    ION_TT_NO,  //  40 -  9 '(':40, ')':41, ',':44, '/':47
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,  //  50   59 ':':58

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  60 -  69

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  70 -  79

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  80 -  89

       ION_TT_NO,   ION_TT_YES,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  90 -  99'[':91,  ']':93
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 100 - 09
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 110 - 19
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,  ION_TT_YES,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 120  129 '{':123,'}':125

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 130 - 139

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 140 - 149

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 150 - 159

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 160 - 169

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 170 - 179

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 180 - 189

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 190 - 199

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 200 - 209

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 210 - 219

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 220 - 229

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 230 - 239

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 240 - 249

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO                               // 250 - 255
}
#endif
;
GLOBAL ION_TERM_TYPE _Ion_value_terminators2[256]
#ifdef INIT_STATICS
= {
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //   0    9 '\0'
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  10   19

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  20 - 29
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  30   39

       ION_TT_NO,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,   ION_TT_YES,    ION_TT_NO,    ION_TT_NO,  //  40 - 49 '*':42, '/':47
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  50   59

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  60 - 69
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  70   79

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  80 - 89
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  //  90   99

       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 100 - 09
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 110 - 19
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 120 - 29
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 130 - 39
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 140 - 49
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 150 - 59
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 160 - 69
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 170 - 79
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 180 - 89
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 190 - 99
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 200 - 09
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 210 - 19
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 220 - 29
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 230 - 39
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,  // 240 - 49
       ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,    ION_TT_NO,   ION_TT_NO                                                           // 250 - 55
}
#endif
;

#define MAX_TYPE_NAME_LEN 9
#define MIN_TYPE_NAME_LEN 3


//
// functions decls for the ion text token / value scanner 
//

iERR _ion_scanner_initialize                        (ION_SCANNER *scanner, ION_READER *preader);
iERR _ion_scanner_reset                             (ION_SCANNER *scanner);
iERR _ion_scanner_reset_value                       (ION_SCANNER *scanner);
iERR _ion_scanner_close                             (ION_SCANNER *scanner);

iERR _ion_scanner_next                              (ION_SCANNER *scanner, ION_SUB_TYPE *p_ist);
iERR _ion_scanner_un_next                           (ION_SCANNER *scanner, ION_SUB_TYPE ist);
iERR _ion_scanner_next_actual                       (ION_SCANNER *scanner, ION_SUB_TYPE *p_ist);
iERR _ion_scanner_next_distinguish_lob              (ION_SCANNER *scanner, ION_SUB_TYPE *p_ist);

iERR _ion_scanner_read_char                         (ION_SCANNER *scanner, int *p_char);
iERR _ion_scanner_read_char_with_validation         (ION_SCANNER *scanner, ION_SUB_TYPE ist, int *p_char);
iERR _ion_scanner_read_char_newline_helper          (ION_SCANNER *scanner, int *p_char);
iERR _ion_scanner_read_past_whitespace              (ION_SCANNER *scanner, int *p_char);
iERR _ion_scanner_read_past_lob_whitespace          (ION_SCANNER *scanner, int *p_char);
iERR _ion_scanner_read_past_unicode_byte_order_mark (ION_SCANNER *scanner, int *p_char);
iERR _ion_scanner_read_past_comment                 (ION_SCANNER *scanner, int *p_char);
iERR _ion_scanner_read_to_one_line_comment          (ION_SCANNER *scanner);
iERR _ion_scanner_read_to_end_of_long_comment       (ION_SCANNER *scanner);
iERR _ion_scanner_unread_char                       (ION_SCANNER *scanner, int c);
void _ion_scanner_unread_char_uncount_line          (ION_SCANNER *scanner);


iERR _ion_scanner_peek_double_colon                 (ION_SCANNER *scanner, BOOL *p_is_double_colon);
iERR _ion_scanner_peek_two_single_quotes            (ION_SCANNER *scanner, BOOL *p_rest_of_triple_quote_found);
iERR _ion_scanner_peek_for_null                     (ION_SCANNER *scanner, BOOL *p_is_null, int *p_char);
iERR _ion_scanner_read_null_type                    (ION_SCANNER *scanner, ION_SUB_TYPE *p_ist);
ION_SUB_TYPE _ion_scanner_check_typename            (char *buf, int len);
iERR _ion_scanner_is_value_terminator               (ION_SCANNER *scanner, int c, BOOL *p_is_terminator);
iERR _ion_scanner_peek_keyword                      (ION_SCANNER *scanner, char *tail, BOOL *p_is_match);

iERR _ion_scanner_skip_value_contents               (ION_SCANNER *scanner, ION_SUB_TYPE t);
iERR _ion_scanner_skip_plain_string                 (ION_SCANNER *scanner);
iERR _ion_scanner_skip_long_string                  (ION_SCANNER *scanner);
iERR _ion_scanner_skip_one_long_string              (ION_SCANNER *scanner);
iERR _ion_scanner_skip_single_quoted_string         (ION_SCANNER *scanner);
iERR _ion_scanner_skip_unknown_lob                  (ION_SCANNER *scanner);
iERR _ion_scanner_skip_plain_clob                   (ION_SCANNER *scanner);
iERR _ion_scanner_skip_long_clob                    (ION_SCANNER *scanner);
iERR _ion_scanner_skip_blob                         (ION_SCANNER *scanner);
iERR _ion_scanner_skip_sexp                         (ION_SCANNER *scanner);
iERR _ion_scanner_skip_list                         (ION_SCANNER *scanner);
iERR _ion_scanner_skip_struct                       (ION_SCANNER *scanner);
iERR _ion_scanner_skip_container                    (ION_SCANNER *scanner, int close_char);

iERR _ion_scanner_read_cached_bytes                 (ION_SCANNER *scanner, BYTE *buf, SIZE len, SIZE *p_bytes_written);

iERR _ion_scanner_read_as_string                    (ION_SCANNER *scanner, BYTE *buf, SIZE len, ION_SUB_TYPE ist, SIZE *p_bytes_written, BOOL *p_eos_encountered);
iERR _ion_scanner_read_as_string_to_quote           (ION_SCANNER *scanner, BYTE *buf, SIZE len, ION_SUB_TYPE ist, SIZE *p_bytes_written, BOOL *p_eos_encountered);
iERR _ion_scanner_read_as_symbol                    (ION_SCANNER *scanner, BYTE *dst, SIZE len, SIZE *p_bytes_written);
iERR _ion_scanner_read_as_extended_symbol           (ION_SCANNER *scanner, BYTE *buf, SIZE len, SIZE *p_bytes_written);
iERR _ion_scanner_encode_utf8_char                  (ION_SCANNER *scanner, int c, BYTE *buf, SIZE remaining, SIZE *p_bytes_written);

iERR _ion_scanner_read_escaped_char                 (ION_SCANNER *scanner, ION_SUB_TYPE ist, int *p_char);
iERR _ion_scanner_read_hex_escape_value             (ION_SCANNER *scanner, int hex_len, int *p_hexchar);
iERR _ion_scanner_peek_for_next_triple_quote        (ION_SCANNER *scanner, BOOL *p_triple_quote_found);
iERR _ion_scanner_read_lob_closing_braces           (ION_SCANNER *scanner);

iERR _ion_scanner_read_as_base64                    (ION_SCANNER *scanner, BYTE *buf, SIZE len, SIZE *p_bytes_written, BOOL *p_eos_encountered);


iERR _ion_scanner_read_possible_number              (ION_SCANNER *scanner, int c, int sign, ION_SUB_TYPE *p_ist);
iERR _ion_scanner_read_radix_int                    (ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, ION_INT_RADIX radix);
iERR _ion_scanner_read_hex_int                      (ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining);
iERR _ion_scanner_read_binary_int                   (ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining);
iERR _ion_scanner_read_digits                       (ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, int *p_char);
iERR _ion_scanner_read_exponent                     (ION_SCANNER *scanner, BYTE **p_dst, SIZE *p_remaining, int *p_char);
iERR _ion_scanner_read_timestamp                    (ION_SCANNER *scanner, int c, BYTE **p_dst, SIZE *p_remaining, ION_SUB_TYPE *p_ist );

#ifdef __cplusplus
}
#endif

#endif

