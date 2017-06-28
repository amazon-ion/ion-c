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

   header to support the ion reader for text
   includes declarations for the ion sub byte values and the ion_read_text_* functions

    These are the structs to support the updated IonC text parser (i.e. the parser which understands the text serialization of an Ion octet stream.)

    The new text parser operates like this (more or less):

        on next - 
            scan until it has read enough characters (aka octets) to recognize the beginning and type of a value - return that type.
            set a mark in the stream on the first significant character, where the value has "noise" characters preceeding the actual
            value (such as "{{", or "'", etc.) these will not be included.
            The type of the recognized value will be recorded and will be more specific than the Ion types (for example different
            types of string or symbol or int). 
            
            // no: The maximum preserved lookahead (aka bytes required to be buffered) should be just 5 bytes (a year and the dash of a 
            // no: timestamp to distinguish it from an int), all other preceeding characters are noise and do not need to be buffered.

            In reality the preserved (buffered) lookahead can be arbitrary since Ion supports arbitrary numeric types. We have
            to read digits until we differentiate int from decimal from float. This can be any number of characters.

            At this point we back the stream up to the beginning of the significant characters and return.

            if the event the recognized value is a symbol we peek ahead to find the terminator. It the symbol is followed by a "::" we
            load the symbol onto our annotation list and repeat.

            note that on following calls to next we may need to scan to the end of the current value before we start our real scan.

        on get<value> - 
            read the characters until the appropriate terminator is encountered.
            for small values - all values of than string, blob, clob or containers - we buffer the bytes and then convert (do we really have to?)
            for blob, clob and string - we copy and convert in one pass.

    If the user calls for the length of some value we can check for some specialized conditions (if we want to). In particular non-ascii
    characters, including escaped characters. If we have a string or symbol or clob with all ascii characters we can byte copy when the
    user chooses to read.

    After a "get length" we also know the end of the value and can skip the scan value in favor of a straight forward seek.


#define TID_NULL       0    
        TID_NULL         "null" ( "." ( "null", "bool", "int", "float", "decimal", "timestamp", "symbol", "string", "clob", "blob", "list", "sexp", "struct" ) )@
#define TID_BOOL       1  
        TID_BOOL         "true" | "false"
#define TID_INT    2    
        TID_POS_INT_1    [1-9][0-9]*
        TID_POS_INT_2    "0x" [0-9,a-f,A-F]+
        TID_NEG_INT_1    "-" [1-9][0-9]*
        TID_NEG_INT_2    "-" "0x"[0-9,a-f,A-F]+
#define TID_FLOAT      4    
        TID_FLOAT_1      <int> "." <positive int> "e" ( "+" | "-" )@ <positive int>
        TID_FLOAT_2      <int> "." <positive int> "f" ( "+" | "-" )@ <positive int>    // is this true? can we use e vs f to distiguish 32 bit vs 64 bit floating point values?
#define TID_DECIMAL    5    
        TID_DECIMAL_1    <int> "." <positive int> 
        TID_DECIMAL_2    <int> "." <positive int> "d" ( "+" | "-" )@ <positive int>
#define TID_TIMESTAMP  6   
        TID_TIMESTAMP    <year> "-" <month> "-" <day> "T" ( <hrs> ":" <min> ( ":" <whole secs> ( "." <fractional secs> )@ )@ )@ ( "z" | "Z" | (( "+" | "-" ) <hrs> ( ":" <mins> )@ ))
#define TID_SYMBOL     7    
        TID_SYMBOL_1     "\'" <string contents> "\'"
        TID_SYMBOL_2     [a-z,A-Z][a-z,A-Z,0-9]*
        TID_SYMBOL_3     <op character>+
#define TID_STRING     8    
        TID_STRING_1      "\"" <string contents> "\""
        TID_STRING_2      "\'\'\'" <string contents> "\'\'\'" ( <WS> "\'\'\'" <string contents> "\'\'\'" )+
#define TID_CLOB       9
        TID_CLOB_1        "{{" <WS> <string_1> <WS> "}}"
        TID_CLOB_2        "{{" <WS> <string_2> <WS> "}}"
#define TID_BLOB      10    
        TID_BLOB          "{{" <WS> <base64 contents> <WS> "}}"
#define TID_LIST      11    
        TID_LIST          "[" ( <WS> <value> ( <WS> "," <WS> <value> )@ ( <WS> "," )@ )@ <WS> "]"
#define TID_SEXP      12    
        TID_SEXP          "(" ( <WS> ( <value> | <symbol_3> ) )@ <WS> ")"
#define TID_STRUCT    13    
        TID_STRUCT        "{" ( <WS> <field> ( <WS> "," <WS> <field> )@ ( <WS> "," )@ )@ <WS> "}"
#define TID_UTA       14    ( <symbol_1> | <symbol_2> ) <WS> "::"

WS                 '\0' '\t' ' ' <newline_1> <newline_2> <newline_3> <comment_1> <comment_2>
newline_1          '\n' 
newline_2          '\r' '\n'
newline_3          '\r'
comment_1          '\\' '\\' [ non-white space ] (newline_1 | newline_2 | newline_3 )
comment_2          '\\' '*' <any character> '*' '\\'
value              ( <uta> )@ ( NULL | BOOL | INT | FLOAT | DECIMAL | TIMESTAMP | SYMBOL | STRING | CLOB | BLOB  | LIST | SEXP | STRUCT )
field              ( <symbol_1> | <symbol_2> ) ":" <value>
string contents    ( <utf8 char> | <esc char> )@
op characters      ( '!'=33, '#'=35, %'=37, '&'=38, '*'=42, '+'=43, -'=45, '.'=46, '/'=47, ';'=59, '<'=60, '='=61, >'=62, '?'=63, '@'=64, '^'=94, '`'=96, '|'=124, '~'=126 )
year               [0-9] [0-9] [0-9] [0-9] 
month              [0-1]@ [0-9] 
day                [0-3]@ [0-9] 
hrs                [0-5]@ [0-9] 
min                [0-5]@ [0-9] 
whole secs         [0-5]@ [0-9] 
fractional secs    [0-9]+
base64 contents    ( ( <base64char> <base64char> <base64char> <base64char> )@ ( ( <base64char> <base64char> <base64char> "=" ) | ( <base64char> <base64char> "==" ) | ( <base64char> "===" ) )@ )
base64char         [a-z,A-Z,0-9,'\','+']

*/

/*
    parser states:

        before uta
        before value

        before sexp uta
        before sexp value

        before list uta
        before list value

        before struct fieldname
        before struct uta
        before struct value

        in value
        in sexp value
        in list value
        in struct value

        after value
        after sexp value
        after list value
        after struct value
        
        error
        eof

    state transitions:

    START -> before uta(DATAGRAM)

    before fieldname(C):                                            // C is container (and it better be STRUCT)
        next():
            <fieldname>     -> [save fieldname], before uta(C)
            "}"             -> after container                      : if C == STRUCT else error
            <eof>           -> eof

    before uta(C):                                                  // C is container
        next():
            <fieldname>     -> error
            <uta>           -> [save uta], before uta(C)
            <value>         -> before value(C)
            ")"             -> after container                      : if C == SEXP else error
            "]"             -> after container                      : if C == LIST else error
            <eof>           -> eof

    before value(C):
        skip value()    -> [scan to end of value], after value(C)
        read partial()  -> [read bytes], in value(C)                : if value is string, clob or blob
        read full()     -> [read bytes], after value(C)             : if value is scalar
        "{"             -> [push(C)], before value(STRUCT)
        "("             -> [push(C)], before value(SEXP)
        "["             -> [push(C)], before value(LIST)
        <eof>           -> eof

    in value(C):
        read partial()  -> in value(C)
        <eov>           -> after value(C)

    before comma(C):
        ","             -> before uta(C)
        "}"             -> 
        "]"             -> 

    after value:
        "}"             -> [P = pop()]
        ")"
        "]"
        ","
        -> before uta

    after container:
        step out()      -> [P = pop()], after value(C)
        next()          -> [return <eof>], after container

    error:
    eof:
        DONE - we do let the caller reset the reader (as in seek)
*/


#ifndef ION_READER_TEXT_H_
#define ION_READER_TEXT_H_

#include <decNumber.h>

#ifdef __cplusplus
extern "C" {
#endif

/** text parser states.
 *
 * interesting transition points are at:
 *
 *       at before bof - really before recognized value
 *       recognized value
 *       partially read value
 *       end of value
 *       at eof
 *
 */

#define IPS_ERROR               -2
#define IPS_EOF                 -1
#define IPS_NONE                 0
#define IPS_BEFORE_UTA           1 /* this is before value is recognized */
#define IPS_BEFORE_FIELDNAME     2 /* this is before value is recognized */

#define IPS_BEFORE_SCALAR        3 /* this is value recognized, not read */
#define IPS_BEFORE_CONTAINER     4 /* this is value recognized, not read */

#define IPS_IN_VALUE             5 /* this is value recognized, partially read, including containers */
#define IPS_AFTER_VALUE          6 /* this is after the value is read, just past last character of value (including punctuation if there is any) */

#define IPS_AFTER_COMMA          7

// these tell us where the scanner value is located
#define SVL_NONE                 0
#define SVL_VALUE_IMAGE          1
#define SVL_IN_STREAM            2
#define SVL_SUB_ION_VALUE        3


// Follow Container Flags, also marks the various nulls
#define FCF_none         0x0000
// This is a valid token to see following a value in a:
#define FCF_DATAGRAM     0x0001
#define FCF_SEXP         0x0002
#define FCF_LIST         0x0004
#define FCF_STRUCT       0x0008

// this is a form of NULL
#define FCF_IS_NULL      0x0010

// this is a form of a number (int float decimal)
#define FCF_IS_NUMBER    0x0020

// this is an intermediate token seen at the end of 
#define FCF_CLOSE_PREV   0x0040

// this is a container
#define FCF_IS_CONTAINER 0x0100

// numeric stop characters
#define NUMERIC_STOP_CHARACTERS "{}[](),\"\' \t\n\r\v\f"

#define IST_FOLLOW_STATE( iii )     ((iii)->follow_state)
#define IST_BASE_TYPE( iii )        ((iii)->base_type)
#define IST_FLAG_IS_ON( iii, fff )  ( ((iii)->flags & (fff)) != 0 )

// provide declarations for the ion sub type "enums"
#define IST_RECORD( NAME, TTT, STATE, FLAGS )  extern ION_SUB_TYPE  NAME ;
#include "ion_sub_type_records.h"


#include "ion_scanner.h"


//
// ion reader (for) text functions
//


// reader level functions
iERR _ion_reader_text_open                      (ION_READER *preader);
iERR _ion_reader_text_open_alloc_buffered_string(ION_READER *preader, SIZE len, ION_STRING *p_string, BYTE **p_buf, SIZE *p_buf_len);
iERR _ion_reader_text_reset                     (ION_READER *preader, ION_TYPE parent_tid, POSITION local_end);
iERR _ion_reader_text_reset_value               (ION_READER *preader);
iERR _ion_reader_text_close                     (ION_READER *preader);

// support for "next" functions
iERR _ion_reader_text_next                      (ION_READER *preader, ION_TYPE *p_value_type);
iERR _ion_reader_text_load_fieldname            (ION_READER *preader, ION_SUB_TYPE *p_ist);
iERR _ion_reader_text_load_utas                 (ION_READER *preader, ION_SUB_TYPE *p_ist);
iERR _ion_reader_text_check_for_system_values_to_skip_or_process(ION_READER *preader, ION_SUB_TYPE ist, BOOL *p_is_system_value );
iERR _ion_reader_text_check_follow_token        (ION_READER *preader);

iERR _ion_reader_text_step_in                   (ION_READER *preader);
iERR _ion_reader_text_step_out                  (ION_READER *preader);

// various forms of "getters" to get information about the readers
// current state, in particular the metadata about the current value
iERR _ion_reader_text_get_depth                 (ION_READER *preader, SIZE *p_depth);
iERR _ion_reader_text_get_type                  (ION_READER *preader, ION_TYPE *p_value_type);
iERR _ion_reader_text_is_null                   (ION_READER *preader, BOOL *p_is_null);
iERR _ion_reader_text_has_any_annotations       (ION_READER *preader, BOOL *p_has_annotations);
iERR _ion_reader_text_has_annotation            (ION_READER *preader, ION_STRING *annotation, BOOL *p_annotation_found);
iERR _ion_reader_text_get_annotation_count      (ION_READER *preader, int32_t *p_count);
iERR _ion_reader_text_get_an_annotation         (ION_READER *preader, int32_t idx, ION_STRING *p_str);
iERR _ion_reader_text_get_an_annotation_sid     (ION_READER *preader, int32_t idx, SID *p_sid);
iERR _ion_reader_text_get_field_name            (ION_READER *preader, ION_STRING **p_pstr);
iERR _ion_reader_text_get_symbol_table          (ION_READER *preader, ION_SYMBOL_TABLE **p_return);
iERR _ion_reader_text_get_field_sid             (ION_READER *preader, SID *p_sid);
iERR _ion_reader_text_get_annotations           (ION_READER *preader, ION_STRING *p_strs, SIZE max_count, SIZE *p_count);
iERR _ion_reader_text_get_annotation_sids       (ION_READER *preader, SID *p_sids, SIZE max_count, SIZE *p_count);
iERR _ion_reader_text_get_value_offset          (ION_READER *preader, POSITION *p_offset);
iERR _ion_reader_text_get_value_length          (ION_READER *preader, SIZE *p_length);

// value getting functions
iERR _ion_reader_text_read_null                 (ION_READER *preader, ION_TYPE *p_value);
iERR _ion_reader_text_read_bool                 (ION_READER *preader, BOOL *p_value);
iERR _ion_reader_text_read_mixed_int_helper     (ION_READER *preader);
iERR _ion_reader_text_read_int32                (ION_READER *preader, int32_t *p_value);
iERR _ion_reader_text_read_int64                (ION_READER *preader, int64_t *p_value);
iERR _ion_reader_text_read_ion_int_helper       (ION_READER *preader, ION_INT *p_value);
iERR _ion_reader_text_read_double               (ION_READER *preader, double *p_value);
//iERR _ion_reader_text_read_float32              (ION_READER *preader, float *p_value);
iERR _ion_reader_text_read_decimal              (ION_READER *preader, decQuad *p_quad, decNumber **p_num);
iERR _ion_reader_text_read_timestamp            (ION_READER *preader, ION_TIMESTAMP *p_value);
iERR _ion_reader_text_read_symbol_sid           (ION_READER *preader, SID *p_value);
iERR _ion_reader_text_read_symbol               (ION_READER *preader, ION_SYMBOL *p_symbol);

// get string functions, these work over value of type string or type symbol
// get length FORCES the value to read into the value_image buffer (which may not be desirable)
iERR _ion_reader_text_get_string_length         (ION_READER *preader, SIZE *p_length);
iERR _ion_reader_text_read_string               (ION_READER *preader, ION_STRING *p_user_str);
iERR _ion_reader_text_load_string_in_value_buffer(ION_READER *preader);
iERR _ion_reader_text_read_string_bytes         (ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length) ;

// get lob value functions, these work over value of type clob or type blob
// get lob size FORCES the value to read into the value_image buffer (which may not be desirable)
iERR _ion_reader_text_get_lob_size              (ION_READER *preader, SIZE *p_length);
iERR _ion_reader_text_read_lob_bytes            (ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length) ;

enum version_marker_result { SUCCESS = 0, ERROR = 1 };
enum version_marker_result _ion_reader_text_parse_version_marker(ION_STRING* version_marker, int* major_version, int* minor_version);

#ifdef __cplusplus
}
#endif

#endif

