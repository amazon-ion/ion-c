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
// Ion internal header, non public API's, types, etc
//

#ifndef ION_INTERNAL_H_
#define ION_INTERNAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <stdlib.h>
#include <ctype.h>
#include <memory.h>

#include <ionc/ion_platform_config.h>

#include <ionc/ion.h>
#include "ion_const.h"
#include "ion_index.h"
#include <ionc/ion_stream.h>
#include <ionc/ion_int.h>
#include "ion_binary.h"
#include <ionc/ion_catalog.h>


// ion_alloc uses the size of the object type enum
#include "ion_alloc.h"

// ION OBJECT TYPES
typedef enum {
    ion_type_invalid        = 0,
    ion_type_unknown_writer = 1,
    ion_type_text_writer    = 2,
    ion_type_binary_writer  = 3,
    ion_type_unknown_reader = 4,
    ion_type_text_reader    = 5,
    ion_type_binary_reader  = 6
} ION_OBJ_TYPE;

// the ion header preceeds the *primary* ion object
typedef struct _ion_header {
    ION_ALLOCATION_CHAIN allocation_header;
} ION_HEADER;

#define HEADER(x)           ((ION_HEADER *) (((BYTE *)(x)) - sizeof(ION_HEADER)))

// used in ion_parser.h
#define BYTES_PER_BASE64_BLOCK 3
#define CHARS_PER_BASE64_BLOCK 4

// was: #include "ion_tokenizer.h"
// was: #include "ion_parser.h"
#include "ion_scanner.h"
#include "ion_reader_text.h"
#include "ion_reader_impl.h"
#include "ion_writer_impl.h"
#include "ion_symbol_table_impl.h"
#include "ion_collection_impl.h"
#include "ion_catalog_impl.h"
#include "ion_timestamp_impl.h"
#include "ion_helpers.h"
#include "decQuadHelpers.h"
//#include "hashfn.h"

#include "ion_index.h"
#include "ion_stream_impl.h" 

// DEFAULT_BLOCK_SIZE is moved to ion_alloc.h, included above.
//#define DEFAULT_BLOCK_SIZE (256*1024) /* was 128*1024*1024 */
//#define DEFAULT_BLOCK_SIZE (1024*64)

// TODO: static final boolean _debug_on = false;
#define ASSERT( x ) while (!(x)) { ion_helper_breakpoint(), assert( x ); }
#define STR(x)   #x
#define XSTR(x)  STR(x)

#define SET_FLAG(ii, f, v)  (v ? SET_FLAG_ON(ii, f) : SET_FLAG_OFF(ii, f))
#define SET_FLAG_ON(ii, f)  (ii |= f)
#define SET_FLAG_OFF(ii, f) (ii &= ~f)
#define IS_FLAG_ON(ii, f)   (!!(ii & f))


GLOBAL BOOL _debug_on           INITTO(TRUE);

#define HANDLE_TO_PTR(h, t) ((t *)((void *)(h)))
#define PTR_TO_HANDLE(ptr)  ((void *)(ptr))

// TODO: what should this be?
#define ION_STREAM_MAX_LENGTH ((int64_t)(0x7fffffffffffffff))

// macro for read_byte
#define ION_GET(xh,xb)          if ((xh)->_curr < (xh)->_limit) {               \
                                  (xb) = (*(xh)->_curr++);                      \
                                }                                               \
                                else {                                          \
                                  IONCHECK(ion_stream_read_byte((xh), &(xb)));  \
                                } while(FALSE)

// macro for read_byte
#define OLD__ION_PUT(xh, xb)   if (((xh)->_curr < ((xh)->_buffer + (xh)->_buffer_size) && ((xh)->_dirty_start != NULL) ))  {  \
                                 *((xh)->_curr) = (xb);                                             \
                                  (xh)->_curr++;                                                    \
                                  (xh)->_dirty_length++;                                            \
                               }                                                                    \
                               else {                                                               \
                                 IONCHECK(ion_stream_write_byte((xh), (xb)));                       \
                               } while(FALSE)

#define ION_PUT(xh, xb)        IONCHECK(ion_stream_write_byte_no_checks((xh), (xb)))


// hex characters, just because we need them
GLOBAL char _ion_hex_chars[]
#ifdef INIT_STATICS
= {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 0
}
#endif
;

#define _IS_IN_BYTE_RANGE(x) (((x) & (~0xff)) == 0)

#define IS_BASE64_CHAR(x) (_IS_IN_BYTE_RANGE(x) && _Ion_is_base64_char[(x)] != 0)
GLOBAL char *_Ion_base64_chars INITTO("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");
GLOBAL BOOL  _Ion_is_base64_char[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //   0 -   9
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  10 -  19
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  20 -  29
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  30 -  39
         0,  0,  0,  1,  0,  0,  0,  1,  1,  1,  //  40 -  49  43 == '+', 47 == '/'
         1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  //  50 -  59  48-57 == '0'-'9'
         0,  0,  0,  0,  0,  1,  1,  1,  1,  1,  //  60 -  69  65-90 == 'A'-'Z'
         1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  //  70 -  79
         1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  //  80 -  89
         1,  0,  0,  0,  0,  0,  0,  1,  1,  1,  //  90 -  99  97-122 == 'a'-'z'
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
#define ION_BASE64_TRAILING_CHAR '='
GLOBAL BOOL  _Ion_base64_value[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //   0 -   9
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  10 -  19
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  20 -  29
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  //  30 -  39
        -1, -1, -1, 62, -1, -1, -1, 63, 52, 53,  //  40 -  49  43 == '+', 47 == '/' (here are '0' and '1')
        54, 55, 56, 57, 58, 59, 60, 61, -1, -1,  //  50 -  59  48-57 == '0'-'9'     ('2' - '9')
        -1, -1, -1, -1, -1,  0,  1,  2,  3,  4,  //  60 -  69  65-90 == 'A'-'Z'     ('A' - 'E')
         5,  6,  7,  8,  9, 10, 11, 12, 13, 14,  //  70 -  79                       ('F' - 'O')
        15, 16, 17, 18, 19, 20, 21, 22, 23, 24,  //  80 -  89                       ('P' - 'Y')
        25, -1, -1, -1, -1, -1, -1, 26, 27, 28,  //  90 -  99  97-122 == 'a'-'z'    ('Z' and 'a' - 'c')
        29, 30, 31, 32, 33, 34, 35, 36, 37, 38,  // 100 - 109                       ('d' - 'm')
        39, 40, 41, 42, 43, 44, 45, 46, 47, 48,  // 110 - 119                       ('n' - 'w')
        49, 50, 51, -1, -1, -1, -1, -1, -1, -1,  // 120 - 129                       ('x' - 'z')
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

#define IS_WHITESPACE_CHAR(x) (_IS_IN_BYTE_RANGE(x) && (_Ion_is_whitespace_char[(x)] != 0))
GLOBAL char *_Ion_whitespace_chars INITTO("\n\r\t ");
GLOBAL BOOL  _Ion_is_whitespace_char[256]
#ifdef INIT_STATICS
= { //   0   1   2   3   4   5   6   7   8   9
         1,  0,  0,  0,  0,  0,  0,  0,  0,  1,  //   0 -   9   0 == '\0', 9 = '\t'
         1,  0,  0,  1,  0,  0,  0,  0,  0,  0,  //  10 -  19  10 == '\n', 13 = '\r'
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  20 -  29
         0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  //  30 -  39  32 == ' '
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  40 -  49  
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  50 -  59  
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  60 -  69  
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  70 -  79
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  80 -  89
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  //  90 -  99  
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 100 - 109
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 110 - 119
         0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  // 120 - 129
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



GLOBAL int TID_NIBBLE_TO_ION_TYPE[]
#ifdef INIT_STATICS
= {
    TID_NULL,        //  tid_NULL      =  0,
    TID_BOOL,        //  tid_BOOL      =  1,
    TID_POS_INT,     //  tid_INT       =  2,
    TID_FLOAT,       //  tid_FLOAT     =  3,
    TID_DECIMAL,     //  tid_DECIMAL   =  4,
    TID_TIMESTAMP,   //  tid_TIMESTAMP =  5,
    TID_STRING,      //  tid_STRING    =  6,
    TID_SYMBOL,      //  tid_SYMBOL    =  7,
    TID_CLOB,        //  tid_CLOB      =  8,
    TID_BLOB,        //  tid_BLOB      =  9,
    TID_STRUCT,      //  tid_STRUCT    = 10,
    TID_LIST,        //  tid_LIST      = 11,
    TID_SEXP         //  tid_SEXP      = 12
}
#endif
;


#ifdef __cplusplus
}
#endif

#endif

