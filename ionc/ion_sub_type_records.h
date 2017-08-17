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
 * These macros are included in ion_reader_text.c to provice declaraions
 * for the ion sub type "enum" structs and the associated constant pointers
 * The are also included in ion_reader_text.h to provide extern definitions
 * for general use.
 */

/*

Follow Container Flags (from ion_reader_text.h):
#define FCF_none         0x0000
#define FCF_DATAGRAM     0x0001
#define FCF_SEXP         0x0002
#define FCF_LIST         0x0004
#define FCF_STRUCT       0x0008

#define FCF_IS_NULL      0x0010
#define FCF_IS_NUMBER    0x0020
#define FCF_IS_CONTAINER 0x0100

* typedef struct _ion_sub_type {
 *     char             *name;
 *     ION_TYPE          base_type;
 *     ION_PARSER_STATE  follow_state;
 *     uint16_t          flags;   // tags for whether this token is valid to follow a value in a particular container
 * } ION_SUB_TYPE_STRUCT, *ION_SUB_TYPE;
 */

#ifdef IST_RECORD
    
    IST_RECORD( IST_ERROR,                      tid_none,       IPS_ERROR,            (FCF_none) )
    IST_RECORD( IST_EOF,                        tid_EOF,        IPS_EOF,              (FCF_DATAGRAM) )
    IST_RECORD( IST_NONE,                       tid_none,       IPS_NONE,             (FCF_none) )

    IST_RECORD( IST_SINGLE_COLON,               tid_none,       IPS_ERROR,            (FCF_none) )
    IST_RECORD( IST_CLOSE_SINGLE_BRACE,         tid_EOF,        IPS_AFTER_VALUE,      (FCF_STRUCT | FCF_CLOSE_PREV) )
    IST_RECORD( IST_DOUBLE_BRACE,               tid_none,       IPS_IN_VALUE,         (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_CLOSE_PAREN,                tid_EOF,        IPS_AFTER_VALUE,      (FCF_SEXP | FCF_CLOSE_PREV) )
    IST_RECORD( IST_CLOSE_BRACKET,              tid_EOF,        IPS_AFTER_VALUE,      (FCF_LIST | FCF_CLOSE_PREV) )

    IST_RECORD( IST_COMMA,                      tid_none,       IPS_ERROR,            (FCF_LIST | FCF_STRUCT) )

    IST_RECORD( IST_NULL_NULL,                  tid_NULL,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_BOOL,                  tid_BOOL,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_INT,                   tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_FLOAT,                 tid_FLOAT,      IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_DECIMAL,               tid_DECIMAL,    IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_TIMESTAMP,             tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_SYMBOL,                tid_SYMBOL,     IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_STRING,                tid_STRING,     IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_CLOB,                  tid_CLOB,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_BLOB,                  tid_BLOB,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_SEXP,                  tid_SEXP,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_LIST,                  tid_LIST,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )
    IST_RECORD( IST_NULL_STRUCT,                tid_STRUCT,     IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NULL) )

    IST_RECORD( IST_BOOL_TRUE,                  tid_BOOL,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_BOOL_FALSE,                 tid_BOOL,       IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )

    IST_RECORD( IST_INT_NEG_DECIMAL,            tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_INT_NEG_HEX,                tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_INT_NEG_BINARY,             tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_INT_POS_DECIMAL,            tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_INT_POS_HEX,                tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_INT_POS_BINARY,             tid_INT,        IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_FLOAT_64,                   tid_FLOAT,      IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_FLOAT_32,                   tid_FLOAT,      IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_DECIMAL,                    tid_DECIMAL,    IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )
    IST_RECORD( IST_DECIMAL_D,                  tid_DECIMAL,    IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP | FCF_IS_NUMBER) )

    IST_RECORD( IST_TIMESTAMP_YEAR,             tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_TIMESTAMP_MONTH,            tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_TIMESTAMP_DAY,              tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_TIMESTAMP_TIME,             tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_TIMESTAMP_WITH_SECS,        tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_TIMESTAMP_WITH_FRAC_SECS,   tid_TIMESTAMP,  IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_SYMBOL_PLAIN,               tid_SYMBOL,     IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_SYMBOL_QUOTED,              tid_SYMBOL,     IPS_AFTER_VALUE,      (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_SYMBOL_EXTENDED,            tid_SYMBOL,     IPS_BEFORE_SCALAR,    (FCF_SEXP) )
    IST_RECORD( IST_PLUS_INF,                   tid_FLOAT,      IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_MINUS_INF,                  tid_FLOAT,      IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_NAN,                        tid_FLOAT,      IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_STRING_PLAIN,               tid_STRING,     IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_STRING_LONG,                tid_STRING,     IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_CLOB_PLAIN,                 tid_CLOB,       IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_CLOB_LONG,                  tid_CLOB,       IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )
    IST_RECORD( IST_BLOB,                       tid_BLOB,       IPS_BEFORE_SCALAR,    (FCF_DATAGRAM | FCF_SEXP) )

    IST_RECORD( IST_SEXP,                       tid_SEXP,       IPS_BEFORE_CONTAINER, (FCF_DATAGRAM | FCF_SEXP | FCF_IS_CONTAINER) )
    IST_RECORD( IST_LIST,                       tid_LIST,       IPS_BEFORE_CONTAINER, (FCF_DATAGRAM | FCF_SEXP | FCF_IS_CONTAINER) )
    IST_RECORD( IST_STRUCT,                     tid_STRUCT,     IPS_BEFORE_CONTAINER, (FCF_DATAGRAM | FCF_SEXP | FCF_IS_CONTAINER) )
    IST_RECORD( IST_DATAGRAM,                   tid_DATAGRAM,   IPS_ERROR,            (FCF_none) )


#undef IST_RECORD

#endif
