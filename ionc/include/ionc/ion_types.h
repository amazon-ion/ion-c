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

/** @file */

/**
 * Public shared types for Ion implementation.
 * This includes typedef's for various platforms and int sizes
 * and other associated configuration
 */

#ifndef ION_TYPES_H_
#define ION_TYPES_H_

#include "ion_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <decNumber/decQuad.h>
#include <decNumber/decNumber.h>

#ifdef ION_INIT
  #define GLOBAL    ION_API_EXPORT
  #define INITTO(x) = x
  #define INIT_STATICS
#else
  #define GLOBAL    extern
  #define INITTO(x) /* nothing */
  #ifdef  INIT_STATICS
  #undef  INIT_STATICS
  #endif
#endif

#ifndef NULL
#define NULL ((void *)0)
#endif

#ifndef TRUE
#define TRUE  1
#define FALSE 0
#endif

#ifndef MAX_INT32
#define MAX_INT32         0x7FFFFFFF    // 2,147,483,647
#define MIN_INT32         -0x7FFFFFFF-1    // -2,147,483,648
#endif

#ifndef MAX_INT64
#define MAX_INT64  0x7FFFFFFFFFFFFFFFLL
#define MIN_INT64  -0x7FFFFFFFFFFFFFFFLL-1
#define HIGH_BIT_INT64 (((uint64_t)(1)) << 63)
#endif

/** Strong typed enum over pointer type. */
typedef struct ion_type    *ION_TYPE;
#define ION_TYPE_INT(x) ((intptr_t) (x))

/**
 * Current value type not known (has not been checked)
 */
#define tid_none      ((ION_TYPE)(tid_none_INT))

/**
 * Stands for both End of File and End of Container, unfortunately.
 */
#define tid_EOF       ((ION_TYPE)(tid_EOF_INT))

/**
 * Ion Value Type NULL.
 */
#define tid_NULL         ((ION_TYPE) tid_NULL_INT)
#define tid_BOOL         ((ION_TYPE) tid_BOOL_INT)
#define tid_INT          ((ION_TYPE) tid_INT_INT)
#define tid_FLOAT        ((ION_TYPE) tid_FLOAT_INT)
#define tid_DECIMAL      ((ION_TYPE) tid_DECIMAL_INT)
#define tid_TIMESTAMP    ((ION_TYPE) tid_TIMESTAMP_INT)
#define tid_SYMBOL       ((ION_TYPE) tid_SYMBOL_INT)
#define tid_STRING       ((ION_TYPE) tid_STRING_INT)
#define tid_CLOB         ((ION_TYPE) tid_CLOB_INT)
#define tid_BLOB         ((ION_TYPE) tid_BLOB_INT)
#define tid_LIST         ((ION_TYPE) tid_LIST_INT)
#define tid_SEXP         ((ION_TYPE) tid_SEXP_INT)
#define tid_STRUCT       ((ION_TYPE) tid_STRUCT_INT)
#define tid_DATAGRAM     ((ION_TYPE) tid_DATAGRAM_INT)

#define tid_none_INT       -0x200
#define tid_EOF_INT        -0x100
#define tid_NULL_INT       0x000
#define tid_BOOL_INT       0x100
#define tid_INT_INT        0x200
#define tid_FLOAT_INT      0x400
#define tid_DECIMAL_INT    0x500
#define tid_TIMESTAMP_INT  0x600
#define tid_SYMBOL_INT     0x700
#define tid_STRING_INT     0x800
#define tid_CLOB_INT       0x900
#define tid_BLOB_INT       0xA00
#define tid_LIST_INT       0xB00
#define tid_SEXP_INT       0xC00
#define tid_STRUCT_INT     0xD00
#define tid_DATAGRAM_INT   0xF00

typedef int32_t             SID;
typedef int32_t             SIZE;
typedef uint8_t             BYTE;
typedef int                 BOOL;
#define MAX_SIZE            INT32_MAX

//
// forward references of pointers for linked lists
//
typedef struct _ion_symbol_table        ION_SYMBOL_TABLE;
typedef struct _ion_catalog             ION_CATALOG;

/**
 * An Ion String.
 *
 * @see _ion_string
 */
typedef struct _ion_string              ION_STRING;
typedef struct _ion_symbol              ION_SYMBOL;

typedef struct _ion_symbol_table_import_descriptor ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR;
typedef struct _ion_symbol_table_import            ION_SYMBOL_TABLE_IMPORT;

typedef struct _ion_reader              ION_READER;
typedef struct _ion_writer              ION_WRITER;
typedef struct _ion_int                 ION_INT;
typedef struct _ion_decimal             ION_DECIMAL;
typedef struct _ion_timestamp           ION_TIMESTAMP;
typedef struct _ion_collection          ION_COLLECTION;

#ifndef ION_STREAM_DECL
#define ION_STREAM_DECL

// needed when the stream is used outside the context of the
// general Ion library. Otherwise this must be defined in the
// Ion type header (ion_types.h).
typedef struct _ion_stream        ION_STREAM;

// decl's for user managed stream
struct _ion_user_stream;
typedef iERR (*ION_STREAM_HANDLER)(struct _ion_user_stream *pstream);
struct _ion_user_stream
{
    BYTE *curr;
    BYTE *limit;
    void *handler_state;
    ION_STREAM_HANDLER handler;
};

#endif

// some public pointers to these, which we don't really need
// and should be changed TODO
typedef ION_STRING              *iSTRING;
typedef ION_SYMBOL              *iSYMBOL;
typedef ION_SYMBOL_TABLE_IMPORT *iIMPORT;
typedef ION_SYMBOL_TABLE        *iSYMTAB;
typedef ION_CATALOG             *iCATALOG;
typedef ION_STREAM              *iSTREAM;
typedef ION_TIMESTAMP           *iTIMESTAMP;

// TODO: switch to: typedef struct _ion_catalog_proxy *hCATALOG;
typedef void                    *hOWNER;
typedef ION_READER              *hREADER;
typedef ION_WRITER              *hWRITER;
typedef ION_SYMBOL_TABLE        *hSYMTAB;
typedef ION_CATALOG             *hCATALOG;

#ifdef __cplusplus
}
#endif

#endif

