/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#ifndef ION_SYMBOL_TABLE_H_
#define ION_SYMBOL_TABLE_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

struct _ion_symbol
{
    SID               sid;
    ION_STRING        value;
    ION_SYMBOL_TABLE *psymtab;
    int32_t           add_count;
};

typedef enum _ION_SYMBOL_TABLE_TYPE {
    ist_EMPTY  = 0,
    ist_LOCAL  = 1,
    ist_SHARED = 2,
    ist_SYSTEM = 3
} ION_SYMBOL_TABLE_TYPE;

#define UNKNOWN_SID -1 /* symbol id's presume not only is this unknown, but sid's must be positive */

//
// Ion Symbol table implementation
//
#define ION_SYS_SYMBOL_SHARED_SYMBOL_TABLE "$ion_shared_symbol_table"
#define ION_SYS_STRLEN_SHARED_SYMBOL_TABLE 24 /* "$ion_shared_symbol_table" */

#define ION_SYS_SYMBOL_ION                 "$ion"
#define ION_SYS_SYMBOL_IVM                 "$ion_1_0"  /* ion type marker */
#define ION_SYS_SYMBOL_ION_SYMBOL_TABLE    "$ion_symbol_table"
#define ION_SYS_SYMBOL_NAME                "name"
#define ION_SYS_SYMBOL_VERSION             "version"
#define ION_SYS_SYMBOL_IMPORTS             "imports"
#define ION_SYS_SYMBOL_SYMBOLS             "symbols"
#define ION_SYS_SYMBOL_MAX_ID              "max_id"

#define ION_SYS_SYMBOL_MAX_ID_UNDEFINED    -1

#define ION_SYS_SID_UNKNOWN                0 /* not necessarily NULL */
#define ION_SYS_SID_ION                    1 /* "$ion" */
#define ION_SYS_SID_IVM                    2 /* "$ion_1_0"  aka ion type marker */
#define ION_SYS_SID_SYMBOL_TABLE           3 /* "$ion_symbol_table" */
#define ION_SYS_SID_NAME                   4 /* "name" */
#define ION_SYS_SID_VERSION                5 /* "version" */
#define ION_SYS_SID_IMPORTS                6 /* "imports" */
#define ION_SYS_SID_SYMBOLS                7 /* "symbols" */
#define ION_SYS_SID_MAX_ID                 8 /* "max_id" */
#define ION_SYS_SID_SHARED_SYMBOL_TABLE    9 /* "$ion_shared_symbol_table" */

#define ION_SYS_STRLEN_ION                  4 /* "$ion" */
#define ION_SYS_STRLEN_IVM                  8 /* "$ion_1_0" */
#define ION_SYS_STRLEN_SYMBOL_TABLE        17 /* "$ion_symbol_table" */
#define ION_SYS_STRLEN_NAME                 4 /* "name" */
#define ION_SYS_STRLEN_VERSION              7 /* "version" */
#define ION_SYS_STRLEN_IMPORTS              7 /* "imports" */
#define ION_SYS_STRLEN_SYMBOLS              7 /* "symbols" */
#define ION_SYS_STRLEN_MAX_ID               6 /* "max_id" */

GLOBAL BYTE ION_SYMBOL_ION_BYTES[]
#ifdef INIT_STATICS
= { '$', 'i', 'o', 'n', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_VTM_BYTES[]
#ifdef INIT_STATICS
= { '$', 'i', 'o', 'n', '_', '1', '_', '0', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_SYMBOL_TABLE_BYTES[]
#ifdef INIT_STATICS
= { '$', 'i', 'o', 'n', '_', 's', 'y', 'm', 'b', 'o', 'l', '_', 't', 'a', 'b', 'l', 'e', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_NAME_BYTES[]
#ifdef INIT_STATICS
= { 'n', 'a', 'm', 'e', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_VERSION_BYTES[]
#ifdef INIT_STATICS
= { 'v', 'e', 'r', 's', 'i', 'o', 'n', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_IMPORTS_BYTES[]
#ifdef INIT_STATICS
= { 'i', 'm', 'p', 'o', 'r', 't', 's', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_SYMBOLS_BYTES[]
#ifdef INIT_STATICS
= { 's', 'y', 'm', 'b', 'o', 'l', 's', 0 }
#endif
;
GLOBAL BYTE ION_SYMBOL_MAX_ID_BYTES[]
#ifdef INIT_STATICS
= { 'm', 'a', 'x', '_', 'i', 'd', 0 }
#endif
;

GLOBAL BYTE ION_SYMBOL_SHARED_SYMBOL_TABLE_BYTES[]
#ifdef INIT_STATICS
= { '$', 'i', 'o', 'n', '_', 's', 'h', 'a', 'r', 'e', 'd', '_', 's', 'y', 'm', 'b', 'o', 'l', '_', 't', 'a', 'b', 'l', 'e', 0 }
#endif
;


GLOBAL ION_STRING ION_SYMBOL_ION_STRING
#ifdef INIT_STATICS
= {
    ION_SYS_STRLEN_ION,
    ION_SYMBOL_ION_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_VTM_STRING
#ifdef INIT_STATICS
= {
    ION_SYS_STRLEN_IVM,
    ION_SYMBOL_VTM_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_SYMBOL_TABLE_STRING
#ifdef INIT_STATICS
= {
    ION_SYS_STRLEN_SYMBOL_TABLE,
    ION_SYMBOL_SYMBOL_TABLE_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_NAME_STRING
#ifdef INIT_STATICS
={
    ION_SYS_STRLEN_NAME,
    ION_SYMBOL_NAME_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_VERSION_STRING
#ifdef INIT_STATICS
={
    ION_SYS_STRLEN_VERSION,
    ION_SYMBOL_VERSION_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_IMPORTS_STRING
#ifdef INIT_STATICS
={
    ION_SYS_STRLEN_IMPORTS,
    ION_SYMBOL_IMPORTS_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_SYMBOLS_STRING
#ifdef INIT_STATICS
={
    ION_SYS_STRLEN_SYMBOLS,
    ION_SYMBOL_SYMBOLS_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_MAX_ID_STRING
#ifdef INIT_STATICS
={
    ION_SYS_STRLEN_MAX_ID,
    ION_SYMBOL_MAX_ID_BYTES
}
#endif
;
GLOBAL ION_STRING ION_SYMBOL_SHARED_SYMBOL_TABLE_STRING
#ifdef INIT_STATICS
={
    ION_SYS_STRLEN_SHARED_SYMBOL_TABLE,
    ION_SYMBOL_SHARED_SYMBOL_TABLE_BYTES
}
#endif
;


/**
 * The set of system symbols as defined by Ion 1.0.
 */
GLOBAL char *SYSTEM_SYMBOLS []
#ifdef INIT_STATICS
= {
    ION_SYS_SYMBOL_ION,
    ION_SYS_SYMBOL_IVM,
    ION_SYS_SYMBOL_ION_SYMBOL_TABLE,
    ION_SYS_SYMBOL_NAME,
    ION_SYS_SYMBOL_VERSION,
    ION_SYS_SYMBOL_IMPORTS,
    ION_SYS_SYMBOL_SYMBOLS,
    ION_SYS_SYMBOL_MAX_ID,
    ION_SYS_SYMBOL_SHARED_SYMBOL_TABLE,
    NULL
}
#endif
;

ION_API_EXPORT iERR ion_symbol_table_open               (hSYMTAB *p_hsymtab, hOWNER owner);
ION_API_EXPORT iERR ion_symbol_table_clone              (hSYMTAB hsymtab, hSYMTAB *p_hclone);
ION_API_EXPORT iERR ion_symbol_table_clone_with_owner   (hSYMTAB hsymtab, hSYMTAB *p_hclone, hOWNER owner);
ION_API_EXPORT iERR ion_symbol_table_get_system_table   (hSYMTAB *p_hsystem_table, int32_t version);
ION_API_EXPORT iERR ion_symbol_table_load               (hREADER hreader, hOWNER owner, hSYMTAB *p_hsymtab);
ION_API_EXPORT iERR ion_symbol_table_unload             (hSYMTAB hsymtab, hWRITER hwriter);

ION_API_EXPORT iERR ion_symbol_table_lock               (hSYMTAB hsymtab);
ION_API_EXPORT iERR ion_symbol_table_is_locked          (hSYMTAB hsymtab, BOOL *p_is_locked);
ION_API_EXPORT iERR ion_symbol_table_get_type           (hSYMTAB hsymtab, ION_SYMBOL_TABLE_TYPE *p_type);

ION_API_EXPORT iERR ion_symbol_table_get_name           (hSYMTAB hsymtab, iSTRING p_name);
ION_API_EXPORT iERR ion_symbol_table_get_version        (hSYMTAB hsymtab, int32_t *p_version);
ION_API_EXPORT iERR ion_symbol_table_get_max_sid        (hSYMTAB hsymtab, SID *p_max_id);

ION_API_EXPORT iERR ion_symbol_table_set_name           (hSYMTAB hsymtab, iSTRING name);
ION_API_EXPORT iERR ion_symbol_table_set_version        (hSYMTAB hsymtab, int32_t version);
ION_API_EXPORT iERR ion_symbol_table_set_max_sid        (hSYMTAB hsymtab, SID max_id);

ION_API_EXPORT iERR ion_symbol_table_get_imports        (hSYMTAB hsymtab, ION_COLLECTION **p_imports);
ION_API_EXPORT iERR ion_symbol_table_add_import         (hSYMTAB hsymtab, ION_SYMBOL_TABLE_IMPORT *pimport);
ION_API_EXPORT iERR ion_symbol_table_import_symbol_table(hSYMTAB hsymtab, hSYMTAB hsymtab_import);

ION_API_EXPORT iERR ion_symbol_table_find_by_name       (hSYMTAB hsymtab, iSTRING name, SID *p_sid);
ION_API_EXPORT iERR ion_symbol_table_find_by_sid        (hSYMTAB hsymtab, SID sid, iSTRING *p_name);
ION_API_EXPORT iERR ion_symbol_table_is_symbol_known    (hSYMTAB hsymtab, SID sid, BOOL *p_is_known);

ION_API_EXPORT iERR ion_symbol_table_get_symbol         (hSYMTAB hsymtab, SID sid, ION_SYMBOL **p_sym); // get symbols by sid, iterate from 1 to max_sid - returns all symbols
ION_API_EXPORT iERR ion_symbol_table_get_local_symbol   (hSYMTAB hsymtab, SID sid, ION_SYMBOL **p_sym); // get symbols by sid, iterate from 1 to max_sid - returns only locally defined symbols

ION_API_EXPORT iERR ion_symbol_table_add_symbol         (hSYMTAB hsymtab, iSTRING name, SID *p_sid);
// TODO: do we want this? iERR ion_symbol_table_add_symbol_and_sid   (hSYMTAB hsymtab, iSTRING name, SID sid);

ION_API_EXPORT iERR ion_symbol_table_close              (hSYMTAB hsymtab);
ION_API_EXPORT iERR ion_symbol_table_free_system_table  ();

ION_API_EXPORT const char *ion_symbol_table_type_to_str (ION_SYMBOL_TABLE_TYPE t);

#ifdef __cplusplus
}
#endif

#endif /* ION_SYMBOL_TABLE_H_ */
