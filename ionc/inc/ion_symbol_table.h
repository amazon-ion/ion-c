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

/**@file */

#ifndef ION_SYMBOL_TABLE_H_
#define ION_SYMBOL_TABLE_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _ion_symbol_import_location
{
    ION_STRING name;
    SID        location;
} ION_SYMBOL_IMPORT_LOCATION;

struct _ion_symbol
{
    SID                         sid;
    ION_STRING                  value;
    ION_SYMBOL_IMPORT_LOCATION  import_location;
    // TODO this is only needed for symbol usage metrics. Consider removal.
    int32_t           add_count;
};

typedef enum _ION_SYMBOL_TABLE_TYPE {
    ist_EMPTY  = 0,
    ist_LOCAL  = 1,
    ist_SHARED = 2,
    ist_SYSTEM = 3
} ION_SYMBOL_TABLE_TYPE;

#define ION_SYMBOL_IMPORT_LOCATION_IS_NULL(symbol) ION_STRING_IS_NULL(&(symbol)->import_location.name)
#define ION_SYMBOL_IS_NULL(symbol) (symbol == NULL || (ION_STRING_IS_NULL(&(symbol)->value) && ION_SYMBOL_IMPORT_LOCATION_IS_NULL(symbol) && (symbol)->sid == UNKNOWN_SID))
#define ION_SYMBOL_INIT(symbol) ION_STRING_INIT(&(symbol)->value); ION_STRING_INIT(&(symbol)->import_location.name); (symbol)->sid = UNKNOWN_SID

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

/**
 * Allocates a new local symbol table.
 * @param p_hsymtab - Pointer to a handle to the newly-allocated symbol table.
 * @param owner - Handle to the new symbol table's memory owner. If NULL, the resulting symbol table is its own memory
 *  owner and must be freed using `ion_symbol_table_close`.
 */
ION_API_EXPORT iERR ion_symbol_table_open               (hSYMTAB *p_hsymtab, hOWNER owner);

/**
 * Allocates a new local symbol table of the given type (i.e. shared or local).
 * @param p_hsymtab - Pointer to a handle to the newly-allocated symbol table.
 * @param owner - Handle to the new symbol table's memory owner. If NULL, the resulting symbol table is its own memory
 *  owner and must be freed using `ion_symbol_table_close`.
 */
ION_API_EXPORT iERR ion_symbol_table_open_with_type     (hSYMTAB *p_hsymtab, hOWNER owner, ION_SYMBOL_TABLE_TYPE type);

/**
 * Clones the given symbol table, using that symbol table's memory owner as the owner of the newly allocated symbol
 * table.
 * @param hsymtab - They symbol table to clone.
 * @param p_hclone - Pointer to a handle to the newly-allocated symbol table clone.
 */
ION_API_EXPORT iERR ion_symbol_table_clone              (hSYMTAB hsymtab, hSYMTAB *p_hclone);

/**
 * Clones the given symbol table, using the given owner as the newly-allocated symbol table's memory owner.
 * @param hsymtab - They symbol table to clone.
 * @param p_hclone - Pointer to a handle to the newly-allocated symbol table clone.
 * @param owner - Handle to the new symbol table's memory owner. If NULL, the resulting symbol table is its own memory
 *  owner and must be freed using `ion_symbol_table_close`.
 */
ION_API_EXPORT iERR ion_symbol_table_clone_with_owner   (hSYMTAB hsymtab, hSYMTAB *p_hclone, hOWNER owner);

/**
 * Gets the global system symbol table for the given Ion version. This global system symbol table must never be closed.
 * @param p_hsystem_table - Pointer to a handle to the global system symbol table.
 * @param version - The Ion version. Currently, must be 1.
 */
ION_API_EXPORT iERR ion_symbol_table_get_system_table   (hSYMTAB *p_hsystem_table, int32_t version);

/**
 * Deserializes a symbol table (shared or local) from the given reader.
 * @param hreader - The reader, positioned at the start of the symbol table struct.
 * @param owner - Handle to the new symbol table's memory owner. If NULL, the resulting symbol table is its own memory
 *  owner and must be freed using `ion_symbol_table_close`.
 * @param p_hsymtab - Pointer to a handle to the newly-allocated symbol table.
 */
ION_API_EXPORT iERR ion_symbol_table_load               (hREADER hreader, hOWNER owner, hSYMTAB *p_hsymtab);

/**
 * Serializes a symbol table (shared or local) using the given writer.
 * @param hsymtab - The symbol table to serialize.
 * @param hwriter - The writer (text or binary).
 */
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

/**
 * Imports a shared symbol table into a local symbol table, given a description of the import and the catalog in which
 * it can be found.
 * NOTE: the best match for the described shared symbol table import that is available in the catalog will be used. If
 * no match is found, all of the import's symbols will be considered to have unknown text.
 * @param hsymtab - The local symbol table into which the imported symbol table will be incorporated.
 * @param pimport - The description of the shared symbol table to be imported.
 * @param catalog - The catalog to query for the matching shared symbol table.
 */
ION_API_EXPORT iERR ion_symbol_table_add_import         (hSYMTAB hsymtab, ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR *pimport, hCATALOG catalog);

/**
 * Imports a shared symbol table into a local symbol table.
 * @param hsymtab - The local symbol table into which the imported symbol table will be incorporated.
 * @param hsymtab_import - The shared symbol table to import.
 */
ION_API_EXPORT iERR ion_symbol_table_import_symbol_table(hSYMTAB hsymtab, hSYMTAB hsymtab_import);

ION_API_EXPORT iERR ion_symbol_table_find_by_name       (hSYMTAB hsymtab, iSTRING name, SID *p_sid);
ION_API_EXPORT iERR ion_symbol_table_find_by_sid        (hSYMTAB hsymtab, SID sid, iSTRING *p_name);
ION_API_EXPORT iERR ion_symbol_table_is_symbol_known    (hSYMTAB hsymtab, SID sid, BOOL *p_is_known);

ION_API_EXPORT iERR ion_symbol_table_get_symbol         (hSYMTAB hsymtab, SID sid, ION_SYMBOL **p_sym); // get symbols by sid, iterate from 1 to max_sid - returns all symbols
ION_API_EXPORT iERR ion_symbol_table_get_local_symbol   (hSYMTAB hsymtab, SID sid, ION_SYMBOL **p_sym); // get symbols by sid, iterate from 1 to max_sid - returns only locally defined symbols

ION_API_EXPORT iERR ion_symbol_table_add_symbol         (hSYMTAB hsymtab, iSTRING name, SID *p_sid);

/**
 * If the given symbol table is its own memory owner, its memory and everything it owns is freed. If the given symbol
 * table has an external owner and that owner has not been freed, this does nothing; this symbol table will be freed
 * when its memory owner is freed. If the given symbol table has an external owner which has been freed, the behavior of
 * this function is undefined.
 * NOTE: Symbol tables constructed and returned by readers and writers are owned by those readers and writers.
 */
ION_API_EXPORT iERR ion_symbol_table_close              (hSYMTAB hsymtab);

/**
 * Copies an ION_SYMBOL to a new memory owner.
 */
ION_API_EXPORT iERR ion_symbol_copy_to_owner(hOWNER owner, ION_SYMBOL *dst, ION_SYMBOL *src);

/**
 * Compares the two given ION_SYMBOLs for equality under the Ion data model.
 */
ION_API_EXPORT iERR ion_symbol_is_equal(ION_SYMBOL *lhs, ION_SYMBOL *rhs, BOOL *is_equal);

ION_API_EXPORT const char *ion_symbol_table_type_to_str (ION_SYMBOL_TABLE_TYPE t);

#ifdef __cplusplus
}
#endif

#endif /* ION_SYMBOL_TABLE_H_ */
