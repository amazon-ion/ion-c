/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
// a catalog holds one or more symbol tables and manages the shared
// symbol tables that might be needed for reading or writing
//

#include "ion_internal.h"

iERR ion_catalog_open(hCATALOG *p_hcatalog)
{
    iENTER;
    ION_CATALOG *catalog;

    if (p_hcatalog == NULL) {
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_catalog_open_with_owner_helper(&catalog, NULL));

    *p_hcatalog = PTR_TO_HANDLE(catalog);

    iRETURN;
}

iERR ion_catalog_open_with_owner(hCATALOG *p_hcatalog, hOWNER owner)
{
    iENTER;
    ION_CATALOG *catalog;

    if (p_hcatalog == NULL) {
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_catalog_open_with_owner_helper(&catalog, owner));

    *p_hcatalog = PTR_TO_HANDLE(catalog);

    iRETURN;
}

iERR _ion_catalog_open_with_owner_helper(ION_CATALOG **p_pcatalog, hOWNER owner)
{
    iENTER;
    ION_CATALOG *catalog;
    ION_SYMBOL_TABLE *system;

    ASSERT(p_pcatalog);

    if (owner == NULL) {
        catalog = (ION_CATALOG *)ion_alloc_owner(sizeof(*catalog));    
        owner = catalog;
    }
    else {
        catalog = (ION_CATALOG *)ion_alloc_with_owner(owner, sizeof(*catalog));
    }
    if (catalog == NULL) FAILWITH(IERR_NO_MEMORY);

    memset(catalog, 0, sizeof(ION_CATALOG));

    catalog->owner = owner;

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    catalog->system_symbol_table = system;

    _ion_collection_initialize(owner, &catalog->table_list, sizeof(ION_SYMBOL_TABLE *)); // collection of ION_SYMBOL_TABLE *

    *p_pcatalog = catalog;

    iRETURN;
}

iERR ion_catalog_get_symbol_table_count(hCATALOG hcatalog, int32_t *p_count)
{
    iENTER;
    ION_CATALOG *catalog;

    if (hcatalog == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_count == NULL) FAILWITH(IERR_INVALID_ARG);

    catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);
    IONCHECK(_ion_catalog_get_symbol_table_count_helper(catalog, p_count));

    iRETURN;
}

iERR _ion_catalog_get_symbol_table_count_helper(ION_CATALOG *pcatalog, int32_t *p_count)
{
    iENTER;
    int32_t count;

    ASSERT(pcatalog);
    ASSERT(p_count);

    count = ION_COLLECTION_SIZE(&pcatalog->table_list);
    *p_count = count;
    SUCCEED();

    iRETURN;
}

iERR ion_catalog_add_symbol_table(hCATALOG hcatalog, hSYMTAB hsymtab)
{
    iENTER;
    ION_CATALOG *catalog;
    ION_SYMBOL_TABLE *symtab;

    if (hcatalog == NULL) FAILWITH(IERR_INVALID_ARG);
    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);

    catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_catalog_add_symbol_table_helper(catalog, symtab));

    iRETURN;
}

iERR _ion_catalog_add_symbol_table_helper(ION_CATALOG *pcatalog, ION_SYMBOL_TABLE *psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE **ppsymtab, *pclone, *ptest = NULL;

    ASSERT(pcatalog != NULL);
    ASSERT(psymtab != NULL);

    // see if we already have it
    IONCHECK(_ion_catalog_find_symbol_table_helper(pcatalog, &psymtab->name, psymtab->version, &ptest));
    if (ptest != NULL) {
        SUCCEED();
    }

    // otherwise ...

    // if this catalog doesn't own it - we have to clone it
    if (pcatalog->owner != psymtab->owner) {
        IONCHECK(_ion_symbol_table_clone_with_owner_helper(&pclone, psymtab, pcatalog->owner, psymtab->system_symbol_table));
        psymtab = pclone;
    }

    // now we attach it
    ppsymtab = _ion_collection_append(&pcatalog->table_list);
    if (!ppsymtab) FAILWITH(IERR_NO_MEMORY);
    *ppsymtab = psymtab;

    psymtab->catalog = pcatalog;

    iRETURN;
}

iERR ion_catalog_find_symbol_table(hCATALOG hcatalog, iSTRING name, long version, hSYMTAB *p_symtab)
{
    iENTER;
    ION_CATALOG *catalog;
    ION_SYMBOL_TABLE *symtab = NULL;

    if (hcatalog == NULL) FAILWITH(IERR_INVALID_ARG);
    if (ION_STRING_IS_NULL(name)) FAILWITH(IERR_INVALID_ARG);
    if (p_symtab == NULL) FAILWITH(IERR_INVALID_ARG);

    catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);

    IONCHECK(_ion_catalog_find_symbol_table_helper(catalog, name, version, &symtab));

    if (symtab) {
        *p_symtab = PTR_TO_HANDLE(symtab);
    }
    else {
        *p_symtab = NULL;
    }

    iRETURN;
}

iERR _ion_catalog_find_symbol_table_helper(ION_CATALOG *pcatalog, ION_STRING *name, int32_t version, ION_SYMBOL_TABLE **p_psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE        *symtab;
    ION_COLLECTION_CURSOR    symtab_cursor;

    ASSERT(pcatalog != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(p_psymtab != NULL);

    if (version == pcatalog->system_symbol_table->version
    && ION_STRING_EQUALS(name, &pcatalog->system_symbol_table->name)
    ) {
        symtab = pcatalog->system_symbol_table;
    }
    else {
        ION_COLLECTION_OPEN(&pcatalog->table_list, symtab_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symtab_cursor, symtab);
            if (!symtab) break;
            if (symtab->version != version) continue;
            if (ION_STRING_EQUALS(name, &symtab->name)) break;
        }
        ION_COLLECTION_CLOSE(symtab_cursor);
    }

    *p_psymtab = symtab;
    SUCCEED();

    iRETURN;
}

iERR ion_catalog_find_best_match(hCATALOG hcatalog, iSTRING name, long version, hSYMTAB *p_symtab)
{
    iENTER;
    ION_CATALOG      *catalog;
    ION_SYMBOL_TABLE *symtab;

    if (hcatalog == NULL) FAILWITH(IERR_INVALID_ARG);
    if (ION_STRING_IS_NULL(name)) FAILWITH(IERR_INVALID_ARG);
    if (p_symtab == NULL) FAILWITH(IERR_INVALID_ARG);

    catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);

    IONCHECK(_ion_catalog_find_best_match_helper(catalog, name, version, -1, &symtab));
    if (symtab != NULL) {
        *p_symtab = PTR_TO_HANDLE(symtab);
    }
    else {
        *p_symtab = NULL;
    }
    iRETURN;
}

iERR _ion_catalog_find_best_match_helper(ION_CATALOG *pcatalog, ION_STRING *name, int32_t version, int32_t max_id, ION_SYMBOL_TABLE **p_psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE      **ppsymtab, *psymtab, *best = NULL;
    ION_COLLECTION_CURSOR    symtab_cursor;

    ASSERT(pcatalog != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(p_psymtab != NULL);

    // check for the system table first (mostly because it's not
    // really in the list of symbol tables in the catalog)
    if (version == pcatalog->system_symbol_table->version
    && ION_STRING_EQUALS(name, &pcatalog->system_symbol_table->name)
    ) {
        best = pcatalog->system_symbol_table;
    }
    else {
        ION_COLLECTION_OPEN(&pcatalog->table_list, symtab_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symtab_cursor, ppsymtab);
            if (!ppsymtab) break;
            psymtab = *ppsymtab;
            if (!ION_STRING_EQUALS(name, &psymtab->name)) continue;
            if (!best) {
                best = psymtab;
            }
            else {
                if (version > 0 && psymtab->version >= version) {
                    if (psymtab->version <= best->version) {
                        best = psymtab;
                    }
                }
                else if (psymtab->version > best->version) {
                    best = psymtab;
                }
            }
            if (best->version == version) break;
        }
        ION_COLLECTION_CLOSE(symtab_cursor);
    }
    if ((version > 0 && max_id <= ION_SYS_SYMBOL_MAX_ID_UNDEFINED) && (!best || best->version != version)) {
        // This isn't an exact match, and the max_id of the import is undefined.
        // NOTE: the ionizer APIs treat version == 0 as a special case where the user is requesting the latest
        // version. When called from other locations where version <= 1 means the version is undefined, the caller
        // should manually validate that the max_id isn't also undefined.
        FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "Invalid symbol table import: found undefined max_id without exact match.")
    }
    *p_psymtab = best;
    SUCCEED();

    iRETURN;
}

iERR ion_catalog_release_symbol_table(hCATALOG hcatalog, hSYMTAB hsymtab)
{
    iENTER;
    ION_CATALOG *catalog;
    ION_SYMBOL_TABLE *symtab;

    if (hcatalog == NULL) FAILWITH(IERR_INVALID_ARG);
    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);

    catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_catalog_release_symbol_table_helper(catalog, symtab));

    iRETURN;
}

iERR _ion_catalog_release_symbol_table_helper(ION_CATALOG *pcatalog, ION_SYMBOL_TABLE *psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE *test;

    ASSERT(pcatalog != NULL);
    ASSERT(psymtab != NULL);

    // if this symbol table is "foriegn" get "our copy" of the table
    if (psymtab->owner != pcatalog->owner) {
        IONCHECK(_ion_catalog_find_symbol_table_helper(pcatalog, &psymtab->name, psymtab->version, &test));
        if (!test) {
            // TODO: again - is this just fine (the table's already released)
            //       or is this a problem to report
            // FAILWITH(IERR_SYMBOL_TABLE_NOT_FOUND);
            SUCCEED();
        }
    }

    _ion_collection_remove(&pcatalog->table_list, psymtab);

    iRETURN;
}


iERR ion_catalog_close(hCATALOG hcatalog)
{
    iENTER;
    ION_CATALOG *catalog;

    if (hcatalog == NULL) FAILWITH(IERR_INVALID_ARG);

    catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);

    IONCHECK(_ion_catalog_close_helper(catalog));

    iRETURN;
}

iERR _ion_catalog_close_helper(ION_CATALOG *pcatalog)
{
    ASSERT(pcatalog != NULL);

    if (pcatalog->owner == pcatalog) {
        ion_free_owner(pcatalog);
    }
    return IERR_OK;
}

