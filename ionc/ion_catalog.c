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
    ION_SYMBOL_TABLE **ppsymtab, *psystem, *pclone, *ptest = NULL;
    ION_STRING         name;
    int32_t            version;
    hOWNER             owner;

    ASSERT(pcatalog != NULL);
    ASSERT(psymtab != NULL);

    IONCHECK(ion_symbol_table_get_name(psymtab, &name));
    IONCHECK(ion_symbol_table_get_version(psymtab, &version));

    // see if we already have it
    IONCHECK(_ion_catalog_find_symbol_table_helper(pcatalog, &name, version, &ptest));
    if (ptest != NULL) {
        SUCCEED();
    }

    // otherwise ...

    IONCHECK(_ion_symbol_table_get_owner(psymtab, &owner));

    // if this catalog doesn't own it - we have to clone it
    if (pcatalog->owner != owner) {
        IONCHECK(_ion_symbol_table_get_system_symbol_table(psymtab, &psystem));
        IONCHECK(_ion_symbol_table_clone_with_owner_and_system_table(psymtab, &pclone, pcatalog->owner, psystem));
        psymtab = pclone;
    }

    // now we attach it
    ppsymtab = _ion_collection_append(&pcatalog->table_list);
    if (!ppsymtab) FAILWITH(IERR_NO_MEMORY);
    *ppsymtab = psymtab;

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

iERR _ion_catalog_find_symbol_table_helper(ION_CATALOG *pcatalog, ION_STRING *name, int32_t version, hSYMTAB *p_symtab)
{
    iENTER;
    ION_SYMBOL_TABLE        **ppsymtab, *psymtab, *found = NULL;
    ION_COLLECTION_CURSOR    symtab_cursor;
    ION_STRING               symtab_name, system_symtab_name;
    int32_t                  symtab_version, system_symtab_version;

    ASSERT(pcatalog != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(p_symtab != NULL);

    IONCHECK(_ion_symbol_table_get_name_helper(pcatalog->system_symbol_table, &system_symtab_name));
    IONCHECK(_ion_symbol_table_get_version_helper(pcatalog->system_symbol_table, &system_symtab_version));

    if (version == system_symtab_version
    && ION_STRING_EQUALS(name, &system_symtab_name)
    ) {
        found = pcatalog->system_symbol_table;
    }
    else {
        ION_COLLECTION_OPEN(&pcatalog->table_list, symtab_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symtab_cursor, ppsymtab);
            if (!ppsymtab) break;
            psymtab = *ppsymtab;

            IONCHECK(_ion_symbol_table_get_name_helper(psymtab, &symtab_name));
            IONCHECK(_ion_symbol_table_get_version_helper(psymtab, &symtab_version));

            if (symtab_version != version) continue;
            if (ION_STRING_EQUALS(name, &symtab_name)) {
                found = psymtab;
                break;
            }
        }
        ION_COLLECTION_CLOSE(symtab_cursor);
    }

    *p_symtab = PTR_TO_HANDLE(found);
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
    ION_SYMBOL_TABLE       **ppsymtab, *psymtab, *best = NULL;
    ION_STRING               symtab_name, system_name;
    int32_t                  symtab_version, best_version, system_version;
    ION_COLLECTION_CURSOR    symtab_cursor;

    ASSERT(pcatalog != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));

    // check for the system table first (mostly because it's not
    // really in the list of symbol tables in the catalog)
    IONCHECK(_ion_symbol_table_get_name_helper(pcatalog->system_symbol_table, &system_name));
    IONCHECK(_ion_symbol_table_get_version_helper(pcatalog->system_symbol_table, &system_version));
    if (version == system_version && ION_STRING_EQUALS(name, &system_name)) {
        best = pcatalog->system_symbol_table;
    }
    else {
        ION_COLLECTION_OPEN(&pcatalog->table_list, symtab_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symtab_cursor, ppsymtab);
            if (!ppsymtab) break;
            psymtab = *ppsymtab;
            IONCHECK(_ion_symbol_table_get_name_helper(psymtab, &symtab_name));
            if (!ION_STRING_EQUALS(name, &symtab_name)) continue;
            if (!best) {
                best = psymtab;
            }
            else {
                IONCHECK(_ion_symbol_table_get_version_helper(psymtab, &symtab_version));
                IONCHECK(_ion_symbol_table_get_version_helper(best, &best_version));
                if (version > 0 && symtab_version >= version) {
                    if (symtab_version <= best_version) {
                        best = psymtab;
                    }
                }
                else if (symtab_version > best_version) {
                    best = psymtab;
                }
            }
            IONCHECK(_ion_symbol_table_get_version_helper(best, &best_version));
            if (best_version == version) break;
        }
        ION_COLLECTION_CLOSE(symtab_cursor);
    }

    if (version > 0 && max_id <= ION_SYS_SYMBOL_MAX_ID_UNDEFINED) {
        // This isn't an exact match, and the max_id of the import is undefined.
        // NOTE: the ionizer APIs treat version == 0 as a special case where the user is requesting the latest
        // version. When called from other locations where version <= 1 means the version is undefined, the caller
        // should manually validate that the max_id isn't also undefined.
        if (!best) FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "Invalid symbol table import: found undefined max_id without exact match.");
        IONCHECK(_ion_symbol_table_get_version_helper(best, &best_version));
        if (best_version != version) FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "Invalid symbol table import: found undefined max_id without exact match.");
    }
    if (p_psymtab) *p_psymtab = best;
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
    ION_SYMBOL_TABLE        **ppsymtab, **found = NULL;
    ION_COLLECTION_CURSOR   symtab_cursor;
    ION_STRING              name, our_name;
    int32_t                 version, our_version;

    ASSERT(pcatalog != NULL);
    ASSERT(psymtab != NULL);

    IONCHECK(ion_symbol_table_get_name(psymtab, &name));
    IONCHECK(ion_symbol_table_get_version(psymtab, &version));

    ASSERT(pcatalog != NULL);

    // the argument to _ion_collection_remove must be a pointer to
    // ION_COLLECTION_NODE._data; i.e. the second parameter of
    // ION_COLLECTION_NEXT(cursor, p_data)
    ION_COLLECTION_OPEN(&pcatalog->table_list, symtab_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(symtab_cursor, ppsymtab);
        if (!ppsymtab) break;
        if (*ppsymtab != psymtab) {
            IONCHECK(ion_symbol_table_get_version(*ppsymtab, &our_version));
            if (our_version != version) continue;

            IONCHECK(ion_symbol_table_get_name(*ppsymtab, &our_name));
            if (!ION_STRING_EQUALS(&our_name, &name)) continue;
        }
        found = ppsymtab;
        break;
    }
    ION_COLLECTION_CLOSE(symtab_cursor);

    if (!found) {
		// TODO: again - is this just fine (the table's already released)
		//       or is this a problem to report
		// FAILWITH(IERR_SYMBOL_TABLE_NOT_FOUND);
		SUCCEED();
    }

    _ion_collection_remove(&pcatalog->table_list, found);

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

