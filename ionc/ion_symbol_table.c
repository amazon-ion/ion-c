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
// symbol tables hold the int to string mapping for ion symbols
// they come in 3 flavors - local, shared, and the system symbol table
//

#include "ion_internal.h"
#include <ctype.h>
#include <string.h>
//#include "hashfn.h"

iERR _ion_symbol_table_local_find_by_sid(ION_SYMBOL_TABLE *symtab, SID sid, ION_SYMBOL **p_sym);

iERR ion_symbol_table_open(hSYMTAB *p_hsymtab, hOWNER owner)
{
    iENTER;
    ION_SYMBOL_TABLE *table, *system;

    if (p_hsymtab == NULL) {
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    IONCHECK(_ion_symbol_table_open_helper(&table, owner, system));

    *p_hsymtab = PTR_TO_HANDLE(table);

    iRETURN;
}

iERR _ion_symbol_table_open_helper(ION_SYMBOL_TABLE **p_psymtab, hOWNER owner, ION_SYMBOL_TABLE *system)
{
    iENTER;
    ION_SYMBOL_TABLE      *symtab;

    if (owner == NULL) {
        symtab = (ION_SYMBOL_TABLE *)ion_alloc_owner(sizeof(*symtab));
        owner = symtab;
    }
    else {
        symtab = (ION_SYMBOL_TABLE *)ion_alloc_with_owner(owner, sizeof(*symtab));
    }
    if (symtab == NULL) FAILWITH(IERR_NO_MEMORY);

    memset(symtab, 0, sizeof(*symtab));

    symtab->system_symbol_table = system;
    symtab->owner = owner;

    _ion_collection_initialize(owner, &symtab->import_list, sizeof(ION_SYMBOL_TABLE_IMPORT)); // collection of ION_SYMBOL_TABLE_IMPORT
    _ion_collection_initialize(owner, &symtab->symbols, sizeof(ION_SYMBOL)); // collection of ION_SYMBOL

    // if there is a system table to work from (there isn't when we
    // create the system symbol table) we need to copy the system
    // symbols to seed our symbol list
    if (system) {
        IONCHECK(_ion_symbol_table_local_incorporate_symbols(symtab, system, system->max_id));
    }
    *p_psymtab = symtab;

    iRETURN;
}

iERR ion_symbol_table_clone(hSYMTAB hsymtab, hSYMTAB *p_hclone)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab, *clone,*system;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_hclone == NULL) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    IONCHECK(_ion_symbol_table_clone_with_owner_helper(&clone, symtab, symtab->owner, system));

    *p_hclone = PTR_TO_HANDLE(clone);

    iRETURN;
}

iERR ion_symbol_table_clone_with_owner(hSYMTAB hsymtab, hSYMTAB *p_hclone, hOWNER owner)
{
    iENTER;
    ION_SYMBOL_TABLE *orig, *clone, *system;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_hclone == NULL) FAILWITH(IERR_INVALID_ARG);

    orig = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    IONCHECK(_ion_symbol_table_clone_with_owner_helper(&clone, orig, owner, system));

    *p_hclone = PTR_TO_HANDLE(clone);

    iRETURN;
}

iERR _ion_symbol_table_clone_with_owner_helper(ION_SYMBOL_TABLE **p_pclone, ION_SYMBOL_TABLE *orig, hOWNER owner, ION_SYMBOL_TABLE *system)
{
    iENTER;
    ION_SYMBOL_TABLE       *clone;
    BOOL                    new_owner, is_shared;
    ION_SYMBOL             *p_symbol;
    ION_COLLECTION_CURSOR   symbol_cursor;
    ION_COPY_FN             copy_fn;
    ION_SYMBOL_TABLE_TYPE   type;

    ASSERT(orig != NULL);
    ASSERT(p_pclone != NULL);

    IONCHECK(_ion_symbol_table_get_type_helper(orig, &type));
    switch(type) {
    default:
    case ist_EMPTY:
        FAILWITH(IERR_INVALID_STATE);
    case ist_LOCAL:
        IONCHECK(_ion_symbol_table_open_helper(&clone, owner, system));
        is_shared = FALSE;
        break;
    case ist_SYSTEM:  // system symbol tables are considered shared tables
    case ist_SHARED:
        // we don't copy the system symbols into shared tables
        IONCHECK(_ion_symbol_table_open_helper(&clone, owner, NULL));
        is_shared = TRUE;
        break;
    }   

    clone->max_id = orig->max_id;
    clone->system_symbol_table = orig->system_symbol_table;

    // since these value should be immutable if the owner
    // has NOT changed we can use cheaper copies
    new_owner = (orig->owner != owner);
    if (is_shared) {
        // if this is a shared table we copy the name and version
        clone->version = orig->version;
        if (new_owner) {
            // otherwise we have to do expensive copies
            IONCHECK(ion_string_copy_to_owner(owner, &clone->name, &orig->name));
        }
        else {
            // we get to share the name contents
            ION_STRING_ASSIGN(&clone->name, &orig->name);
        }
    }

    // now we move the imports
    copy_fn = new_owner ? _ion_symbol_table_local_import_copy_new_owner 
                        : _ion_symbol_table_local_import_copy_same_owner;
    IONCHECK(_ion_collection_copy(&clone->import_list, &orig->import_list, copy_fn, owner));

    // and finally copy the actual symbols
    copy_fn = new_owner ? _ion_symbol_local_copy_new_owner 
                        : _ion_symbol_local_copy_same_owner;
    IONCHECK(_ion_collection_copy(&clone->symbols, &orig->symbols, copy_fn, owner));

    // now adjust the symbol table owner handles (hsymtab)
    ION_COLLECTION_OPEN(&clone->symbols, symbol_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(symbol_cursor, p_symbol);
        if (!p_symbol) break;
        if (p_symbol->psymtab == orig) {
            p_symbol->psymtab = clone;
        }
    }
    ION_COLLECTION_CLOSE(symbol_cursor);

    *p_pclone = clone;

    iRETURN;
}

iERR ion_symbol_table_get_system_table(hSYMTAB *p_hsystem_table, int32_t version)
{
    iENTER;
    ION_SYMBOL_TABLE *system;

    if (p_hsystem_table != NULL) FAILWITH(IERR_INVALID_ARG);
    if (version != 1)            FAILWITH(IERR_INVALID_ION_VERSION);

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, version));
    *p_hsystem_table = PTR_TO_HANDLE(system);

    iRETURN;
}

// HACK - TODO - hate this, needs to be fixed up for thread safety issues
static ION_SYMBOL_TABLE *p_system_symbol_table_version_1 = NULL;
iERR _ion_symbol_table_get_system_symbol_helper(ION_SYMBOL_TABLE **pp_system_table, int32_t version)
{
    iENTER;

    ASSERT( pp_system_table != NULL );
    ASSERT( version == 1 ); // only one we understand at this point

    if (!p_system_symbol_table_version_1) {
        IONCHECK(_ion_symbol_table_local_make_system_symbol_table_helper(version));
    }
    *pp_system_table = p_system_symbol_table_version_1;

    iRETURN;
}


// currently the system symbol table uses 1304 bytes or so
#define kIonSystemSymbolMemorySize 2048
static char gSystemSymbolMemory[kIonSystemSymbolMemorySize];

void* smallLocalAllocationBlock();
void* smallLocalAllocationBlock()
{
    ION_ALLOCATION_CHAIN *new_block = (ION_ALLOCATION_CHAIN*)gSystemSymbolMemory;
    SIZE                  alloc_size = kIonSystemSymbolMemorySize;
    
    new_block->size     = alloc_size;
    
    new_block->next     = NULL;
    new_block->head     = NULL;
    
    new_block->position = ION_ALLOC_BLOCK_TO_USER_PTR(new_block);
    new_block->limit    = ((BYTE*)new_block) + alloc_size;
    
    return new_block->position;
}


// HACK - hate this
iERR _ion_symbol_table_local_make_system_symbol_table_helper(int32_t version)
{
    iENTER;
    ION_SYMBOL_TABLE      *psymtab;
    hOWNER sysBlock;

    ASSERT( version == 1 ); // only one we understand at this point
    ASSERT(p_system_symbol_table_version_1 == NULL);
    
    // need a SMALL block for the system symbol table
    sysBlock = smallLocalAllocationBlock();
    
    IONCHECK(_ion_symbol_table_open_helper(&psymtab, sysBlock, NULL));

    psymtab->version = version;
    ION_STRING_ASSIGN(&psymtab->name, &ION_SYMBOL_ION_STRING);
    psymtab->system_symbol_table = psymtab; // the system symbol table is it's own system symbol table (hmmm)

    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_ION_STRING, ION_SYS_SID_ION, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_VTM_STRING, ION_SYS_SID_IVM, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_SYMBOL_TABLE_STRING, ION_SYS_SID_SYMBOL_TABLE, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_NAME_STRING, ION_SYS_SID_NAME, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_VERSION_STRING, ION_SYS_SID_VERSION, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_IMPORTS_STRING, ION_SYS_SID_IMPORTS, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_SYMBOLS_STRING, ION_SYS_SID_SYMBOLS, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_MAX_ID_STRING, ION_SYS_SID_MAX_ID, psymtab));
    IONCHECK(_ion_symbol_table_add_symbol_and_sid_helper(psymtab, &ION_SYMBOL_SHARED_SYMBOL_TABLE_STRING, ION_SYS_SID_SHARED_SYMBOL_TABLE, psymtab));

    IONCHECK(_ion_symbol_table_lock_helper(psymtab));

    // TODO: THREADING - THIS ASSIGNMENT NEEDS TO BE MAKE THREAD SAFE !!
    // but we only need 1 copy of each system symbol table (the system symbol table)
    p_system_symbol_table_version_1 = psymtab;

    iRETURN;
}

iERR ion_symbol_table_free_system_table() 
{
    iENTER;
    ION_SYMBOL_TABLE *psymtab = p_system_symbol_table_version_1;

    if (!psymtab) {
        SUCCEED();
    }
    p_system_symbol_table_version_1 = NULL;

    // we don't free the system symbol table, since it isn't allocated
    // see: smallLocalAllocationBlock() which has a small global buffer for this
    // ion_free_owner(psymtab);

    iRETURN;
}

iERR _ion_symbol_table_local_load_import_list(ION_READER *preader, hOWNER owner, ION_COLLECTION *pimport_list)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import;
    ION_TYPE   type;
    ION_STRING str;
    SID        fld_sid;

    ION_STRING_INIT(&str);

    IONCHECK(_ion_reader_step_in_helper(preader));
    for (;;) {
        IONCHECK(_ion_reader_next_helper(preader, &type));
        if (type == tid_EOF) break;
        if (type != tid_STRUCT) continue;

        import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(pimport_list);
        memset(import, 0, sizeof(ION_SYMBOL_TABLE_IMPORT));
        import->max_id = ION_SYS_SYMBOL_MAX_ID_UNDEFINED;

        // step into the import struct
        IONCHECK(_ion_reader_step_in_helper(preader));
        for (;;) {
            IONCHECK(_ion_reader_next_helper(preader, &type));
            if (type == tid_EOF) break;

            IONCHECK(_ion_reader_get_field_sid_helper(preader, &fld_sid));
            switch(fld_sid) {
            case ION_SYS_SID_NAME:     /* "name" */
                if (!ION_STRING_IS_NULL(&import->name)) FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "too many names in import list");
                if (type == tid_STRING) {
                    IONCHECK(_ion_reader_read_string_helper(preader, &str));
                    IONCHECK(ion_string_copy_to_owner(owner, &import->name, &str));
                }
                break;
            case ION_SYS_SID_VERSION:  /* "version" */
                if (import->version) FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "too many versions in import list");
                if (type == tid_INT) {
                    IONCHECK(_ion_reader_read_int32_helper(preader, &import->version));
                }
                break;
            case ION_SYS_SID_MAX_ID:   /* "max_id" */
                // Edge case: the import contains n max_id declarations, and the first x <= n are explicitly -1. In this
                // case, the following line won't trigger a failure. However, the spec doesn't clearly define what
                // implementations must do when multiple of the same field is encountered in an import, so it doesn't
                // seem worth it to address this now.
                if (import->max_id != ION_SYS_SYMBOL_MAX_ID_UNDEFINED) FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "too many max_id fields in import list");
                BOOL is_null;
                IONCHECK(ion_reader_is_null(preader, &is_null));
                if (type == tid_INT && !is_null) {
                    IONCHECK(_ion_reader_read_int32_helper(preader, &import->max_id));
                }
                break;
            default:
                break;
            }
        }
        if (import->version < 1) {
            import->version = 1;
        }
        // step back out to the list of imports
        IONCHECK(_ion_reader_step_out_helper(preader));
    }
    // step back out to the symbol table struct
    IONCHECK(_ion_reader_step_out_helper(preader));

    iRETURN;
}

iERR _ion_symbol_table_local_load_symbol_struct(ION_READER *preader, hOWNER owner, ION_COLLECTION *psymbol_list)
{
    iENTER;
    ION_SYMBOL *sym;
    ION_TYPE    type;
    SID         sid;
    ION_STRING  str;

    IONCHECK(_ion_reader_step_in_helper(preader));
    for (;;) {
        IONCHECK(_ion_reader_next_helper(preader, &type));
        if (type == tid_EOF) break;
        if (type != tid_STRING) continue; // all symbol fields in the symbol struct have string values

        IONCHECK(_ion_reader_get_field_sid_helper(preader, &sid));
        IONCHECK(_ion_reader_read_string_helper(preader, &str));

        sym = (ION_SYMBOL *)_ion_collection_append(psymbol_list);

        IONCHECK(ion_string_copy_to_owner(owner, &sym->value, &str));
        sym->sid = sid;
        sym->psymtab = NULL;
    }
    // step back out to the symbol table struct
    IONCHECK(_ion_reader_step_out_helper(preader));

    iRETURN;
}

iERR _ion_symbol_table_local_load_symbol_list(ION_READER *preader, hOWNER owner, ION_COLLECTION *psymbol_list)
{
    iENTER;

    ION_SYMBOL *sym;
    ION_TYPE    type;
    ION_STRING  str;

    IONCHECK(_ion_reader_step_in_helper(preader));
    for (;;) {
        IONCHECK(_ion_reader_next_helper(preader, &type));
        if (type == tid_EOF) break;
        if (type != tid_STRING) continue; // all symbol fields in the symbol struct have string values

        IONCHECK(_ion_reader_read_string_helper(preader, &str));

        sym = (ION_SYMBOL *)_ion_collection_append(psymbol_list);
        
        IONCHECK(ion_string_copy_to_owner(owner, &sym->value, &str));
        sym->sid = UNKNOWN_SID;
        sym->psymtab = NULL;
    }
    // step back out to the symbol table struct
    IONCHECK(_ion_reader_step_out_helper(preader));

    iRETURN;
}

iERR ion_symbol_table_load(hREADER hreader, hOWNER owner, hSYMTAB *p_hsymtab)
{
    iENTER;
    ION_READER       *preader;
    ION_SYMBOL_TABLE *psymtab, *system;


    if (hreader   == NULL) FAILWITH(IERR_INVALID_ARG);
    if (owner     == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);

    preader = HANDLE_TO_PTR(hreader, ION_READER);

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    IONCHECK(_ion_symbol_table_load_helper(preader, owner, system, &psymtab));

    *p_hsymtab = PTR_TO_HANDLE(psymtab);

    iRETURN;
}

iERR _ion_symbol_table_load_helper(ION_READER *preader, hOWNER owner, ION_SYMBOL_TABLE *system, ION_SYMBOL_TABLE **p_psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE        *symtab;
    ION_TYPE                 type;
    ION_SYMBOL_TABLE_IMPORT *import;
    ION_SYMBOL              *symbol;
    int                      version = 0;
    SID                      fld_sid, sid, max_id = 0;
    BOOL                     is_shared_table, is_symbol_table;
    ION_STRING               str, name;
    ION_COLLECTION_CURSOR    import_cursor, symbol_cursor;

    ASSERT(preader   != NULL);
    ASSERT(owner     != NULL);
    ASSERT(p_psymtab != NULL);

    ION_STRING_INIT(&name);
    ION_STRING_INIT(&str);

    IONCHECK(_ion_reader_has_annotation_helper(preader, &ION_SYMBOL_SHARED_SYMBOL_TABLE_STRING, &is_shared_table));
    if (!is_shared_table) {
        IONCHECK(_ion_reader_has_annotation_helper(preader, &ION_SYMBOL_SYMBOL_TABLE_STRING, &is_symbol_table));
    }
    else {
        is_symbol_table = TRUE;
    }
    if (!is_symbol_table) FAILWITH(IERR_NOT_A_SYMBOL_TABLE);

    // shared symbol tables don't need the system table symbols, but local tables do
    if (is_shared_table) {
        IONCHECK(_ion_symbol_table_open_helper(&symtab, owner, NULL));
        symtab->system_symbol_table = system; // we still need this reference, we just don't incoporate the symbols into the table
    }
    else {
        IONCHECK(_ion_symbol_table_open_helper(&symtab, owner, system));
    }

    // now we step into the struct that has the data we actually use to fill out the table
    IONCHECK(_ion_reader_step_in_helper(preader));

    for (;;) {
        IONCHECK(_ion_reader_next_helper(preader, &type));
        if (type == tid_EOF) break;
        IONCHECK(_ion_reader_get_field_sid_helper(preader, &fld_sid));
        switch(fld_sid) {
        case ION_SYS_SID_NAME:     /* "name" */
            if (!is_shared_table) break; // no meaning for local tables
            if (!ION_STRING_IS_NULL(&symtab->name)) break; // FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "too many names");
            if (type == tid_STRING) {
                IONCHECK(_ion_reader_read_string_helper(preader, &str));
                if (ION_STRING_IS_NULL(&str) || str.length < 1) break; // not any name we want
                IONCHECK(ion_string_copy_to_owner(owner, &name, &str));
            }
            break;
        case ION_SYS_SID_VERSION:  /* "version" */
            if (!is_shared_table) break; // no meaning for local tables
            if (symtab->version) break; // FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "too many versions");
            if (type == tid_INT) {
                IONCHECK(_ion_reader_read_int32_helper(preader, &version));
                if (version < 1) break; // FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "version must be 1 or greater");
            }
            break;
        case ION_SYS_SID_IMPORTS:  /* "imports" */
            if (type == tid_LIST) {
                IONCHECK(_ion_symbol_table_local_load_import_list(preader, owner, &symtab->import_list));
            }
            break;
        case ION_SYS_SID_SYMBOLS:  /* "symbols" */
            if (type == tid_STRUCT) {
                IONCHECK(_ion_symbol_table_local_load_symbol_struct(preader, owner, &symtab->symbols));
            }
            else if (type == tid_LIST) {
                IONCHECK(_ion_symbol_table_local_load_symbol_list(preader, owner, &symtab->symbols));
            }
            break;
        case ION_SYS_SID_MAX_ID:   /* "max_id" */
            if (!is_shared_table) break; // no meaning for local tables
            if (max_id > 0) break; // FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "too many import lists");
            if (type == tid_INT) {
                IONCHECK(_ion_reader_read_int32_helper(preader, &max_id));
                if (max_id < 1) FAILWITHMSG(IERR_INVALID_SYMBOL_TABLE, "max_id must be 1 or greater");
            }
            break;
        default:
            // we just ignore "extra" fields
            break;
        }
    }

    IONCHECK(_ion_reader_step_out_helper(preader));
       

    // now we check for our import list since we need to import before adding symbols
    // but only if this a non-shared table - shared tables have already had the
    // symbols included in their symbol list already
    if (!is_shared_table && !ION_COLLECTION_IS_EMPTY(&symtab->import_list)) {
        ION_COLLECTION_OPEN(&symtab->import_list, import_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(import_cursor, import);
            if (!import) break;
            if (!preader->_catalog) {
                IONCHECK(_ion_catalog_open_with_owner_helper(&preader->_catalog, preader));
            }
            IONCHECK(_ion_symbol_table_add_import_helper(symtab, import, preader->_catalog));
        }
        ION_COLLECTION_CLOSE(import_cursor);
    }

    // now adjust the symbol table owner handles (hsymtab)
    // and the sid values for any local symbols we stored but
    // didn't fully initialize
    if (!ION_COLLECTION_IS_EMPTY(&symtab->symbols)) {
        symtab->has_local_symbols = TRUE; // and now we know it has some local symbols
        sid = symtab->max_id;
        ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, symbol);
            if (!symbol) break;
            if (symbol->sid == UNKNOWN_SID) {  // TODO -------------------------- WHAT IS THE RIGHT BEHAVIOR?
                sid++;
                symbol->sid = sid;
            }
            if (symbol->psymtab == NULL) {
                symbol->psymtab = symtab;
            }
        }
        ION_COLLECTION_CLOSE(symbol_cursor);
        symtab->max_id = sid;
    }

    // we grabbed these values as they went by (if they were there) now assign them
    if (is_shared_table) {
        if (version > 0) {
            symtab->version = version;
        }
        else {
            symtab->version = 1;
        }
        // we can only make the max_id shorter
        if (max_id > 0 && max_id < symtab->max_id) symtab->max_id = max_id;
        if (!ION_STRING_IS_NULL(&name)) {
            ION_STRING_ASSIGN(&symtab->name, &name);
        }
    }

    IONCHECK(_ion_symbol_table_initialize_indices_helper(symtab));

    *p_psymtab = symtab;

    iRETURN;
}

iERR ion_symbol_table_unload(hSYMTAB hsymtab, hWRITER hwriter)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;
    ION_WRITER *pwriter;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (hwriter == NULL) FAILWITH(IERR_INVALID_ARG);

    symtab  = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_symbol_table_unload_helper(symtab, pwriter));

    iRETURN;
}

iERR _ion_symbol_table_unload_helper(ION_SYMBOL_TABLE *symtab, ION_WRITER *pwriter)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import;
    ION_SYMBOL              *sym;
    BOOL                     has_symbols;
    SID                      sid, annotation;
    ION_COLLECTION_CURSOR    symbol_cursor, import_cursor;
    ION_SYMBOL_TABLE_TYPE    table_type;


    ASSERT(symtab != NULL);
    ASSERT(pwriter != NULL);

    IONCHECK(_ion_symbol_table_get_type_helper(symtab, &table_type));
    switch (table_type) {
    case ist_LOCAL:
        annotation = ION_SYS_SID_SYMBOL_TABLE;
        break;
    case ist_SHARED:
    case ist_SYSTEM: // system tables are just shared tables
        annotation = ION_SYS_SID_SHARED_SYMBOL_TABLE;
        break;
    default:
        annotation = UNKNOWN_SID;
        break;
    }   

    // we annotate the struct appropriately for the table type
    // with no annotation for tables that don't have a recognizable type
    if (annotation != UNKNOWN_SID) {
        IONCHECK(_ion_writer_add_annotation_sid_helper(pwriter, annotation));
    }
    IONCHECK(_ion_writer_start_container_helper(pwriter, tid_STRUCT));

    if (!ION_STRING_IS_NULL(&symtab->name)) {
        IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_NAME));
        IONCHECK(_ion_writer_write_string_helper(pwriter, &symtab->name));
    }
    if (symtab->version > 0) {
        IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_VERSION));
        IONCHECK(_ion_writer_write_int64_helper(pwriter, symtab->version));
    }
    // HACK - we need a table type that's assigned to the table when it's created (or
    //        when the type is set) and that should control the max id and sid assignment
    if (symtab->max_id > 0 && table_type != ist_SHARED) {   
        IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_MAX_ID));
        IONCHECK(_ion_writer_write_int64_helper(pwriter, symtab->max_id));
    }

    if (!ION_COLLECTION_IS_EMPTY(&symtab->import_list)) {
        IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_IMPORTS));
        IONCHECK(_ion_writer_start_container_helper(pwriter, tid_LIST));

        ION_COLLECTION_OPEN(&symtab->import_list, import_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(import_cursor, import);
            if (!import) break;
        
            IONCHECK(_ion_writer_start_container_helper(pwriter, tid_STRUCT));
            if (!ION_STRING_IS_NULL(&import->name)) {
                IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_NAME));
                IONCHECK(_ion_writer_write_string_helper(pwriter, &import->name));
            }
            if (import->version > 0) {
                IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_VERSION));
                IONCHECK(_ion_writer_write_int64_helper(pwriter, import->version));
            }
            if (import->max_id > 0) {
                IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_MAX_ID));
                IONCHECK(_ion_writer_write_int64_helper(pwriter, import->max_id));
            }
            IONCHECK(_ion_writer_finish_container_helper(pwriter));
        }
        ION_COLLECTION_CLOSE(import_cursor);
        IONCHECK(_ion_writer_finish_container_helper(pwriter));
    }

    has_symbols = FALSE;
    sid = 0;
    ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(symbol_cursor, sym);
        if (!sym) break;
        if (sym->psymtab == symtab) {
            has_symbols = TRUE;
            break;
        }
        sid = sym->sid;
    }
    ION_COLLECTION_CLOSE(symbol_cursor);

    if (has_symbols) {
        // start the symbols list
        IONCHECK(_ion_writer_write_field_sid_helper(pwriter, ION_SYS_SID_SYMBOLS));
        IONCHECK(_ion_writer_start_container_helper(pwriter, tid_LIST));

        ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, sym);
            if (!sym) break;
            if (sym->psymtab == symtab) {
                IONCHECK(_ion_writer_write_string_helper(pwriter, &sym->value));
            }
        }
        ION_COLLECTION_CLOSE(symbol_cursor);

        IONCHECK(_ion_writer_finish_container_helper(pwriter)); // close the symbol list
    }

    IONCHECK(_ion_writer_finish_container_helper(pwriter));

    iRETURN;
}

iERR ion_symbol_table_lock(hSYMTAB hsymtab)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_lock_helper(symtab));

    iRETURN;
}

iERR _ion_symbol_table_lock_helper(ION_SYMBOL_TABLE *symtab)
{
    iENTER;
    ASSERT(symtab != NULL);
    if (symtab->is_locked) SUCCEED();

    if (symtab->max_id > 0 && !INDEX_IS_ACTIVE(symtab)) {
        IONCHECK(_ion_symbol_table_initialize_indices_helper(symtab));
    }

    symtab->is_locked = TRUE;

    iRETURN;;
}

iERR ion_symbol_table_is_locked(hSYMTAB hsymtab, BOOL *p_is_locked)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_is_locked == NULL) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_is_locked_helper(symtab, p_is_locked));

    iRETURN;
}

iERR _ion_symbol_table_is_locked_helper(ION_SYMBOL_TABLE *symtab, BOOL *p_is_locked)
{
    ASSERT(symtab != NULL);
    ASSERT(p_is_locked != NULL);

    *p_is_locked = symtab->is_locked;

    return IERR_OK;
}

iERR ion_symbol_table_get_type(hSYMTAB hsymtab, ION_SYMBOL_TABLE_TYPE *p_type)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_type == NULL)  FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_type_helper(symtab, p_type));

    iRETURN;
}

iERR _ion_symbol_table_get_type_helper(ION_SYMBOL_TABLE *symtab, ION_SYMBOL_TABLE_TYPE *p_type)
{
    ION_SYMBOL_TABLE_TYPE type = ist_EMPTY;

    ASSERT(symtab != NULL);
    ASSERT(p_type != NULL);

    if (!ION_STRING_IS_NULL(&symtab->name)) {
        // it's either system or shared
        if (symtab->version == 1 
         && ION_STRING_EQUALS(&symtab->name, &ION_SYMBOL_VTM_STRING)
        ) {
            type = ist_SYSTEM;
        }
        else {
            type = ist_SHARED;
        }
    }
    else {
        type = ist_LOCAL;
    }

    *p_type = type;

    return IERR_OK;
}

iERR ion_symbol_table_get_name(hSYMTAB hsymtab, iSTRING p_name)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_name == NULL)  FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_name_helper(symtab, p_name));
    
    iRETURN;
}

iERR _ion_symbol_table_get_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *p_name)
{
    ASSERT(symtab != NULL);
    ASSERT(p_name != NULL);

    ION_STRING_ASSIGN(p_name, &symtab->name);
    
    return IERR_OK;
}

iERR ion_symbol_table_get_version(hSYMTAB hsymtab, int32_t *p_version)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_version == NULL)  FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_version_helper(symtab, p_version));

    iRETURN;
}

iERR _ion_symbol_table_get_version_helper(ION_SYMBOL_TABLE *symtab, int32_t *p_version)
{
    ASSERT(symtab != NULL);
    ASSERT(p_version != NULL);

    *p_version = symtab->version;

    return IERR_OK;
}

iERR ion_symbol_table_get_max_sid(hSYMTAB hsymtab, SID *p_max_id)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_max_id == NULL)  FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_max_sid_helper(symtab, p_max_id));

    iRETURN;
}

iERR _ion_symbol_table_get_max_sid_helper(ION_SYMBOL_TABLE *symtab, SID *p_max_id)
{
    int max_id;

    ASSERT(symtab != NULL);
    ASSERT(p_max_id != NULL);

    max_id = symtab->max_id;
    if (max_id <= 0) {
        ASSERT(symtab->system_symbol_table != NULL);
        max_id = symtab->system_symbol_table->max_id;

    }

    *p_max_id = max_id;

    return IERR_OK;
}

iERR ion_symbol_table_set_name(hSYMTAB hsymtab, iSTRING name)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (ION_STRING_IS_NULL(name)) FAILWITH(IERR_INVALID_ARG);
    if (name->length < 1) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_set_name_helper(symtab, name));

    iRETURN;
}

iERR _ion_symbol_table_set_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name)
{
    iENTER;

    ASSERT(symtab != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(name->length > 0);

    if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);

    IONCHECK(ion_string_copy_to_owner(symtab->owner, &symtab->name, name));    

    iRETURN;
}

iERR ion_symbol_table_set_version(hSYMTAB hsymtab, int32_t version)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (version < 0) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_set_version_helper(symtab, version));

    iRETURN;
}

iERR _ion_symbol_table_set_version_helper(ION_SYMBOL_TABLE *symtab, int32_t version)
{
    iENTER;

    ASSERT(symtab != NULL);
    ASSERT(version >= 0);

    if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);

    symtab->version = version;

    iRETURN;
}

iERR ion_symbol_table_set_max_sid(hSYMTAB hsymtab, SID max_id)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (max_id < 0) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_set_max_sid_helper(symtab, max_id));

    iRETURN;
}

iERR _ion_symbol_table_set_max_sid_helper(ION_SYMBOL_TABLE *symtab, SID max_id)
{
    iENTER;

    ASSERT(symtab != NULL);
    ASSERT(max_id >= 0);

    if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);

    symtab->max_id = max_id;

    iRETURN;
}

iERR ion_symbol_table_get_imports(hSYMTAB hsymtab, ION_COLLECTION **p_imports)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    if (p_imports == NULL) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_get_imports_helper(symtab, p_imports));

    iRETURN;
}

iERR _ion_symbol_table_get_imports_helper(ION_SYMBOL_TABLE *symtab, ION_COLLECTION **p_imports)
{
    ASSERT(symtab != NULL);
    ASSERT(p_imports != NULL);

    *p_imports = &symtab->import_list;

    return IERR_OK;
}

iERR ion_symbol_table_import_symbol_table(hSYMTAB hsymtab, hSYMTAB hsymtab_import)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab, *import;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
    if (hsymtab_import == NULL) FAILWITH(IERR_INVALID_ARG);
    import = HANDLE_TO_PTR(hsymtab_import, ION_SYMBOL_TABLE);

    if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);

    IONCHECK(_ion_symbol_table_import_symbol_table_helper(symtab, import));

    iRETURN;
}

iERR _ion_symbol_table_import_symbol_table_helper(ION_SYMBOL_TABLE *symtab, ION_SYMBOL_TABLE *import_symtab)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import;

    import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&symtab->import_list);
    if (!import) FAILWITH(IERR_NO_MEMORY);

    memset(import, 0, sizeof(ION_SYMBOL_TABLE_IMPORT));
    import->max_id = import_symtab->max_id;
    import->version = import_symtab->version;
    IONCHECK(ion_string_copy_to_owner(symtab->owner, &import->name, &import_symtab->name));

    IONCHECK(_ion_symbol_table_local_incorporate_symbols(symtab, import_symtab, import_symtab->max_id));

    iRETURN;
}

iERR ion_symbol_table_add_import(hSYMTAB hsymtab, ION_SYMBOL_TABLE_IMPORT *p_import)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
    if (p_import == NULL) FAILWITH(IERR_INVALID_ARG);
    if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);
    if (symtab->has_local_symbols) FAILWITH(IERR_HAS_LOCAL_SYMBOLS);

    IONCHECK(_ion_symbol_table_add_import_helper(symtab, p_import, symtab->catalog));

    iRETURN;
}

iERR _ion_symbol_table_add_import_helper(ION_SYMBOL_TABLE        *symtab
                                        ,ION_SYMBOL_TABLE_IMPORT *p_import
                                        ,ION_CATALOG             *pcatalog
                                        )
{
    iENTER;
    ION_SYMBOL_TABLE *import_symbol_table;
    int32_t version = (p_import->version < 1) ? 1 : p_import->version;

    ASSERT(symtab != NULL);
    ASSERT(p_import != NULL);
    ASSERT(!symtab->is_locked);
    ASSERT(!symtab->has_local_symbols);

    if (!pcatalog) FAILWITH(IERR_IMPORT_NOT_FOUND);

    IONCHECK(_ion_catalog_find_best_match_helper(pcatalog, &p_import->name, version, p_import->max_id, &import_symbol_table));
    if (import_symbol_table == NULL) {
        IONCHECK(_ion_symbol_table_create_substitute(p_import, pcatalog, &import_symbol_table));
    }

    IONCHECK(_ion_symbol_table_local_incorporate_symbols(symtab, import_symbol_table, p_import->max_id));

    iRETURN;
}

iERR _ion_symbol_table_local_incorporate_symbols(ION_SYMBOL_TABLE *symtab, ION_SYMBOL_TABLE *import, int32_t import_max_id)
{
    iENTER;
    ION_COLLECTION_CURSOR cursor;
    int32_t               base_max, duplicates;
    ION_SYMBOL           *sym;
    SID                   sid;
    iERR               addErr;

    ASSERT(symtab != NULL);
    ASSERT(import != NULL);
    ASSERT(!symtab->is_locked);
    ASSERT(!symtab->has_local_symbols);

    // we're adding to what already there, so we have to offset the sid's
    base_max = symtab->symbols._count;//symtab->max_id;
    duplicates = 0;

    if (!ION_COLLECTION_IS_EMPTY(&import->symbols)) {

        ION_COLLECTION_OPEN(&import->symbols, cursor);
        for (;;) {
            ION_COLLECTION_NEXT(cursor, sym);
            if (sym == NULL) break;
            if (sym->sid > import->max_id) continue;
            sid = sym->sid + base_max - duplicates;
            
            addErr = _ion_symbol_table_add_symbol_and_sid_helper(symtab, &sym->value, sid, sym->psymtab);
            if (addErr == IERR_DUPLICATE_SYMBOL)
            {
                ++duplicates;
            }
            else
            {
                IONCHECK(addErr);
            }
        }
        ION_COLLECTION_CLOSE( cursor );
    }

    // the symbol tables max id should have bumped up to match this:
//    ASSERT(symtab->max_id == base_max + import_max_id - duplicates);

    iRETURN;
}

iERR _ion_symbol_table_local_find_by_name(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid, ION_SYMBOL **p_sym)
{
    iENTER;
    ION_COLLECTION_CURSOR   symbol_cursor;
    ION_SYMBOL             *sym;
    int                     ii, c;
    SID                     sid;
    ION_SYMBOL_TABLE       *system_table;

    if(ION_STRING_IS_NULL(name)) {
        FAILWITH(IERR_NULL_VALUE);
    }
    
    ASSERT(symtab);
    ASSERT(p_sid != NULL);

    if (!INDEX_IS_ACTIVE(symtab) && symtab->max_id > DEFAULT_INDEX_BUILD_THRESHOLD) {
        IONCHECK(_ion_symbol_table_initialize_indices_helper(symtab));
    }

    if (INDEX_IS_ACTIVE(symtab)) {
        sym = _ion_symbol_table_index_find_by_name_helper(symtab, name);
    }
    else {
        // we only do this when there aren't very many symbols (see threshold above)
        ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, sym);
            if (!sym) break;
            if (ION_STRING_EQUALS(name, &sym->value)) {
                break;
            }
        }
        ION_COLLECTION_CLOSE(symbol_cursor);

    }
    // if we didn't find it when we tried to look it up, see if it's one
    // of the "$<int> symbols
    if (sym) {
        sid = sym->sid;
    }
    else if (name->value[0] == '$' && name->length > 1) {
        sid = 0;
        for (ii=1; ii<name->length; ii++) {
            c = name->value[ii];
            if (c < '0' || c > '9') {
                sid = UNKNOWN_SID;
                goto done;
            }
            sid *= 10;
            sid += c - '0';
        }
        if (sid == 0) {
            goto done;
        }
        IONCHECK(_ion_symbol_table_local_find_by_sid(symtab, sid, &sym));
        if (!sym) {
            IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system_table, ION_SYSTEM_VERSION));
            IONCHECK(_ion_symbol_table_local_find_by_sid(system_table, sid, &sym));
            if (!sym) {
                FAILWITH(IERR_INVALID_SYMBOL); // The requested SID is out of range for the current symtab context.
            }
        }
    }
    else {
        sid = UNKNOWN_SID;
    }
done:
    if (p_sid) *p_sid = sid;
    if (p_sym) *p_sym = sym;

    iRETURN;
}

iERR ion_symbol_table_find_by_name(hSYMTAB hsymtab, iSTRING name, SID *p_sid)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL)            FAILWITH(IERR_INVALID_ARG);
    if (name == NULL)               FAILWITH(IERR_INVALID_ARG);
    if (ION_STRING_IS_NULL(name))   FAILWITH(IERR_INVALID_ARG);
    if (name->length < 1)           FAILWITH(IERR_INVALID_ARG);
    if (p_sid == NULL)              FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    // if we were to delegate: IONCHECK(_ion_symbol_table_find_by_name_helper(symtab, name, p_sid));
    IONCHECK(_ion_symbol_table_local_find_by_name(symtab, name, p_sid, NULL));

    iRETURN;
}


#ifdef IF_WE_WANT_TO_DELEGATE_LOOKUPS
    // we put all the symbols into out local symbol list so that we don't
    // have to go through the "look it up in this list of tables" effort

iERR _ion_symbol_table_find_by_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid)
{
    iENTER;
    ION_SYMBOL_TABLE        *imported;
    ION_CATALOG             *catalog;
    SID                      sid = UNKNOWN_SID;
    ION_SYMBOL_TABLE_IMPORT *imp;
    ION_COLLECTION_CURSOR    import_cursor;
    int32_t                  offset;

    ASSERT(symtab != NULL);
    ASSERT(name != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(name->length > 0);
    ASSERT(p_sid != NULL);


    // first we check the system symbol table, if there is one
   IONCHECK(_ion_symbol_table_local_find_by_name(symtab->system_symbol_table, name, &sid));

    // first we have to look in the imported tables
    // TODO:  make this smarter

    // really if this is a local symbol table there should be only 1 import
    // and if this is a shared (named) symbol table you only have to look
    // locally since shared table have all the imported symbols in their list
    catalog = symtab->catalog;
    if (sid == UNKNOWN_SID && catalog && !ION_COLLECTION_IS_EMPTY(&symtab->import_list)) {
        offset = symtab->system_symbol_table->max_id;

        ION_COLLECTION_OPEN(&symtab->import_list, import_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(import_cursor, imp);
            if (!imp) break;
            IONCHECK(_ion_catalog_find_best_match_helper(catalog, &imp->name, imp->version, &imported));
            IONCHECK(_ion_symbol_table_local_find_by_name(imported, name, &sid));
            if (sid > imp->max_id) sid = UNKNOWN_SID;
            if (sid != UNKNOWN_SID) {
                sid += offset;
                break;
            }
            offset += imp->max_id;
        }
        ION_COLLECTION_CLOSE(import_cursor);
    }

    // and last we look in the local table itself
    if (sid == UNKNOWN_SID) {
        IONCHECK(_ion_symbol_table_local_find_by_name(symtab, name, &sid));
    }

    *p_sid = sid;

    iRETURN;
}
#endif

iERR _ion_symbol_table_local_find_by_sid(ION_SYMBOL_TABLE *symtab, SID sid, ION_SYMBOL **p_sym)
{
    iENTER;
    ION_SYMBOL              *sym;
    ION_COLLECTION_CURSOR    symbol_cursor;

    ASSERT(symtab != NULL);
    ASSERT(p_sym != NULL);

    if (!INDEX_IS_ACTIVE(symtab) && symtab->max_id > DEFAULT_INDEX_BUILD_THRESHOLD) {
        IONCHECK(_ion_symbol_table_initialize_indices_helper(symtab));
    }
    if (INDEX_IS_ACTIVE(symtab)) {
        sym = _ion_symbol_table_index_find_by_sid_helper(symtab, sid);
    }
    else {
        // this really needs to be better!
        ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
        for(;;) {
            ION_COLLECTION_NEXT(symbol_cursor, sym);
            if (!sym) break;
            if (sym->sid == sid) {
                break;
            }
        }
        ION_COLLECTION_CLOSE(symbol_cursor);

    }
    *p_sym = sym;

    iRETURN;
}

iERR ion_symbol_table_find_by_sid(hSYMTAB hsymtab, SID sid, iSTRING *p_name)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL)    FAILWITH(IERR_INVALID_ARG);
    if (sid < UNKNOWN_SID)  FAILWITH(IERR_INVALID_ARG);
    if (p_name == NULL)     FAILWITH(IERR_INVALID_ARG);

    if (sid == UNKNOWN_SID) {
        p_name = NULL;
    }
    else {
        symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
        IONCHECK(_ion_symbol_table_find_by_sid_helper(symtab, sid, p_name));
    }

    iRETURN;
}

iERR _ion_symbol_table_find_by_sid_helper(ION_SYMBOL_TABLE *symtab, SID sid, ION_STRING **p_name)
{
    iENTER;
    int32_t                  len;
    ION_STRING              *str;
    ION_SYMBOL              *sym;
    char                     temp[1 + MAX_INT32_LENGTH + 1]; // '$' <int> '\0'

    ASSERT(symtab != NULL);
    ASSERT(p_name != NULL);
    ASSERT(sid > UNKNOWN_SID);
//    ASSERT(symtab->system_symbol_table != NULL);

    *p_name = NULL;
    
    IONCHECK(_ion_symbol_table_local_find_by_sid(symtab, sid, &sym));
    if (sym != NULL) {
        *p_name = &sym->value;
        SUCCEED();
    }

    // we didn't find it - make a scratch string with the $<int> form of the name
    temp[0] = '$';
    len = (int32_t)strlen(_ion_itoa_10(sid, temp + 1, sizeof(temp)-1)) + 1; // we're writing into the 2nd byte
    str = ion_alloc_with_owner(symtab->owner, sizeof(ION_STRING));
    if (!str) FAILWITH(IERR_NO_MEMORY);
    str->length = len;
    str->value = ion_alloc_with_owner(symtab->owner, len);
    if (!str->value) FAILWITH(IERR_NO_MEMORY);
    memcpy(str->value, temp, len);
    *p_name = str;

    iRETURN;
}

iERR ion_symbol_table_is_symbol_known(hSYMTAB hsymtab, SID sid, BOOL *p_is_known)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL)    FAILWITH(IERR_INVALID_ARG);
    if (sid < UNKNOWN_SID)  FAILWITH(IERR_INVALID_ARG);
    if (p_is_known == NULL) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_is_symbol_known_helper(symtab, sid, p_is_known));

    iRETURN;
}

iERR _ion_symbol_table_is_symbol_known_helper(ION_SYMBOL_TABLE *symtab, SID sid, BOOL *p_is_known)
{
    iENTER;
    ION_STRING *pname = NULL;

    ASSERT(symtab != NULL);
    ASSERT(p_is_known != NULL);
    IONCHECK(_ion_symbol_table_find_by_sid_helper(symtab, sid, &pname));

    // TODO is this ever FALSE? Also should succeed on IERR_UNKNOWN_SYMBOL
    *p_is_known = (pname != NULL);

    iRETURN;
}

// get symbols by sid, iterate from 1 to max_sid - returns all symbol
iERR ion_symbol_table_get_symbol(hSYMTAB hsymtab, SID sid, ION_SYMBOL **p_sym)
{
    iENTER;
    ION_SYMBOL *sym;
    ION_SYMBOL_TABLE *symtab;

    if (!hsymtab) FAILWITH(IERR_INVALID_ARG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
    if (sid < UNKNOWN_SID || sid > symtab->max_id) FAILWITH(IERR_INVALID_ARG);
    if (!p_sym) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_symbol_table_local_find_by_sid(symtab, sid, &sym));
    *p_sym = sym;

    iRETURN;
}

// get symbols by sid, iterate from 1 to max_sid - returns only locally defined symbols
iERR ion_symbol_table_get_local_symbol(hSYMTAB hsymtab, SID sid, ION_SYMBOL **p_sym)
{
    iENTER;
    ION_SYMBOL *sym;
    ION_SYMBOL_TABLE *symtab;

    if (!hsymtab) FAILWITH(IERR_INVALID_ARG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
    if (sid < UNKNOWN_SID || sid > symtab->max_id) FAILWITH(IERR_INVALID_ARG);
    if (!p_sym) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_symbol_table_local_find_by_sid(symtab, sid, &sym));
    if (sym) {
        if (sym->psymtab != symtab) sym = NULL;
    }
    *p_sym = sym;

    iRETURN;
}


iERR ion_symbol_table_add_symbol(hSYMTAB hsymtab, iSTRING name, SID *p_sid)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL)            FAILWITH(IERR_INVALID_ARG);
    if (ION_STRING_IS_NULL(name))   FAILWITH(IERR_INVALID_ARG);
    if (name->length < 0)           FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_add_symbol_helper(symtab, name, p_sid));

    iRETURN;
}

iERR _ion_symbol_table_add_symbol_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid)
{
    iENTER;
    SID         sid;
    ION_SYMBOL *sym;

    ASSERT(symtab != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));

    // with delegate: IONCHECK(_ion_symbol_table_find_by_name_helper(symtab, name, &temp_sid));
    IONCHECK(_ion_symbol_table_local_find_by_name(symtab, name, &sid, &sym));
    if (sid == UNKNOWN_SID) {
        // make sure it's really ok to add new symbols
        if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);

        // we'll assign this symbol to the next id (_add_ will update max_id for us)
        sid = symtab->max_id + 1;
        IONCHECK(_ion_symbol_table_local_add_symbol_helper(symtab, name, sid, symtab, &sym));
    }

    if (sym) sym->add_count++;
    if (p_sid) *p_sid = sid;

    iRETURN;
}

iERR _ion_symbol_table_add_symbol_and_sid_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID sid, ION_SYMBOL_TABLE *symbol_owning_table)
{
    iENTER;
    SID temp_sid;

    ASSERT(symtab != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(name->length > 0);
    ASSERT(sid > UNKNOWN_SID);

    if (symtab->is_locked) FAILWITH(IERR_IS_IMMUTABLE);

    IONCHECK(_ion_symbol_table_local_find_by_name(symtab, name, &temp_sid, NULL));
    if (temp_sid == sid) {
        // we found the name and the sid matches, it's all good.
        SUCCEED();
    }
    if (temp_sid != UNKNOWN_SID) {
        // if you specify a sid and it doesn't match then duplicates are errors
        // (even though generally adding a symbol that's dup isn't an error)
        DONTFAILWITH(IERR_DUPLICATE_SYMBOL);
    }

    IONCHECK(_ion_symbol_table_local_add_symbol_helper(symtab, name, sid, symbol_owning_table, NULL));

    iRETURN;
}

iERR _ion_symbol_table_local_add_symbol_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID sid, ION_SYMBOL_TABLE *symbol_owning_table, ION_SYMBOL **p_psym)
{
    iENTER;
    ION_SYMBOL *sym;
    SIZE        trailing_bytes = 0;

    ASSERT(symtab != NULL);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(name->length >= 0);
    ASSERT(sid > UNKNOWN_SID);
    ASSERT(!symtab->is_locked);

    sym = (ION_SYMBOL *)_ion_collection_append(&symtab->symbols);
    if (!sym) FAILWITH(IERR_NO_MEMORY);
    memset(sym, 0, sizeof(ION_SYMBOL));

    // see if the named they passed is value bytes
    IONCHECK(_ion_reader_binary_validate_utf8(name->value, name->length, trailing_bytes, &trailing_bytes));
    if (trailing_bytes != 0) FAILWITH(IERR_INVALID_UTF8);

    
    IONCHECK(ion_string_copy_to_owner(symtab->owner, &sym->value, name));
    sym->sid = sid;
    if (sym->sid > symtab->max_id) 
    {
        symtab->max_id = sym->sid;
    }
    sym->psymtab = symbol_owning_table;
    if (symbol_owning_table == symtab) symtab->has_local_symbols = TRUE;

    if (INDEX_IS_ACTIVE(symtab)) {
        IONCHECK(_ion_symbol_table_index_insert_helper(symtab, sym));
    }

    if (p_psym) *p_psym = sym;

    iRETURN;
}

iERR ion_symbol_table_close(hSYMTAB hsymtab)
{
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    if (hsymtab == NULL) FAILWITH(IERR_INVALID_ARG);

    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_symbol_table_close_helper(symtab));

    iRETURN;
}

iERR _ion_symbol_table_close_helper(ION_SYMBOL_TABLE *symtab)
{
    iENTER;

    ASSERT(symtab != NULL);

    if (symtab->catalog) {
        IONCHECK(_ion_catalog_release_symbol_table_helper(symtab->catalog, symtab));
        symtab->catalog = NULL;
    }

    if (symtab->owner == symtab) {
        ion_free_owner(symtab);
    }

    iRETURN;
}

//
// these are helpers for validating symbols for a variety of purposes
// especially to handle cstr vs ion_string at the user boundary
//
BOOL _ion_symbol_needs_quotes_string(ION_STRING *p_str)
{
    if (!p_str || !p_str->value) return FALSE;
    return _ion_symbol_needs_quotes_cstr_length((char *)p_str->value, p_str->length);
}

BOOL _ion_symbol_needs_quotes_cstr(char *cp)
{
    SIZE len;
    if (!cp) return FALSE;
    len = strlen(cp);       // yuck
    return _ion_symbol_needs_quotes_cstr_length(cp, len);
}

BOOL _ion_symbol_needs_quotes_cstr_length(char *cp, SIZE length)
{
    char *start, *limit;
    BOOL  is_possible_keyword = FALSE;

    if (!cp) return FALSE;
    if (length < 1) return TRUE;

    start = cp;
    limit = cp + length;

    // check the first character for $, _, or alpha
    switch(*cp) {
        case '$': case '_':
            break;
        case 't': case 'f': case 'n': // true, false, null, nan
            is_possible_keyword = TRUE;
            break;
        case 'a': case 'b': case 'c': case 'd': case 'e': /* 'f' */
        case 'g': case 'h': case 'i': case 'j': case 'k': case 'l':
        case 'm': /* 'n' */ case 'o': case 'p': case 'q': case 'r':
        case 's': /* 't' */ case 'u': case 'v': case 'w': case 'x':
        case 'y': case 'z':
        case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
        case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
        case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
        case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
        case 'Y': case 'Z':
            break;
        case '0': case '1': case '2': case '3': case '4':
        case '5': case '6': case '7': case '8': case '9':
        default:
            return TRUE;
    }
    cp++;

    // now check the rest
    while (cp < limit) {
        switch(*cp) {
                // all alpha-numeric that are non-leading chars in: false, true, nan and null
                // which is: a e l n r s u
            case 'a': case 'e': case 'l': case 'n':
            case 'r': case 's': case 'u':
                break;
                // all alpha-numeric that are NOT non-leading chars in: false, true, nan and null
            case '$': case '_':
                /* 'a' */ case 'b': case 'c': case 'd': /* 'e' */ case 'f':
            case 'g': case 'h': case 'i': case 'j': case 'k': /* 'l' */
            case 'm': /* 'n' */ case 'o': case 'p': case 'q': /* 'r' */
                /* 's' */ case 't': /* 'u' */ case 'v': case 'w': case 'x':
            case 'y': case 'z':
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
            case 'G': case 'H': case 'I': case 'J': case 'K': case 'L':
            case 'M': case 'N': case 'O': case 'P': case 'Q': case 'R':
            case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
            case 'Y': case 'Z':
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':
                is_possible_keyword = FALSE;
                break;
            default:
                return TRUE;
        }
        cp++;
    }

    // if the leading char was the start of one of our keywords
    // and we never hit a special character we can use the length
    // to check our
    if (is_possible_keyword) {
        switch (length) {
            case 3: // nan
                if (memcmp(start, "nan", 3) == 0) return TRUE;
                break;
            case 4: // true or null
                if (memcmp(start, "true", 4) == 0) return TRUE;
                if (memcmp(start, "null", 4) == 0) return TRUE;
                break;
            case 5: // false
                if (memcmp(start, "false", 5) == 0) return TRUE;
                break;
        }
    }

    return FALSE;
}


iERR _ion_symbol_local_copy_new_owner(void *context, void *dst, void *src, int32_t data_size)
{
    iENTER;
    ION_SYMBOL *symbol_dst = (ION_SYMBOL *)dst;
    ION_SYMBOL *symbol_src = (ION_SYMBOL *)src;

    if (data_size != sizeof(ION_SYMBOL)) FAILWITH(IERR_INVALID_ARG);
    ASSERT(dst);
    ASSERT(src);
    ASSERT(context);

    symbol_dst->sid     = symbol_src->sid;
    symbol_dst->psymtab = symbol_src->psymtab;
    IONCHECK(ion_string_copy_to_owner(context, &symbol_dst->value, &symbol_src->value));

    iRETURN;
}

iERR _ion_symbol_local_copy_same_owner(void *context, void *dst, void *src, int32_t data_size)
{
    iENTER;
    ION_SYMBOL *symbol_dst = (ION_SYMBOL *)dst;
    ION_SYMBOL *symbol_src = (ION_SYMBOL *)src;

    if (data_size != sizeof(ION_SYMBOL)) FAILWITH(IERR_INVALID_ARG);
    ASSERT(dst);
    ASSERT(src);

    symbol_dst->sid = symbol_src->sid;
    symbol_dst->psymtab = symbol_src->psymtab;
    ION_STRING_ASSIGN(&symbol_dst->value, &symbol_src->value);

    iRETURN;
}

iERR _ion_symbol_table_local_import_copy_new_owner(void *context, void *dst, void *src, int32_t data_size)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import_dst = (ION_SYMBOL_TABLE_IMPORT *)dst;
    ION_SYMBOL_TABLE_IMPORT *import_src = (ION_SYMBOL_TABLE_IMPORT *)src;

    if (data_size != sizeof(ION_SYMBOL_TABLE_IMPORT)) FAILWITH(IERR_INVALID_ARG);
    ASSERT(dst);
    ASSERT(src);
    ASSERT(context);

    memcpy(import_dst, import_src, data_size);
    IONCHECK(ion_string_copy_to_owner(context, &import_dst->name, &import_src->name));

    iRETURN;
}

iERR _ion_symbol_table_local_import_copy_same_owner(void *context, void *dst, void *src, int32_t data_size)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import_dst = (ION_SYMBOL_TABLE_IMPORT *)dst;
    ION_SYMBOL_TABLE_IMPORT *import_src = (ION_SYMBOL_TABLE_IMPORT *)src;

    if (data_size != sizeof(ION_SYMBOL_TABLE_IMPORT)) FAILWITH(IERR_INVALID_ARG);
    ASSERT(dst);
    ASSERT(src);

    memcpy(import_dst, import_src, data_size);
    ION_STRING_ASSIGN(&import_dst->name, &import_src->name);

    iRETURN;
}

iERR _ion_symbol_table_initialize_indices_helper(ION_SYMBOL_TABLE *symtab) 
{
    iENTER;
    int32_t                initial_size;
    ION_COLLECTION_CURSOR  symbol_cursor;
    ION_SYMBOL            *sym;
    ION_INDEX_OPTIONS      index_options = {
        NULL,                           // void          *_memory_owner;
        _ion_symbol_table_compare_fn,   // II_COMPARE_FN  _compare_fn;
        _ion_symbol_table_hash_fn,      // II_HASH_FN     _hash_fn;
        NULL,                           // void          *_fn_context;
        0,                              // int32_t        _initial_size;  /* number of actual keys */
        0                               // uint8_t        _density_target_percent; /* whole percent for table size increases 200% is usual (and default) */
    };

    ASSERT(symtab->is_locked == FALSE);

    if (INDEX_IS_ACTIVE(symtab)) SUCCEED(); // it's been done before

    initial_size = symtab->max_id + 1;  // size is 0, id's are 1 based
    if (initial_size < DEFAULT_SYMBOL_TABLE_SIZE) initial_size = DEFAULT_SYMBOL_TABLE_SIZE;
    
    index_options._initial_size = initial_size;
    index_options._memory_owner = symtab->owner;

    IONCHECK(_ion_index_initialize(&symtab->by_name, &index_options));

    // copy is false --- this time
    IONCHECK(_ion_index_grow_array((void **)&symtab->by_id, 0, initial_size, sizeof(symtab->by_id[0]), FALSE, symtab->owner));
    symtab->by_id_max = initial_size - 1; // size is 0 based, id's are 1 based

    if (!ION_COLLECTION_IS_EMPTY(&symtab->symbols)) {
        // if we have symbols we should index them
        ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, sym);
            if (!sym) break;
            symtab->by_id[sym->sid] = sym;
            IONCHECK(_ion_index_insert(&symtab->by_name, sym, sym));
        }
        ION_COLLECTION_CLOSE(symbol_cursor);
    }

    iRETURN;
}

int_fast8_t  _ion_symbol_table_compare_fn(void *key1, void *key2, void *context)
{
    int_fast8_t cmp;
    ION_SYMBOL *sym1 = (ION_SYMBOL *)key1;
    ION_SYMBOL *sym2 = (ION_SYMBOL *)key2;

    ASSERT(sym1);
    ASSERT(sym2);
#ifdef DEBUG
    ASSERT(!ION_STRING_IS_NULL(&sym1->value));
    ASSERT(!ION_STRING_IS_NULL(&sym2->value));
#endif

    // this compare is for the purposes of the hash table only !
    if (sym1 == sym2) {
        cmp = 0;
    }
    else if ((cmp = (sym1->value.length - sym2->value.length)) != 0) {
        cmp = (cmp > 0) ? 1 : -1; // normalize the value
    }
    else {
        cmp = memcmp(sym1->value.value, sym2->value.value, sym1->value.length);
    }
    return cmp;
}

int_fast32_t _ion_symbol_table_hash_fn(void *key, void *context)
{
    ION_SYMBOL  *sym = (ION_SYMBOL *)key;
    int_fast32_t hash;
    int len;
    BYTE *cb;
    
    ASSERT(sym);
    
#if 0
    hash = hashfn_mem(sym->value.value, sym->value.length);
    return hash;
#else
    hash = 0;
    len = sym->value.length;
    cb = sym->value.value;
    
    while (len)
    {
        hash = *cb + (hash << 6) + (hash << 16) - hash;
        ++cb;
        --len;
    }
    // the previous hash function was only returning 24 bits.
    return hash & 0x00FFFFFF;
#endif
}

iERR _ion_symbol_table_index_insert_helper(ION_SYMBOL_TABLE *symtab, ION_SYMBOL *sym) 
{
    iENTER;
    int32_t new_count, old_count;

    ASSERT(symtab->is_locked == FALSE);
    ASSERT(INDEX_IS_ACTIVE(symtab));

    if (sym->sid >= symtab->by_id_max) {
        // the +1 is because sid's are 1 based (so we're losing the 0th slot, and need 1 extra entry)
        old_count = (symtab->by_id_max + 1);
        new_count =  old_count * DEFAULT_SYMBOL_TABLE_SID_MULTIPLIER;
        if (new_count < DEFAULT_SYMBOL_TABLE_SIZE) new_count = DEFAULT_SYMBOL_TABLE_SIZE;
        IONCHECK(_ion_index_grow_array((void **)&symtab->by_id, old_count, new_count, sizeof(symtab->by_id[0]), TRUE, symtab->owner));
        symtab->by_id_max = new_count - 1; // adjust for 1 vs 0 based value (count is 0 based, id is 1 based)
    }
    symtab->by_id[sym->sid] = sym;

    IONCHECK(_ion_index_insert(&symtab->by_name, sym, sym));

    iRETURN;
}

iERR _ion_symbol_table_index_remove_helper(ION_SYMBOL_TABLE *symtab, ION_SYMBOL *sym) 
{
    iENTER;
    ION_SYMBOL *old_sym;

    ASSERT(symtab->is_locked == FALSE);
    ASSERT(INDEX_IS_ACTIVE(symtab));

    _ion_index_delete(&symtab->by_name, &sym->value, (void**)&old_sym);
    ASSERT( old_sym == sym );

    if (sym->sid > symtab->by_id_max) FAILWITH(IERR_INVALID_STATE);
    symtab->by_id[sym->sid] = NULL;
    SUCCEED();

    iRETURN;
}

ION_SYMBOL *_ion_symbol_table_index_find_by_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *str)
{
//  iENTER;
    ION_SYMBOL *found_sym;
    ION_SYMBOL  key_sym;

    ASSERT(symtab);
    ASSERT(!ION_STRING_IS_NULL(str));
    ASSERT(INDEX_IS_ACTIVE(symtab));

    // dummy up a symbol with the right key
    key_sym.value.length = str->length;
    key_sym.value.value = str->value;

    found_sym = _ion_index_find(&symtab->by_name, &key_sym);

    return found_sym;
}

ION_SYMBOL *_ion_symbol_table_index_find_by_sid_helper(ION_SYMBOL_TABLE *symtab, SID sid)
{
    ION_SYMBOL *found_sym;

    ASSERT(symtab);
    ASSERT(INDEX_IS_ACTIVE(symtab));

    if (sid <= UNKNOWN_SID || sid > symtab->by_id_max) {
        found_sym = NULL;
    }
    else {        
        found_sym = symtab->by_id[sid];
    }

    return found_sym;
}

const char *ion_symbol_table_type_to_str(ION_SYMBOL_TABLE_TYPE t)
{
    switch (t) {
    case ist_EMPTY:  return "ist_EMPTY";  // 0
    case ist_LOCAL:  return "ist_LOCAL";  // 1
    case ist_SHARED: return "ist_SHARED"; // 2
    case ist_SYSTEM: return "ist_SYSTEM"; // 3
    default:
        return _ion_hack_bad_value_to_str((intptr_t)t, "Bad ION_SYMBOL_TABLE_TYPE");
    }
}

iERR _ion_symbol_table_create_substitute(ION_SYMBOL_TABLE_IMPORT* import, ION_CATALOG* catalog, ION_SYMBOL_TABLE** result)
{
    iENTER;
    ION_SYMBOL_TABLE* symbol_table;
    _ion_symbol_table_open_helper(&symbol_table, NULL, catalog->system_symbol_table);

    symbol_table->version = import->version;
    ION_STRING_ASSIGN(&symbol_table->name, &import->name);
    symbol_table->system_symbol_table = catalog->system_symbol_table;
    symbol_table->max_id = import->max_id;

    *result = symbol_table;

    iRETURN;
}