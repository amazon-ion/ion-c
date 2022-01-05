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

#ifndef ION_SYMBOL_TABLE_IMPL_H_
#define ION_SYMBOL_TABLE_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

// strings are commonly used pointer length values
// the referenced data is (should be) immutable
// and is often shared or owned by others
// the character encoding is utf-8 and both comparisons
// and collation is only done as memcmp

struct _ion_symbol_table
{
    void               *owner;          // this may be a reader, writer, catalog or itself
    BOOL                is_locked;
    BOOL                has_local_symbols;
    ION_STRING          name;
    int32_t             version;
    int32_t             max_id;         // the max SID of this symbol tables symbols, including shared symbols.
    int32_t             min_local_id;   // the lowest local SID. Only valid if has_local_symbols is TRUE. by_id[0] holds this symbol.
    int32_t             flushed_max_id; // the max SID already serialized. If symbols are appended, only the ones after this need to be serialized.
    ION_COLLECTION      import_list;    // collection of ION_SYMBOL_TABLE_IMPORT
    ION_COLLECTION      symbols;        // collection of ION_SYMBOL
    ION_SYMBOL_TABLE   *system_symbol_table;

    int32_t             by_id_max;      // current size of by_id, which holds the local symbols, but NOT necessarily the number of declared local symbols.
    ION_SYMBOL        **by_id;          // the local symbols. Accessing shared symbols requires delegate lookups to the imports.
    ION_INDEX           by_name;        // the local symbols (by name).

};

struct _ion_symbol_table_import_descriptor
{
    ION_STRING name;
    int32_t    version;
    int32_t    max_id;

};

struct _ion_symbol_table_import
{
    ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR descriptor;  // Description of the import.
    ION_SYMBOL_TABLE *shared_symbol_table;          // The resolved import. NULL if no match was found in the catalog.
};

// "locals" in ion_symbol_table.c
iERR _ion_symbol_table_local_find_by_name(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid, ION_SYMBOL **p_sym);
// Returns TRUE if and only if the given SID matches a known IVM SID.
BOOL _ion_symbol_table_sid_is_IVM(SID sid);
// Attempts to parse an IVM. Returns TRUE if the given string is an IVM, and FALSE otherwise.
BOOL _ion_symbol_table_parse_version_marker(ION_STRING *version_marker, int *major_version, int *minor_version);
BOOL _ion_symbol_needs_quotes(ION_STRING *p_str, BOOL symbol_identifiers_need_quotes);

// internal (pointer based helpers) functions for symbol tables (in ion_symbol_table.c)
iERR _ion_symbol_table_open_helper(ION_SYMBOL_TABLE **p_psymtab, hOWNER owner, ION_SYMBOL_TABLE *psystem);
iERR _ion_symbol_table_clone_with_owner_helper(ION_SYMBOL_TABLE **p_pclone, ION_SYMBOL_TABLE *orig, hOWNER owner, ION_SYMBOL_TABLE *system_symtab);
iERR _ion_symbol_table_clone_with_owner_and_system_table(hSYMTAB hsymtab, hSYMTAB *p_hclone, hOWNER owner, hSYMTAB hsystem);
iERR _ion_symbol_table_get_system_symbol_helper(ION_SYMBOL_TABLE **pp_system_table, int32_t version);
//iERR _ion_symbol_table_load_import_list_helper(ION_READER *preader, hOWNER owner, ION_SYMBOL_TABLE_IMPORT **p_head);
iERR _ion_symbol_table_load_symbol_list_helper(ION_READER *preader, hOWNER owner, ION_SYMBOL **p_listhead);
iERR _ion_symbol_table_load_helper(ION_READER *preader, hOWNER owner, ION_SYMBOL_TABLE *system_symtab, ION_SYMBOL_TABLE **p_psymtab);
iERR _ion_symbol_table_unload_helper(ION_SYMBOL_TABLE *symtab, ION_WRITER *pwriter);
iERR _ion_symbol_table_lock_helper(ION_SYMBOL_TABLE *symtab);
iERR _ion_symbol_table_is_locked_helper(ION_SYMBOL_TABLE *symtab, BOOL *p_is_locked);
iERR _ion_symbol_table_get_type_helper(ION_SYMBOL_TABLE *symtab, ION_SYMBOL_TABLE_TYPE *p_type);
iERR _ion_symbol_table_get_owner(hSYMTAB hsymtab, hOWNER *howner);
iERR _ion_symbol_table_get_system_symbol_table(hSYMTAB hsymtab, hSYMTAB *p_hsymtab_system);
iERR _ion_symbol_table_get_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *p_name);
iERR _ion_symbol_table_get_version_helper(ION_SYMBOL_TABLE *symtab, int32_t *p_version);
iERR _ion_symbol_table_get_max_sid_helper(ION_SYMBOL_TABLE *symtab, SID *p_max_id);
iERR _ion_symbol_table_get_flushed_max_sid_helper(ION_SYMBOL_TABLE *symtab, SID *p_flushed_max_id);
iERR _ion_symbol_table_set_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name);
iERR _ion_symbol_table_set_version_helper(ION_SYMBOL_TABLE *symtab, int32_t version);
iERR _ion_symbol_table_set_max_sid_helper(ION_SYMBOL_TABLE *symtab, SID max_id);
iERR _ion_symbol_table_set_flushed_max_sid_helper(ION_SYMBOL_TABLE *symtab, SID flushed_max_id);
iERR _ion_symbol_table_get_imports_helper(ION_SYMBOL_TABLE *symtab, ION_COLLECTION **p_imports);
iERR _ion_symbol_table_get_symbols_helper(ION_SYMBOL_TABLE *symtab, ION_COLLECTION **p_symbols);
iERR _ion_symbol_table_parse_possible_symbol_identifier(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid, ION_SYMBOL **p_sym, BOOL *p_is_symbol_identifier);
iERR _ion_symbol_table_find_by_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid, ION_SYMBOL **p_sym, BOOL symbol_identifiers_as_sids);
iERR _ion_symbol_table_find_by_sid_helper(ION_SYMBOL_TABLE *symtab, SID sid, ION_STRING **p_name);
iERR _ion_symbol_table_find_symbol_by_sid_helper(ION_SYMBOL_TABLE *symtab, SID sid, ION_SYMBOL **p_sym);
iERR _ion_symbol_table_get_unknown_symbol_name(ION_SYMBOL_TABLE *symtab, SID sid, ION_STRING **p_name);
iERR _ion_symbol_table_find_by_sid_force(ION_SYMBOL_TABLE *symtab, SID sid, ION_STRING **p_name, BOOL *p_is_symbol_identifier);
void _ion_symbol_table_allocate_symbol_unknown_text(hOWNER owner, SID sid, ION_SYMBOL **p_symbol);
iERR _ion_symbol_table_is_symbol_known_helper(ION_SYMBOL_TABLE *symtab, SID sid, BOOL *p_is_known);
iERR _ion_symbol_table_add_symbol_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID *p_sid);
iERR _ion_symbol_table_close_helper(ION_SYMBOL_TABLE *symtab);

#define DEFAULT_SYMBOL_TABLE_SID_MULTIPLIER  2
#define DEFAULT_INDEX_BUILD_THRESHOLD       15
#define DEFAULT_SYMBOL_TABLE_SIZE           15

// local function forward reference declarations
iERR _ion_symbol_table_local_make_system_symbol_table_helper(int32_t version);

// The text reader doesn't automatically provide SIDs for known symbols. This forces a by-name lookup to the system
// symbol table in those cases.
iERR _ion_symbol_table_get_field_sid_force(ION_READER *preader, SID *fld_sid);

iERR _ion_symbol_table_local_load_import_list   (ION_READER *preader, hOWNER owner, ION_COLLECTION *pimport_list);
iERR _ion_symbol_table_local_load_symbol_struct (ION_READER *preader, hOWNER owner, ION_COLLECTION *psymbol_list);
iERR _ion_symbol_table_local_load_symbol_list   (ION_READER *preader, hOWNER owner, ION_COLLECTION *psymbol_list);

iERR _ion_symbol_table_import_compare(ION_SYMBOL_TABLE_IMPORT *lhs, ION_SYMBOL_TABLE_IMPORT *rhs, BOOL *is_equal);
iERR _ion_symbol_table_import_compare_fn(void *lhs, void *rhs, BOOL *is_equal);
iERR _ion_symbol_table_import_symbol_table_helper(ION_SYMBOL_TABLE *symtab, ION_SYMBOL_TABLE *import_symtab, ION_STRING *import_name, int32_t import_version, int32_t import_max_id);
iERR _ion_symbol_table_local_incorporate_symbols(ION_SYMBOL_TABLE *symtab, ION_SYMBOL_TABLE *shared, int32_t import_max_id);
iERR _ion_symbol_table_local_add_symbol_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *name, SID sid, ION_SYMBOL **p_psym);

iERR _ion_symbol_local_copy_same_owner(void *context, void *dst, void *src, int32_t data_size);
iERR _ion_symbol_local_copy_new_owner(void *context, void *dst, void *src, int32_t data_size);
iERR _ion_symbol_table_local_import_copy_new_owner(void *context, void *dst, void *src, int32_t data_size);
iERR _ion_symbol_table_local_import_copy_same_owner(void *context, void *dst, void *src, int32_t data_size);

#define INDEX_IS_ACTIVE(symtab) ((symtab)->by_id_max > 0)
iERR         _ion_symbol_table_initialize_indices_helper(ION_SYMBOL_TABLE *symtab);
int_fast8_t  _ion_symbol_table_compare_fn               (void *key1, void *key2, void *context);
int_fast32_t _ion_symbol_table_hash_fn                  (void *key, void *context);
iERR         _ion_symbol_table_index_insert_helper      (ION_SYMBOL_TABLE *symtab, ION_SYMBOL *sym);
iERR         _ion_symbol_table_index_remove_helper      (ION_SYMBOL_TABLE *symtab, ION_SYMBOL *sym);
ION_SYMBOL  *_ion_symbol_table_index_find_by_name_helper(ION_SYMBOL_TABLE *symtab, ION_STRING *str);
ION_SYMBOL  *_ion_symbol_table_index_find_by_sid_helper (ION_SYMBOL_TABLE *symtab, SID sid);

#ifdef __cplusplus
}
#endif

#endif /* ION_SYMBOL_TABLE_IMPL_H_ */
