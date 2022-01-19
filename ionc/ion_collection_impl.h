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

#ifndef ION_COLLECTION_IMPL_H_
#define ION_COLLECTION_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef iERR (*ION_COPY_FN)(void *context, void *dst, void *src, int32_t data_size);
typedef iERR (*ION_COMPARE_FN)(void *lhs, void *rhs, BOOL *is_equal);

void  _ion_collection_initialize(void *allocation_parent, ION_COLLECTION *collection, int32_t data_length);
void *_ion_collection_push      (ION_COLLECTION *collection);
void *_ion_collection_append    (ION_COLLECTION *collection);
void  _ion_collection_pop_head  (ION_COLLECTION *collection);
void  _ion_collection_pop_tail  (ION_COLLECTION *collection);
void  _ion_collection_remove    (ION_COLLECTION *collection, void *p_entry);
void *_ion_collection_head      (ION_COLLECTION *collection);
void *_ion_collection_tail      (ION_COLLECTION *collection);
void  _ion_collection_reset     (ION_COLLECTION *collection);  // resets the collection contents, preserves the freelist
void  _ion_collection_release   (ION_COLLECTION *collection);  // frees the pages back to the owner, back to new state - valid only on empty collections
iERR  _ion_collection_copy      (ION_COLLECTION *dst, ION_COLLECTION *src, ION_COPY_FN copy_contents_fn, void *copy_fn_context);
iERR  _ion_collection_compare   (ION_COLLECTION *lhs, ION_COLLECTION *rhs, ION_COMPARE_FN compare_contents_fn, BOOL *is_equal);
iERR  _ion_collection_contains  (ION_COLLECTION *collection, void *element, ION_COMPARE_FN compare_contents_fn, BOOL *contains);

#ifdef __cplusplus
}
#endif

#ifdef HANDY_CODE_TO_COPY
    ION_COLLECTION_CURSOR    symbol_cursor;
    if (!ION_COLLECTION_IS_EMPTY(&symtab->symbols)) {
        ION_COLLECTION_OPEN(&symtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, sym);
            if (!sym) break;

        }
        ION_COLLECTION_CLOSE(symbol_cursor);
    }

    ION_COLLECTION_CURSOR    import_cursor;
    if (!ION_COLLECTION_IS_EMPTY(&symtab->import_list)) {
        ION_COLLECTION_OPEN(&symtab->import_list, import_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(import_cursor, import);
            if (!import) break;

        }
        ION_COLLECTION_CLOSE(import_cursor);
    }

    ION_COLLECTION_CURSOR    symtab_cursor;
    if (!ION_COLLECTION_IS_EMPTY(&pcatalog->table_list)) {
        ION_COLLECTION_OPEN(&pcatalog->table_list, symtab_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symtab_cursor, symtab);
            if (!symtab) break;

        }
        ION_COLLECTION_CLOSE(symtab_cursor);
    }

#endif
#endif /* ION_COLLECTION_IMPL_H_ */
