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

/*
 * these function provide indexed collections used for symbol support
 * in Ion.c.  These follow the collection pattern and use the base
 * collection heavily.  And, like ion_index, the memory used 
 * for the nodes is allocated on the parent, which is passed in when 
 * the user initializes an index.
 *
 * each collections holds a high water mark free list of nodes that
 * were previously used but aren't currently being used
 *
 * index supports:
 *    iERR  initialize(ION_INDEX *idx, CMP_FN cmp, HASH_FN hash)
 *    BOOL  exists    (void *key)
 *    void *find      (void *key)
 *    BOOL  insert    (void *key, void *data)
 *    BOOL  upsert    (void *key, void *data)
 *    void  delete    (void *key)
 *    void  reset     ()
 *    void  release   ()
 *
 * unlike collection index expects the caller to own the key and
 * the data objects and index itself only maintains the additional
 * data to manage these functions.
 *
 * there are also cursor macros that mirror the collection cursors
 *
 * to define the index comparison behavior the user supplies a
 * compare function and a hash function
 */

#include "ion_internal.h"

static inline size_t _ion_index_hash_to_bucket_index(ION_INDEX* index, ION_INDEX_NODE* node) {
    return ((size_t) node->_hash) % index->_bucket_count;
}

// local functions forward declarations
iERR  _ion_index_set_options_helper(ION_INDEX *index, ION_INDEX_OPTIONS *p_options);

ION_INDEX_NODE **_ion_index_get_bucket_helper(ION_INDEX *index, ION_INDEX_NODE *key);
BOOL             _ion_index_scan_bucket_helper(ION_INDEX *index, ION_INDEX_NODE *head, ION_INDEX_NODE *node, ION_INDEX_NODE **p_found, ION_INDEX_NODE **p_prev);
iERR             _ion_index_insert_helper(ION_INDEX *index, void *key, void *data, ION_INDEX_NODE **p_new_node);


// actual index functions

iERR  _ion_index_initialize(ION_INDEX *index, ION_INDEX_OPTIONS *p_options)
{
    iENTER;

    if (!index) FAILWITH(IERR_INVALID_ARG);

    memset(index, 0, sizeof(ION_INDEX));

    if (p_options) {
        IONCHECK(_ion_index_set_options_helper(index, p_options));
    }

    _ion_collection_initialize(index->_memory_owner, &index->_nodes, sizeof(ION_INDEX_NODE));

    if (p_options && p_options->_initial_size) {
        IONCHECK(_ion_index_make_room(index, p_options->_initial_size));
    }

    iRETURN;
}

iERR _ion_index_make_room(ION_INDEX *index, int32_t expected_new)
{
    iENTER;
    int32_t          new_key_threshold, new_bucket_count, old_bucket_count;
    ION_INDEX_NODE **new_table, **old_table;
    ION_INDEX_NODE  *node, *next;
    int32_t          ii, in_use;
    size_t           jj;

    if (!index) FAILWITH(IERR_INVALID_ARG);

    if (expected_new  + index->_key_count < index->_grow_at) {
        SUCCEED();
    }

    // key count <= bucket * (_grow_density_percent/100)
    // bucket count = ( key count * 100 ) / _grow_density_percent
    new_key_threshold  = index->_key_count + expected_new;
    new_key_threshold *= 128; // _grow_density_percent really is whole percent
    new_key_threshold /= index->_density_target_percent_128x;

    // turn this into the desired bucket size
    new_bucket_count = index->_grow_at;
    if (new_bucket_count < II_DEFAULT_MINIMUM) new_bucket_count = II_DEFAULT_MINIMUM;
    while(new_bucket_count < new_key_threshold) {
        II_GROW(new_bucket_count);
        ASSERT(new_bucket_count);
    }

    // grow the bucket
    old_table = index->_bucket_table;
    IONCHECK(_ion_index_grow_array(
                 (void**)&index->_bucket_table
                 ,index->_bucket_count
                 ,new_bucket_count
                 ,sizeof(ION_INDEX_NODE)
                 ,FALSE
                 ,index->_memory_owner
    ));
    
    // now resplit the buckets
    new_table = index->_bucket_table;
    old_bucket_count = index->_bucket_count;
    index->_bucket_count = new_bucket_count;

    in_use = 0;
    for (ii=0; ii<old_bucket_count; ii++) {
        node = old_table[ii];
        while(node) {
            next = node->_next;
            jj = _ion_index_hash_to_bucket_index( index, node );
            if (new_table[jj] == NULL) {
                in_use++;
            }
            node->_next = new_table[jj];
            new_table[jj] = node;
            node = next;
        }
    }
    index->_bucket_in_use_count = in_use;
    index->_bucket_count = new_bucket_count;

    // next threshold
    II_GROW(new_bucket_count);
    index->_grow_at = new_bucket_count;

    iRETURN;
}

BOOL  _ion_index_exists(ION_INDEX *index, void *key)
{
    BOOL exists;
    ION_INDEX_NODE   node;
    ION_INDEX_NODE  *entry, *prev;
    ION_INDEX_NODE **bucket;

    memset(&node, 0, sizeof(node));
    node._key = key;
    bucket = _ion_index_get_bucket_helper(index, &node);
    if (!bucket || !(*bucket)) return FALSE;
    
    exists = _ion_index_scan_bucket_helper(index, *bucket, &node, &entry, &prev);
    return exists;
}

void *_ion_index_find(ION_INDEX *index, void *key)
{
    BOOL exists;
    ION_INDEX_NODE   node;
    ION_INDEX_NODE  *entry, *prev;
    ION_INDEX_NODE **bucket;

    memset(&node, 0, sizeof(node));
    node._key = key;
    bucket = _ion_index_get_bucket_helper(index, &node);
    if (!bucket || !(*bucket)) return NULL;

    exists = _ion_index_scan_bucket_helper(index, *bucket, &node, &entry, &prev);
    return exists ? entry->_data : NULL;
}

iERR _ion_index_insert(ION_INDEX *index, void *key, void *data)
{
    iENTER;
    ION_INDEX_NODE *entry;

    err = _ion_index_insert_helper(index, key, data, &entry);
    if (err == IERR_KEY_ALREADY_EXISTS) DONTFAILWITH(err);
    IONCHECK(err);

    iRETURN;
}

iERR _ion_index_upsert(ION_INDEX *index, void *key, void *data)
{
    iENTER;
    ION_INDEX_NODE *entry;

    err = _ion_index_insert_helper(index, key, data, &entry);
    if (err == IERR_KEY_ALREADY_EXISTS) {
        entry->_data = data;
        SUCCEED(); // which will clear the already exists "error"
    }
    IONCHECK(err);

    iRETURN;
}

void _ion_index_delete(ION_INDEX *index, void *key, void **p_data)
{
//  iENTER;
    ION_INDEX_NODE   node;
    ION_INDEX_NODE  *entry, *prev;
    ION_INDEX_NODE **bucket;

    if (index->_key_count < 1) return;

    node._key = key;
    bucket = _ion_index_get_bucket_helper(index, &node);

    ASSERT(bucket); // there are some keys, there should be bucket table at least
    if (!_ion_index_scan_bucket_helper(index, *bucket, &node, &entry, &prev)) {
        *p_data = NULL;
        return;
    }
    ASSERT(entry);
    *p_data = entry->_data; // while we still have it around

    // remove this from the list
    if (prev) {
        prev->_next = entry->_next;
        entry->_next = NULL; // just because this might get reused
    }
    else {
        *bucket = entry->_next;
        if (!entry->_next) index->_bucket_in_use_count--;
    }
    index->_key_count--;

    _ion_collection_remove(&index->_nodes, entry);

    return;
}

void _ion_index_reset(ION_INDEX *index)
{
    int32_t table_size;

    ASSERT(index);

    if (index->_key_count < 1) return;

    _ion_collection_reset(&index->_nodes);
    table_size = index->_bucket_count * sizeof(ION_INDEX_NODE *);
    if (table_size > 0) {
        memset(index->_bucket_table, 0, table_size);
    }
    index->_key_count = 0;
    index->_bucket_in_use_count = 0;
    return;
}


// really a local helper function, but it'll probably be useful
// for the sid to symbol array as well
iERR _ion_index_grow_array(void **p_array, int32_t old_count, int32_t new_count, int32_t entry_size, BOOL with_copy, void *owner)
{
    iENTER;
    int32_t in_use, new_size;
    void *new_array;
    void *old_array = *p_array;

    new_size = new_count * entry_size;
    new_array = ion_alloc_with_owner(owner, new_size);
    if (!new_array) FAILWITH(IERR_NO_MEMORY);

    in_use = 0;
    if (with_copy && old_array && old_count > 0) {
        in_use = old_count * entry_size;
        in_use /= sizeof(uint8_t);
        memcpy((uint8_t *)new_array, (uint8_t *)old_array, in_use);
    }

    // zero out anything we didn't have to copy
    memset(((uint8_t *)new_array) + in_use, 0, new_size - in_use);

    *p_array = new_array;
    iRETURN;
}

 
// local function impl's

iERR _ion_index_set_options_helper(ION_INDEX *index, ION_INDEX_OPTIONS *p_options)
{
    iENTER;

    if (!p_options)              FAILWITH(IERR_INVALID_ARG);
    if (!p_options->_compare_fn) FAILWITH(IERR_INVALID_ARG);
    if (!p_options->_hash_fn)    FAILWITH(IERR_INVALID_ARG);

    index->_memory_owner = p_options->_memory_owner;
    index->_compare_fn = p_options->_compare_fn;
    index->_hash_fn = p_options->_hash_fn;
    index->_fn_context = p_options->_fn_context;

    if (p_options->_density_target_percent) {
        index->_density_target_percent_128x  = p_options->_density_target_percent * 100;
        index->_density_target_percent_128x /= 128;
    }
    else {
        index->_density_target_percent_128x = II_DEFAULT_128X_PERCENT;
    }

    iRETURN;
}

// gets the initial bucket pointer and fills in the hash
ION_INDEX_NODE **_ion_index_get_bucket_helper(ION_INDEX *index, ION_INDEX_NODE *node)
{
    node->_hash = (*index->_hash_fn)(node->_key, index->_fn_context);

    if (!index->_bucket_count) return NULL;
    size_t bucket_index = _ion_index_hash_to_bucket_index( index, node );

    return index->_bucket_table + bucket_index;
}

BOOL _ion_index_scan_bucket_helper(
     ION_INDEX *index
    ,ION_INDEX_NODE *head
    ,ION_INDEX_NODE *node
    ,ION_INDEX_NODE **p_found
    ,ION_INDEX_NODE **p_prev
)
{
    int_fast8_t cmp;
    ION_INDEX_NODE *entry = NULL;
    ION_INDEX_NODE *prev = NULL;


    ASSERT(p_found);
    ASSERT(p_prev);

    if (!head) goto return_values;  

    for (entry = head; entry; prev = entry, entry = entry->_next) {
        if (entry->_hash != node->_hash) continue;
        cmp = (*index->_compare_fn)(entry->_key, node->_key, index->_fn_context);
        if (cmp == 0) break;
    }
    goto return_values;

return_values:
    *p_found = entry;
    *p_prev = prev;
    return (entry != NULL);
}

iERR _ion_index_insert_helper(ION_INDEX *index, void *key, void *data, ION_INDEX_NODE **p_new_node)
{
    iENTER;
    ION_INDEX_NODE   node;
    ION_INDEX_NODE  *entry, *found, *prev;
    ION_INDEX_NODE **bucket;

    // we pre-grow the bucket table so we can't avoid
    // the "no table" edge cases - including the bucket table moving
    if (index->_key_count + 1 >= index->_grow_at) {
        // if this is an EMTPY index we make room for default
        // otherwise we just need room for the 1 key
        IONCHECK(_ion_index_make_room(index, index->_bucket_count ? 1 : II_DEFAULT_MINIMUM));
    }

    node._key = key;
    bucket = _ion_index_get_bucket_helper(index, &node);

    ASSERT(bucket); // since we forced buckets to exist
    if (_ion_index_scan_bucket_helper(index, *bucket, &node, &found, &prev)) {
        *p_new_node = found;
        DONTFAILWITH(IERR_KEY_ALREADY_EXISTS);
    }

    // it's not there so we can add this key to the bucket
    entry = (ION_INDEX_NODE *)_ion_collection_append(&index->_nodes);
    if (!entry) FAILWITH(IERR_NO_MEMORY);
    entry->_hash = node._hash;
    entry->_key  = key;
    entry->_data = data;
    if (!*bucket) {
        index->_bucket_in_use_count++;
    }
    entry->_next = *bucket;
    *bucket = entry;
    index->_key_count++;
    *p_new_node = entry;

    iRETURN;
}
