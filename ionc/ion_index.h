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
 * this helps define indexed collections used for symbol support
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

#ifndef ION_INDEX_H_
#define ION_INDEX_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef int_fast8_t  (*II_COMPARE_FN)(void *key1, void *key2, void *context);
typedef int_fast32_t (*II_HASH_FN)   (void *key, void *context);

#define II_DEFAULT_128X_PERCENT 104 /* 80% pre-converted to "base 128 percent" */
#define II_DEFAULT_MINIMUM       16 /* net desired buckets */

typedef struct _ion_index_options ION_INDEX_OPTIONS;
struct _ion_index_options
{
    void          *_memory_owner;
    II_COMPARE_FN  _compare_fn;
    II_HASH_FN     _hash_fn;
    void          *_fn_context;
    int32_t        _initial_size;  /* number of actual keys */
    uint8_t        _density_target_percent; /* whole percent for table size increases 200% is usual (and default) */

};

// the node allocation scheme depends on this
// layout - currently that there are only 2 members
// so it uses the size of the ptr as the base to 
// allocate
typedef struct _ion_index_node ION_INDEX_NODE;
struct _ion_index_node
{
    int_fast32_t    _hash;
    void           *_key;
    void           *_data;
    ION_INDEX_NODE *_next;
};

typedef struct _ion_index ION_INDEX;
struct _ion_index
{
    void           *_memory_owner;
    II_COMPARE_FN   _compare_fn;
    II_HASH_FN      _hash_fn;
    void           *_fn_context;
    uint8_t         _density_target_percent_128x;

    int32_t         _key_count;
    int32_t         _bucket_count;
    int32_t         _bucket_in_use_count;  // watch for bad distribution
    int32_t         _grow_at;
    ION_INDEX_NODE**_bucket_table;
    ION_COLLECTION  _nodes;

};

GLOBAL int32_t g_ion_index_multiplier_x128 INITTO(2*128);

#define II_GROW(x)                          \
    do {                                    \
        (x) *= g_ion_index_multiplier_x128; \
        (x) /= 128;                         \
   } while (0)

// BOOL ion_index_is_empty(ION_INDEX *index)
#define ION_INDEX_IS_EMPTY(index)       (ION_INDEX_SIZE(index) == 0)

// SIZE count = ion_index_size(ION_INDEX *index)
#define ION_INDEX_SIZE(index)           ((index)->_count)

typedef struct _ion_index_node *ION_INDEX_CURSOR;

// ION_INDEX_CURSOR pcursor = ion_index_open(ION_INDEX *collection)
#define ION_INDEX_OPEN(index, pcursor)  ION_COLLECTION_OPEN(&(index)->_nodes, pcursor)

// void *pbuf = ion_index_next(ION_INDEX_CURSOR *pcursor)
#define ION_INDEX_NEXT(index, pbuf)     ION_COLECTION_NEXT( &(index)->_nodes, pbuf )

// ION_INDEX_CURSOR *pcursor = ion_index_close();
#define ION_INDEX_CLOSE(pcursor)        ION_COLLECTION_CLOSE( pcursor )

iERR  _ion_index_initialize(ION_INDEX *index, ION_INDEX_OPTIONS *p_options);
iERR  _ion_index_make_room(ION_INDEX *index, int32_t expected_new);

BOOL  _ion_index_exists    (ION_INDEX *index, void *key);
void *_ion_index_find      (ION_INDEX *index, void *key);
iERR  _ion_index_insert    (ION_INDEX *index, void *key, void *data);
iERR  _ion_index_upsert    (ION_INDEX *index, void *key, void *data);
void  _ion_index_delete    (ION_INDEX *index, void *key, void **p_data);
void  _ion_index_reset     (ION_INDEX *index);
void  _ion_index_release   (ION_INDEX *index);

iERR _ion_index_grow_array(void **p_array, int32_t old_count, int32_t new_count, int32_t entry_size, BOOL with_copy, void *owner);

#ifdef __cplusplus
}
#endif

#endif

