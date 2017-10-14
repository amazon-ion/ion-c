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
 * the collections used by the parser are linked lists which are
 * managed by the collection header.
 * the memory used for the nodes is allocated on the parent, which
 * is passed in when the user initializes the collection
 *
 * the nodes in the list have a user sized data buffer, which is
 * expected to a small struct (like ion string) or a scaler (like an
 * int or pointer).
 *
 * the push, pop, and append routines return the address of this
 * data buffer - for push and append it is the buffer of the new node
 * for pop it is the buffer of the released node - which is still
 * allocated and is, therefore, good UNTIL ANOTHER push or append
 * or copy is executed against the containing collection.
 *
 * each collections holds a high water mark free list of nodes that
 * were previously used but aren't currently being used
 */


#include "ion_internal.h"

// forward references for the internal helper functions
ION_COLLECTION_NODE *_ion_collection_alloc_node_helper (ION_COLLECTION *collection);
ION_COLLECTION_NODE *_ion_collection_push_node_helper  (ION_COLLECTION *collection);
ION_COLLECTION_NODE *_ion_collection_append_node_helper(ION_COLLECTION *collection);
void                 _ion_collection_remove_node_helper(ION_COLLECTION *collection, ION_COLLECTION_NODE *node);
void                 _ion_collection_free_node_helper  (ION_COLLECTION *collection, ION_COLLECTION_NODE *node);
void                 _ion_collection_clear_node        (ION_COLLECTION *collection, ION_COLLECTION_NODE *node);
void                 _ion_collection_clear_data        (ION_COLLECTION *collection, void *pdata);


// public functions
void _ion_collection_initialize(hOWNER allocation_owner, ION_COLLECTION *collection, SIZE data_length)
{
    ASSERT( allocation_owner != NULL );
    ASSERT( collection != NULL );
    ASSERT( data_length > 0 );

    memset( collection, 0, sizeof( ION_COLLECTION ) );
    collection->_owner = allocation_owner;
    collection->_node_size = IPCN_OVERHEAD_SIZE + data_length; // WARNING: this has to be in sync with the ION_COLLECTION_NODE definition
}

void *_ion_collection_push(ION_COLLECTION *collection)
{
//  iENTER;
    ION_COLLECTION_NODE *node;
    void                *data;

    node = _ion_collection_push_node_helper(collection);
    data = (node == NULL) ? NULL :IPCN_pNODE_TO_pDATA(node);

    return data;
}

void *_ion_collection_append(ION_COLLECTION *collection)
{
    ION_COLLECTION_NODE *node;
    void                *data;

    node = _ion_collection_append_node_helper( collection );
    data = (node == NULL) ? NULL :IPCN_pNODE_TO_pDATA(node);

    return data;
}

#ifdef MEM_DEBUG
void _ion_collection_clear_node(ION_COLLECTION *collection, ION_COLLECTION_NODE *node)
{
    memset(node, 0xfa, collection->_node_size);
}
void _ion_collection_clear_data(ION_COLLECTION *collection, void *pdata)
{
    ION_COLLECTION_NODE *node;
    node = IPCN_pDATA_TO_pNODE(pdata);
    _ion_collection_clear_node(collection, node);
}
#endif

void _ion_collection_pop_head(ION_COLLECTION *collection)
{
    ION_COLLECTION_NODE *node;

    node = collection->_head;
    if (node) {
        _ion_collection_remove_node_helper(collection, node);
    }
    return;;
}

void _ion_collection_pop_tail(ION_COLLECTION *collection)
{
    ION_COLLECTION_NODE *node;

    node = collection->_tail;
    if (node) {
        _ion_collection_remove_node_helper(collection, node);
    }

    return;
}

void _ion_collection_remove(ION_COLLECTION *collection, void *p_entry)
{
    ION_COLLECTION_NODE *node;

    ASSERT(collection);
    ASSERT(p_entry);

    node = IPCN_pDATA_TO_pNODE(p_entry);
    _ion_collection_remove_node_helper(collection, node);

    return;
}

void *_ion_collection_head(ION_COLLECTION *collection)
{
    ION_COLLECTION_NODE *node;
    void                *pdata;

    node = collection->_head;
    if (node == NULL) {
        pdata = NULL;
    }
    else {
        pdata = IPCN_pNODE_TO_pDATA(node);
    }

    return pdata;
}

void *_ion_collection_tail(ION_COLLECTION *collection)
{
    ION_COLLECTION_NODE *node;
    void                *pdata;

    node = collection->_tail;
    if (node == NULL) {
        pdata = NULL;
    }
    else {
        pdata = IPCN_pNODE_TO_pDATA(node);
    }

    return pdata;
}

void _ion_collection_reset( ION_COLLECTION *collection )
{
    ASSERT(collection != NULL);

    // short circuit empty list - mostly to avoid null references
    if (collection->_head) {
        // move the allocated nodes to the free list
        collection->_tail->_next = collection->_freelist ? collection->_freelist->_next : NULL;
        collection->_freelist = collection->_head;
        collection->_tail = NULL;
        collection->_head = NULL;
        collection->_count = 0;
    }
 
    return;
}

// the copy is done by first freeing all the nodes in the dst
// (which is prefix free list with node in in-use list
// the for all the nodes in the src collection
//   allocate a node in the dst
//   copy the data section
//   prefix it to the list
// then make that list the head and tail
iERR _ion_collection_copy( ION_COLLECTION *dst, ION_COLLECTION *src, ION_COPY_FN copy_contents_fn, void *copy_fn_context)
{
    iENTER;

    int data_size;
    ION_COLLECTION_NODE *srcnode, *dstnode;

    if (dst == NULL) FAILWITH(IERR_INVALID_ARG);
    if (src == NULL) FAILWITH(IERR_INVALID_ARG);
    if (dst->_node_size != src->_node_size) FAILWITH(IERR_INVALID_STATE);

    // move the allocated nodes to the free list
    _ion_collection_reset( dst );

    data_size = dst->_node_size - IPCN_OVERHEAD_SIZE;

    for (srcnode = src->_head; srcnode; srcnode = srcnode->_next) {
        dstnode = _ion_collection_append_node_helper( dst );
        if (!copy_contents_fn) {
            memcpy( IPCN_pNODE_TO_pDATA(dstnode), IPCN_pNODE_TO_pDATA(srcnode), data_size );
        }
        else {
            IONCHECK((*copy_contents_fn)(copy_fn_context, IPCN_pNODE_TO_pDATA(dstnode), IPCN_pNODE_TO_pDATA(srcnode), data_size));
        }
    }

    ASSERT(dst->_count == src->_count);

    iRETURN;
}

iERR _ion_collection_compare(ION_COLLECTION *lhs, ION_COLLECTION *rhs, ION_COMPARE_FN compare_contents_fn, BOOL *is_equal)
{
    iENTER;
    ION_COLLECTION_NODE *lhs_node, *rhs_node;

    ASSERT(is_equal != NULL);
    ASSERT(compare_contents_fn != NULL);

    if (lhs == NULL ^ rhs == NULL) {
        *is_equal = FALSE;
        SUCCEED();
    }
    if (lhs == NULL) {
        ASSERT(rhs == NULL);
        *is_equal = TRUE;
        SUCCEED();
    }
    if (lhs->_count != rhs->_count) {
        *is_equal = FALSE;
        SUCCEED();
    }
    if (lhs->_node_size != rhs->_node_size) {
        *is_equal = FALSE;
        SUCCEED();
    }

    lhs_node = lhs->_head;
    rhs_node = rhs->_head;

    while (lhs_node != NULL) {
        ASSERT(rhs_node != NULL);
        IONCHECK(compare_contents_fn(IPCN_pNODE_TO_pDATA(lhs_node), IPCN_pNODE_TO_pDATA(rhs_node), is_equal));
        if (!(*is_equal)) {
            SUCCEED();
        }
        lhs_node = lhs_node->_next;
        rhs_node = rhs_node->_next;
    }

    *is_equal = TRUE;

    iRETURN;
}

iERR _ion_collection_contains(ION_COLLECTION *collection, void *element, ION_COMPARE_FN compare_contents_fn, BOOL *contains)
{
    iENTER;
    ION_COLLECTION_NODE *node;
    BOOL is_equal;

    ASSERT(contains);
    ASSERT(compare_contents_fn != NULL);
    ASSERT(collection);

    if (!element || collection->_count == 0) {
        *contains = FALSE;
        SUCCEED();
    }

    node = collection->_head;
    while (node != NULL) {
        IONCHECK(compare_contents_fn(IPCN_pNODE_TO_pDATA(node), element, &is_equal));
        if (is_equal) {
            *contains = TRUE;
            SUCCEED();
        }
        node = node->_next;
    }
    *contains = FALSE;

    iRETURN;
}


// internal routines

ION_COLLECTION_NODE *_ion_collection_alloc_node_helper( ION_COLLECTION *collection )
{
    ION_COLLECTION_NODE *node;

    ASSERT ( collection != NULL );

    if (collection->_freelist) {
        node = collection->_freelist;
        collection->_freelist = node->_next;
    }
    else {
        node = ion_alloc_with_owner( collection->_owner, collection->_node_size );
        if (node == NULL) return NULL;
    }

    // clean up the node before anyone uses it
    node->_next = NULL;
    node->_prev = NULL;

    return node;    
}

ION_COLLECTION_NODE *_ion_collection_push_node_helper(ION_COLLECTION *collection)
{
//  iENTER;
    ION_COLLECTION_NODE *node;

    node = _ion_collection_alloc_node_helper( collection );
    if (node == NULL) return NULL;

    node->_next = collection->_head;
    if (collection->_head) {
        collection->_head->_prev = node;
    }
    else {
        // the first node added to an empty list
        collection->_tail = node;
    }
    collection->_head = node;

    collection->_count++;

    return node;
}

ION_COLLECTION_NODE *_ion_collection_append_node_helper(ION_COLLECTION *collection)
{
//  iENTER;
    ION_COLLECTION_NODE *node;

    node = _ion_collection_alloc_node_helper( collection );
    if (node == NULL) return NULL;

    node->_prev = collection->_tail;
    if (collection->_tail) {
        collection->_tail->_next = node;
    }
    else {
        // the first node added to an empty list
        collection->_head = node;
    }
    collection->_tail = node;

    collection->_count++;

    return node;
}

void _ion_collection_remove_node_helper(ION_COLLECTION *collection, ION_COLLECTION_NODE *node)
{
    ION_COLLECTION_NODE *next, *prev;

    ASSERT(collection);
    ASSERT(node);

    // remove the node from the "in use" list
    prev = node->_prev;
    next = node->_next;

    if (next) {
        next->_prev = prev;
    }
    else {
        collection->_tail = prev;
    }
    if (prev) {
        prev->_next = next;
    }
    else {
        collection->_head = next;
    }

    // put the node on the freelist
    _ion_collection_free_node_helper(collection, node);
    //node->_next = collection->_freelist;
    //collection->_freelist = node;

    // and uncount
    collection->_count--;

    ASSERT(collection->_count >= 0);
    ASSERT(collection->_count > 0 || (collection->_head == NULL && collection->_tail == NULL));

    return;
}

void _ion_collection_free_node_helper(ION_COLLECTION *collection, ION_COLLECTION_NODE *node)
{
#ifdef MEM_DEBUG
    node->_next = collection->_freelist;
//    collection->_freelist = node;
    _ion_collection_clear_node(collection, node);
#else
    node->_next = collection->_freelist;
    collection->_freelist = node;
#endif
}
