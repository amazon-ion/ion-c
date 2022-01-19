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

/**@file */

#ifndef ION_COLLECTION_H_
#define ION_COLLECTION_H_

#include "ion_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _ion_collection_node  ION_COLLECTION_NODE;
typedef struct _ion_collection_node *ION_COLLECTION_CURSOR;


#define IPCN_DATA_SIZE      sizeof(void *)
#define IPCN_OVERHEAD_SIZE  (sizeof(ION_COLLECTION_NODE) - IPCN_DATA_SIZE)

#define IPCN_pNODE_TO_pDATA(x) (&((x)->_data[0]))
#define IPCN_pDATA_TO_pNODE(x) ((ION_COLLECTION_NODE *) (((uint8_t *)(x)) - IPCN_OVERHEAD_SIZE))


/** The node allocation scheme depends on this layout !
 * currently that there are only 2 members so it uses
 * the size of the ptr as the base to allocate
 */
struct _ion_collection_node
{
    ION_COLLECTION_NODE *_next;
    ION_COLLECTION_NODE *_prev;
    uint8_t              _data[IPCN_DATA_SIZE]; // this is a place holder, length is max value length
};


/** The collections used by the parser are linked lists which are
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
 *
 * to use this as a:
 *    queue you'll want to "append" and "pop head"
 *    stack you'll want to "push" and "pop head"
 */
struct _ion_collection
{
    void                *_owner;
    int32_t              _node_size;
    int32_t              _count;
    ION_COLLECTION_NODE *_head;
    ION_COLLECTION_NODE *_tail;
    ION_COLLECTION_NODE *_freelist;
};

// BOOL ion_collection_is_empty(ION_COLLECTION *collection)
#define ION_COLLECTION_IS_EMPTY(collection)         \
    ((collection)->_head == NULL)

// SIZE count = ion_collection_size(ION_COLLECTION_CURSOR *pcursor)
#define ION_COLLECTION_SIZE(pcol)                   \
    ((pcol)->_count)

// ION_COLLECTION_CURSOR pcursor = ion_collection_open(ION_COLLECTION *collection)
#define ION_COLLECTION_OPEN(collection, pcursor)    \
    (pcursor) = (collection)->_head

// void *pbuf = ion_collection_next(ION_COLLECTION_CURSOR *pcursor)
#define ION_COLLECTION_NEXT(pcursor, pbuf)          \
    if ((pcursor) != NULL) {                        \
        *((void **)&(pbuf)) = IPCN_pNODE_TO_pDATA(pcursor);  \
        (pcursor) = (pcursor)->_next;               \
    } else {                                        \
        *((void **)&(pbuf)) =  NULL;                \
    }

// ION_COLLECTION_CURSOR *pcursor = ion_collection_close();
#define ION_COLLECTION_CLOSE(pcursor)               \
    (pcursor) = NULL


#ifdef __cplusplus
}
#endif

#endif
