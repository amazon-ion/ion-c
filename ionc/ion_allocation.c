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

#include "ion_internal.h"

#ifdef MEM_DEBUG
#define VALIDATION_NONE      0
#define VALIDATION_OK        1
#define VALIDATION_BAD_HEAD  2
#define VALIDATION_BAD_TAIL  4
#define VALIDATION_BAD_BOTH  6

int  _dbg_ion_validation_id(BOOL head_valid, BOOL tail_valid);
void _dbg_ion_message(const char *fn_name, void *addr, SIZE len);
void _dbg_ion_message_validate(const char *fn_name, void *addr, SIZE len, int validation_id);
#endif

void                 *_ion_alloc_with_owner_helper  (ION_ALLOCATION_CHAIN *phead, SIZE length, BOOL force_new_block);
void                 *_ion_alloc_on_chain           (ION_ALLOCATION_CHAIN *phead, SIZE length);
ION_ALLOCATION_CHAIN *_ion_alloc_block              (SIZE min_needed);
void                  _ion_free_block               (ION_ALLOCATION_CHAIN *pblock);


//
//  public functions 
//

void *_ion_alloc_owner(SIZE len)
{
    void                 *owner;
    ION_ALLOCATION_CHAIN *new_chain;

    if (g_ion_alloc_page_list.page_size == ION_ALLOC_PAGE_POOL_PAGE_SIZE_NONE) {
        ion_initialize_page_pool(ION_ALLOC_PAGE_POOL_DEFAULT_PAGE_SIZE, ION_ALLOC_PAGE_POOL_DEFAULT_LIMIT);
    }

    new_chain = _ion_alloc_block(len);
    if (!new_chain) return NULL;

    owner = _ion_alloc_with_owner_helper(new_chain, len, FALSE);

    return owner;
}

void *_ion_alloc_with_owner(hOWNER owner, SIZE length)
{
    ION_ALLOCATION_CHAIN *phead;
    void *ptr;

    ASSERT(owner);

    phead = ION_ALLOC_USER_PTR_TO_BLOCK(owner);
    ptr = _ion_alloc_with_owner_helper(phead, length, FALSE);

    return ptr;
}

void _ion_free_owner(hOWNER owner)
{
    ION_ALLOCATION_CHAIN *powner = ION_ALLOC_USER_PTR_TO_BLOCK(owner);
    ION_ALLOCATION_CHAIN *pblk, *pnext;

    // free all the blocks in the owners allocation chain
    // (release them back to the shared block pool)
    for (pblk = powner->head; pblk; pblk = pnext) {
        pnext = pblk->next;
        _ion_free_block(pblk);
    }

    // now free the owner
    _ion_free_block(powner);

    return;
}

iERR _ion_strdup(hOWNER owner, iSTRING dst, iSTRING src)
{
    iENTER;

    BOOL is_empty = (src->length == 0 && src->value); // Distinguishing from null string, which has NULL value.

    if (!owner || !dst || !src) FAILWITH(IERR_INVALID_ARG);

    if (dst->length < src->length || src->length == 0) {
        dst->value = (BYTE *)ion_alloc_with_owner(owner, (is_empty) ? 1 : src->length);
        if (!dst->value) FAILWITH(IERR_NO_MEMORY);
    }
    memcpy(dst->value, (is_empty) ? "\0" : src->value, (is_empty) ? 1 : src->length);

    dst->length = src->length;

    iRETURN;
}

//
// internal (to this file) helper functions
//
void *_ion_alloc_with_owner_helper(ION_ALLOCATION_CHAIN *powner, SIZE request_length, BOOL force_new_block)
{
    ION_ALLOCATION_CHAIN *pblock = powner;
    BYTE                 *ptr, *next_ptr;
    SIZE                  length;

    ASSERT(powner);

    length = ALIGN_SIZE(request_length);
    if (length < 0 || length < request_length) {
        return NULL;
    }

    if ( !force_new_block ) {
        // check for space in blocks that are "in progress", starting with the
        // owner block, we only need to do this if we aren't already directed
        // to allocate a new block by force_new_block
        next_ptr = pblock->position + length;
        if (next_ptr > pblock->limit) {
            pblock = powner->head;
            if (pblock == NULL) {
                force_new_block = TRUE;
            }
            else {
                next_ptr = pblock->position + length;
                if (next_ptr > pblock->limit) {
                    force_new_block = TRUE;
                }
            }
        }
    }

    // no "else" since even if the caller didn't say we *had* to
    // create a new block we might need to just to make room
    if ( force_new_block ) {
        // otherwise we add a new block
        pblock = _ion_alloc_block(length);
        if (!pblock) return NULL;

        if (pblock->size > g_ion_alloc_page_list.page_size && powner->head != NULL) {
            // this is an oversized block, so don't put it
            // at the front since it will be full and we'll
            // have wasted the freespace in the current
            // front block.  This avoids the degenerate case
            // where we waste nearly empty pages everytime
            // an oversized page comes along and that's
            // reasonably often.  (yes, it happened)
            pblock->next = powner->head->next;
            powner->head->next = pblock;
        }
        else {
            // just another block, so put it at the front
            // of the list.  we've used as much memory from
            // the current block as we can.
            pblock->next = powner->head;
            powner->head = pblock;
        }
        next_ptr = pblock->position + length;
        assert(next_ptr <= pblock->limit); // we better have room at this point
    }

    // if we have a block here, there's enough room in it
    // save the current position ptr for our caller, and move
    // the "next available position" pointer forward
    ptr = pblock->position;
    pblock->position = next_ptr;

    if (ptr) memset(ptr, 0, length);
    return ptr;
}

ION_ALLOCATION_CHAIN *_ion_alloc_block(SIZE min_needed)
{
    ION_ALLOCATION_CHAIN *new_block;
    SIZE                  alloc_size = min_needed + sizeof(ION_ALLOCATION_CHAIN); // subtract out the block[1]

    if (alloc_size > g_ion_alloc_page_list.page_size) {
        // it's an oversize block - we'll ask the system for this one
        new_block = (ION_ALLOCATION_CHAIN *)ion_xalloc(alloc_size);    
        new_block->size = alloc_size;
    }
    else {
        // it's a normal size block - go out to the block pool for it
        new_block = (ION_ALLOCATION_CHAIN *)_ion_alloc_page();
        if (new_block) {
            new_block->size = g_ion_alloc_page_list.page_size;
        }
    }
    
    // see if we suceeded
    if (!new_block) return NULL;
    
    new_block->next     = NULL;
    new_block->head     = NULL;

    new_block->position = ION_ALLOC_BLOCK_TO_USER_PTR(new_block);
    new_block->limit    = ((BYTE*)new_block) + new_block->size;

    assert(new_block->position == ((BYTE *)(&new_block->limit) + sizeof(new_block->limit)));

    return new_block;
}

void _ion_free_block(ION_ALLOCATION_CHAIN *pblock)
{
    if (!pblock) return;
    if (pblock->size > g_ion_alloc_page_list.page_size) {
        ion_xfree(pblock);
    }
    else {
        _ion_release_page((ION_ALLOC_PAGE *)pblock);
    }
    return;
}

void ion_initialize_page_pool(SIZE page_size, int free_page_limit)
{
    // once the page list is in use, you can't change your mind
    if (g_ion_alloc_page_list.page_size != ION_ALLOC_PAGE_POOL_PAGE_SIZE_NONE)
    {
        return;
    }
    
    // we need a min size to hold the pointers we use to maintain the page list
    if (page_size < ION_ALLOC_PAGE_MIN_SIZE) 
    {
        page_size = ION_ALLOC_PAGE_MIN_SIZE;
    }
    g_ion_alloc_page_list.page_size       = page_size;
    g_ion_alloc_page_list.free_page_limit = free_page_limit;

    // TODO: should we pre-allocate the pages?
    
    return;
}

void ion_release_page_pool(void)
{
    ION_ALLOC_PAGE *page;
    
    while ((page = g_ion_alloc_page_list.head) != NULL) {
        g_ion_alloc_page_list.head = page->next;
        ion_xfree(page);
        g_ion_alloc_page_list.page_count--;
    }
    ASSERT(g_ion_alloc_page_list.page_count == 0);

    // here we mark the page size to indicate the pool is inactive
    g_ion_alloc_page_list.page_size = ION_ALLOC_PAGE_POOL_PAGE_SIZE_NONE;

    return;
}

ION_ALLOC_PAGE *_ion_alloc_page(void)
{
    ION_ALLOC_PAGE *page;
    
    if ((page = g_ion_alloc_page_list.head) != NULL) {
        g_ion_alloc_page_list.head = page->next;
        g_ion_alloc_page_list.page_count--;
    }
    else {
        ASSERT(g_ion_alloc_page_list.page_size != ION_ALLOC_PAGE_POOL_PAGE_SIZE_NONE);
        page = ion_xalloc(g_ion_alloc_page_list.page_size);
    }
    return page;
}

void _ion_release_page(ION_ALLOC_PAGE *page)
{
    if (page == NULL) return;

    if (g_ion_alloc_page_list.free_page_limit != ION_ALLOC_PAGE_POOL_NO_LIMIT
     && g_ion_alloc_page_list.page_count >= g_ion_alloc_page_list.free_page_limit
    ) {
        ion_xfree(page);
    }
    else {
        page->next = g_ion_alloc_page_list.head;
        g_ion_alloc_page_list.head = page;
        g_ion_alloc_page_list.page_count++;
    }
    return;
}



#ifdef MEM_DEBUG

long malloc_inuse = 0;
long malloc_alloced = 0;
long malloc_block = 0;
long malloc_cmd = 0;

long debug_cmd_counter() {
    malloc_cmd++;

    if (malloc_cmd == 34) {
        malloc_block += 1 - 2/2;
    }

    return malloc_cmd;
}

BYTE debug_pattern[] = {
    0xfe, 0xe0, 0xf1, 0xe0, 0xf0, 0xe0, 0xfe, 0xef
};
SIZE debug_pattern_size = sizeof(debug_pattern);

void *debug_malloc(SIZE size, const char *file, int line) 
{
    BYTE   *ptr, *psize, *head, *user, *tail;
    SIZE  adjusted_size = size + 2*debug_pattern_size * 2 + sizeof(SIZE);

    assert( debug_pattern_size == 8 ); // just to make sure we're getting the right value and know what's actually happening

    ptr = (BYTE *)malloc(adjusted_size);

    malloc_block++;
    malloc_inuse++;
    malloc_alloced += size;
    debug_cmd_counter();

    if (!ptr) {
        return NULL;
    }


    // calculate the offsets of the various interesting parts
    // where the size is stored, the start of the header
    // and tail patterns, and the users pointer
    // the order is:
    //    ptr = malloced address, also where the size is stored
    //   head = after size, holds header pattern
    //   user = after header pattern holds user data (variable)
    //   tail = after user data for tail pattern
    psize = ptr;
    head  = ptr + sizeof(SIZE);
    user  = head + debug_pattern_size;
    tail  = user + size;

    memcpy(head, debug_pattern, debug_pattern_size);
    memcpy(tail, debug_pattern, debug_pattern_size);

    *((SIZE *)psize) = size;

    _dbg_ion_message("___MALLOC", user, size);

    return user;
}

void debug_free(const void *addr, const char *file, int line) 
{
    BYTE    *ptr, *psize, *head, *user, *tail;
    SIZE  size;
    BOOL    head_valid, tail_valid;

    user = (BYTE *)addr;  // just to save casting all over the place

    debug_cmd_counter();

    assert( addr != NULL );

    // calculate the offsets of the various interesting parts
    // where the size is stored, the start of the header
    // and tail patterns, and the users pointer
    // the order is:
    //    ptr = malloced address, also where the size is stored
    //   head = after size, holds header pattern
    //   user = after header pattern holds user data (variable)
    //   tail = after user data for tail pattern
    head  = user - debug_pattern_size;
    psize = head - sizeof(SIZE);
    ptr   = psize;
    size  = *((SIZE *)psize);
    tail  = user + size;

    malloc_inuse--;
    malloc_alloced -= size;

    // check the patterns we wrote into the head and tail
    head_valid = (memcmp(head, debug_pattern, debug_pattern_size) == 0);
    tail_valid = (memcmp(tail, debug_pattern, debug_pattern_size) == 0);

    _dbg_ion_message_validate("___free"
         , user
         , size
         , _dbg_ion_validation_id(head_valid, tail_valid)
    );

    memset(ptr, 0xb0, size + 2*debug_pattern_size + sizeof(SIZE));
    free(ptr);
}

void *_dbg_ion_alloc_owner(SIZE len, const char *file, int line)
{
    long                  cmd  = debug_cmd_counter();
    void                 *owner;
    ION_ALLOCATION_CHAIN *new_chain;

    new_chain = _ion_alloc_block(len);
    if (!new_chain) return NULL;

    owner = _ion_alloc_with_owner_helper(new_chain, len, FALSE);

    _dbg_ion_message("___OWNER", ION_ALLOC_USER_PTR_TO_BLOCK(owner), len);

    return owner;
}

void *_dbg_ion_alloc_with_owner(hOWNER owner, SIZE length, const char *file, int line)
{
    long cmd  = debug_cmd_counter();
    DBG_ION_ALLOCATION_CHAIN *phead;
    void *ptr;

    ASSERT(owner);

    phead = ION_ALLOC_USER_PTR_TO_BLOCK(owner);
    ptr = _ion_alloc_with_owner_helper(phead, length, TRUE);  // DEBUG: TRUE, always make a new block

    _dbg_ion_message("___MEMBER", ION_ALLOC_USER_PTR_TO_BLOCK(ptr), length);

    return ptr;
}

void _dbg_ion_free_owner(hOWNER owner, const char *file, int line)
{
    long cmd  = debug_cmd_counter();
    DBG_ION_ALLOCATION_CHAIN *pcurr = ION_ALLOC_USER_PTR_TO_BLOCK(owner);
    DBG_ION_ALLOCATION_CHAIN *pnext = pcurr->head;

    _dbg_ion_message("___FREE_OWNER", pcurr, -1);

    ion_xfree(pcurr);

    while (pnext) {
        pcurr = pnext;
        pnext = pcurr->next;
        ion_xfree(pcurr);
    }
}

iERR _dbg_ion_strdup(hOWNER owner, iSTRING dst, iSTRING src, const char *file, int line)
{
    iENTER;
    long cmd  = debug_cmd_counter();

    if (!owner || !dst || !src) FAILWITH(IERR_INVALID_ARG);

    if (dst->length < src->length) {
        dst->value = ion_alloc_with_owner(owner, src->length);
        if (!dst->value) FAILWITH(IERR_NO_MEMORY);
    }
    memcpy(dst->value, src->value, src->length);
    dst->length = src->length;

    _dbg_ion_message("___STR", ION_ALLOC_USER_PTR_TO_BLOCK(dst->value), src->length);

    iRETURN;
}


int _dbg_ion_validation_id(BOOL head_valid, BOOL tail_valid)
{
    int validation_id = VALIDATION_NONE;

    if (head_valid && tail_valid) {
        validation_id = VALIDATION_OK;
    }
    else if (!head_valid && !tail_valid) {
        validation_id = VALIDATION_BAD_BOTH;
    }
    else if (!head_valid && tail_valid) {
        validation_id = VALIDATION_BAD_HEAD;
    }
    else if (head_valid && !tail_valid) {
        validation_id = VALIDATION_BAD_TAIL;
    }
    return validation_id;
}

void _dbg_ion_message(const char *fn_name, void *addr, SIZE len)
{
    _dbg_ion_message_validate( fn_name, addr, len, VALIDATION_NONE);
}

BOOL dbg_needs_header = TRUE;
void _dbg_ion_message_validate(const char *fn_name, void *addr, SIZE len, int validation_id)
{
    if (dbg_needs_header) {
        dbg_needs_header = FALSE;
        printf("fn, ptr, cmd_id, size, block, test\n");
    }

    printf("\"%s\"", (const char *)fn_name);
    printf(", %ld", (long)addr);
    printf(", %ld", (long)malloc_cmd);

    if ((long)len == -1) {
        printf(", na");
    }
    else {
        printf(", %ld", (long)len);
    }
    printf(", %ld", (long)malloc_alloced);

    switch(validation_id) {
    case VALIDATION_NONE:
        break;
    case VALIDATION_OK:
        printf(", ok");
        break;
    case VALIDATION_BAD_HEAD:
        printf(", INVALID_HEAD");
        break;
    case VALIDATION_BAD_TAIL:
        printf(", INVALID_TAIL");
        break;
    case VALIDATION_BAD_BOTH:
        printf(", INVALID_HEAD|INVALID_TAIL");
        break;
    default:
        assert(FALSE && "invalid switch on validation id");
    }
    printf("\n");
}

#endif
