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

#ifndef ION_ALLOC_H_
#define ION_ALLOC_H_

#include <ionc/ion_types.h>
#include <ionc/ion_platform_config.h>

#ifdef __cplusplus
extern "C" {
#endif

//
// ion managed resources are broken into two general classes, primary
// and secondary (ignoring the third case of "user").
//
// primary resource include readers, writers, catalogs and sometimes
// symbol tables (symtabs).
//
// primary resources are separately allocated and are freed when they
// are closed.
//
// secondary resources are allocated in the scope of a primary resource.
// secondary resources are, for example, ion_strings, decimal and timestamp
// values.  and often symbol tables.
// secodary resources are automattically freed when their associated
// primary resource is freed.
//
// this is handled by making allocating memory in pages, where the
// primary resource is the first page of such a chain.  Then freeing
// the entire chain as a single operation.
//

//
// support routines for memory managment
//
ION_API_EXPORT char       *ion_alloc_name      (hOWNER  owner, SIZE length);
ION_API_EXPORT iIMPORT    *ion_alloc_import    (hSYMTAB hsymtab);
ION_API_EXPORT iSYMBOL    *ion_alloc_symbol    (hSYMTAB hsymtab);
ION_API_EXPORT decQuad    *ion_alloc_decimal   (hOWNER  owner);
ION_API_EXPORT iTIMESTAMP  ion_alloc_timestamp (hOWNER  owner);
ION_API_EXPORT void        ion_alloc_free      (void *ptr);

// define MEM_DEBUG with compiler flag to turn on memory debugging
    
#if defined(MEM_DEBUG)

    #include <stdio.h>

    void *debug_malloc(size_t size, const char *file, int line);
    void  debug_free(const void *ptr, const char *file, int line);

    #define ion_xalloc(x) debug_malloc((x), __FILE__, __LINE__)
    #define ion_xfree(x)  debug_free((x), __FILE__, __LINE__)

#else

    #include <stdlib.h>

    #define ion_xalloc(sz)  malloc(sz)
    #define ion_xfree(ptr)  free(ptr)

#endif

//#ifndef ION_ALLOCATION_BLOCK_SIZE
//#define ION_ALLOCATION_BLOCK_SIZE DEFAULT_BLOCK_SIZE
//#endif

// DEFAULT_BLOCK_SIZE was defined in ion_internal.h, but needed for initializing g_ion_alloc_page_list.
#define DEFAULT_BLOCK_SIZE (1024*64)

// force aligned allocations
#ifndef ALLOC_ALIGNMENT
#define ALLOC_ALIGNMENT 4
#endif
    
#define ALIGN_MASK ((ALLOC_ALIGNMENT)-1)
    
#if ((ALLOC_ALIGNMENT) & ALIGN_MASK) != 0
#error Invalid ALLOC_ALIGNMENT. Must be a power of 2.
#endif
    
#define ALIGN_SIZE(size) ((((size_t)(size)) + ALIGN_MASK) & ~ALIGN_MASK)
#define ALIGN_PTR(ptr) ALIGN_SIZE(ptr)

typedef struct _ion_allocation_chain ION_ALLOCATION_CHAIN;

struct _ion_allocation_chain 
{
    SIZE                  size;
    ION_ALLOCATION_CHAIN *next;
    ION_ALLOCATION_CHAIN *head;

    BYTE                 *position;
    BYTE                 *limit;
    // user bytes follow this header, though there may be some unused bytes here for alignment purposes
};

#define ION_ALLOC_BLOCK_TO_USER_PTR(block) ((BYTE*)ALIGN_PTR(((BYTE*)(block)) + sizeof(ION_ALLOCATION_CHAIN)))
#define ION_ALLOC_USER_PTR_TO_BLOCK(ptr)   ((ION_ALLOCATION_CHAIN *)(((BYTE*)(ptr)) - sizeof(ION_ALLOCATION_CHAIN)))


#ifdef MEM_DEBUG
typedef struct _ion_allocation_chain DBG_ION_ALLOCATION_CHAIN;

#define ion_alloc_owner(len)                _dbg_ion_alloc_owner(len, __FILE__, __LINE__)
#define ion_alloc_with_owner(owner, length) _dbg_ion_alloc_with_owner(owner, length, __FILE__, __LINE__)
#define ion_free_owner(owner)               _dbg_ion_free_owner(owner, __FILE__, __LINE__)
#define ion_strdup(owner, dst, src)         _dbg_ion_strdup(owner, dst, src, __FILE__, __LINE__)
#else
#define ion_alloc_owner(len)                _ion_alloc_owner(len) 
#define ion_alloc_with_owner(owner, length) _ion_alloc_with_owner(owner, length)
#define ion_free_owner(owner)               _ion_free_owner(owner)
#define ion_strdup(owner, dst, src)         _ion_strdup(owner, dst, src)
#endif


//
//  structures and functions for the ion allocation page pool
//  this is a list of free pages which are used by the various
//  pools created by the Ion routines on behalf of the various
//  objects, such as the reader, writer, or catalog.
//
//  THIS IS NOT THREAD SAFE - (which could be corrected)
//

typedef struct _ion_alloc_page      ION_ALLOC_PAGE;
typedef struct _ion_alloc_page_list ION_ALLOC_PAGE_LIST;

struct _ion_alloc_page
{
    ION_ALLOC_PAGE *next;
};

struct _ion_alloc_page_list
{
    SIZE            page_size;
    int             page_count;
    int             free_page_limit;
    ION_ALLOC_PAGE *head;
};

#define ION_ALLOC_PAGE_POOL_NO_LIMIT           (-1)
#define ION_ALLOC_PAGE_POOL_DEFAULT_LIMIT      (16)
#define ION_ALLOC_PAGE_MIN_SIZE                (ALIGN_SIZE(sizeof(ION_ALLOCATION_CHAIN)))
#define ION_ALLOC_PAGE_POOL_DEFAULT_PAGE_SIZE  (DEFAULT_BLOCK_SIZE)
#define ION_ALLOC_PAGE_POOL_PAGE_SIZE_NONE     (-1)

GLOBAL THREAD_LOCAL_STORAGE ION_ALLOC_PAGE_LIST g_ion_alloc_page_list
#ifdef INIT_STATICS
= { ION_ALLOC_PAGE_POOL_DEFAULT_PAGE_SIZE, 0, ION_ALLOC_PAGE_POOL_DEFAULT_LIMIT, NULL }
#endif
;

ION_API_EXPORT void             ion_initialize_page_pool    (SIZE page_size, int free_page_limit);
ION_API_EXPORT void             ion_release_page_pool       (void);

ION_ALLOC_PAGE *_ion_alloc_page              (void);
void            _ion_release_page            (ION_ALLOC_PAGE *page);

void *_ion_alloc_owner     (SIZE len);
void *_ion_alloc_with_owner(hOWNER owner, SIZE length);
void  _ion_free_owner      (hOWNER owner);
iERR  _ion_strdup          (hOWNER owner, iSTRING dst, iSTRING src);



#ifdef MEM_DEBUG 
void *_dbg_ion_alloc_owner     (SIZE len, const char *file, int line);
void *_dbg_ion_alloc_with_owner(hOWNER owner, SIZE length, const char *file, int line);
void  _dbg_ion_free_owner      (hOWNER owner, const char *file, int line);
iERR  _dbg_ion_strdup          (hOWNER owner, iSTRING dst, iSTRING src, const char *file, int line);
#endif


#ifdef __cplusplus
}
#endif

#endif

