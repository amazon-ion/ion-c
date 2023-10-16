#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>

#if defined(__MACH__)
#  include <malloc/malloc.h>
#elif defined(__linux__)
#  include <malloc.h>
#  define malloc_size(x) malloc_usable_size(x)
#endif

#include "memory.h"

// This is not threadsafe. We should not be executing benchmarks using
// multiple threads. This should only be included in a build where we
// want to track memory usage. Which should be handled by our CMake
// config.


struct memory_usage MEMUSAGE = {
   .alloc  = 0,
   .current= 0,
   .freed  = 0,
   .high   = 0,
   .low    = 0,
};

extern void *__libc_malloc(size_t size);
extern void *__libc_calloc(size_t n, size_t size);
extern void *__libc_realloc(void *oldmem, size_t bytes);
extern void __libc_free(void *ptr);

void *malloc_hook(size_t size);
void free_hook(void *ptr);

int hook_active = 0;

void start_memusage() {
   hook_active = 1;
}

void stop_memusage() {
   hook_active = 0;
}

extern void *malloc(size_t size) {
   if (hook_active == 1) {
      return malloc_hook(size);
   } else {
      return __libc_malloc(size);
   }
}

void *malloc_hook(size_t size) {
   hook_active = 0;

   void *ptr = __libc_malloc(size);
   size_t alloc_size = malloc_size(ptr);

   MEMUSAGE.alloc += alloc_size;
   MEMUSAGE.current += alloc_size;

   if (MEMUSAGE.current > MEMUSAGE.high) {
      MEMUSAGE.high = MEMUSAGE.current;
   }

   if (MEMUSAGE.low == 0)
      MEMUSAGE.low = MEMUSAGE.current;
   // printf("[MEM] malloc(%lu) alloc:%lu hi:%lu lo:%lu current:%lu\n", size, alloc_size, MEMUSAGE.high, MEMUSAGE.low, MEMUSAGE.current);
   hook_active = 1;
   return ptr;
}

void *calloc_hook(size_t n, size_t size) {
   hook_active = 0;

   void *ptr = __libc_calloc(n, size);
   size_t alloc_size = malloc_size(ptr);

   MEMUSAGE.alloc += alloc_size;
   MEMUSAGE.current += alloc_size;

   if (MEMUSAGE.current > MEMUSAGE.high) {
      MEMUSAGE.high = MEMUSAGE.current;
   }

   if (MEMUSAGE.low == 0)
      MEMUSAGE.low = MEMUSAGE.current;

   hook_active = 1;
   return ptr;
}

extern void *calloc(size_t n, size_t size) {
   if (hook_active == 1) {
      return calloc_hook(n, size);
   } else {
      return __libc_calloc(n, size);
   }
}

extern void free(void *ptr) {
   if (hook_active == 1) {
      free_hook(ptr);
   } else {
      __libc_free(ptr);
   }
}

void free_hook(void *ptr) {
   hook_active = 0;
   if (ptr != NULL) {
      size_t size = malloc_size(ptr);

      size_t old_current = MEMUSAGE.current;

      MEMUSAGE.freed += size;
      MEMUSAGE.current -= size;

      // Detecting underflow, JIC we missed something.. like an allocation, or re-allocation.
      if (MEMUSAGE.current > old_current) {
         printf("[FREE] UNDERFLOW old: %lu  free'd: %lu\n", old_current, size);
      }

      if (MEMUSAGE.current < MEMUSAGE.low || MEMUSAGE.low == 0) {
         MEMUSAGE.low = MEMUSAGE.current;
      }

      __libc_free(ptr);
      // printf("[MEM] free(%lu) hi:%lu lo:%lu current:%lu\n", size, MEMUSAGE.high, MEMUSAGE.low, MEMUSAGE.current);
   }
   hook_active = 1;
}

void *realloc_hook(void *oldmem, size_t bytes) {
   hook_active = 0;

   size_t presize = malloc_size(oldmem);
   void *ptr = __libc_realloc(oldmem, bytes);
   size_t postsize = malloc_size(ptr);

   // Given our new size, we need to adjust..
   long diff = (postsize - presize);
   MEMUSAGE.alloc += diff;
   MEMUSAGE.current += diff;
   if (MEMUSAGE.current > MEMUSAGE.high)
      MEMUSAGE.high = MEMUSAGE.current;
   if (MEMUSAGE.current < MEMUSAGE.low)
      MEMUSAGE.low = MEMUSAGE.current;

   // printf("[REALLOC] %p old: %lu   new: %lu\n", oldmem, presize, postsize);

   hook_active = 1;
   return ptr;
}

extern void *realloc(void *oldmem, size_t bytes) {
   if (hook_active == 1 ) {
      return realloc_hook(oldmem, bytes);
   } else {
      return __libc_realloc(oldmem, bytes);
   }
}


void load_memusage(struct memory_usage *usage) {
   memcpy(usage, &MEMUSAGE, sizeof(struct memory_usage));
}

void clear_memusage() {
   memset(&MEMUSAGE, 0, sizeof(struct memory_usage));
}
