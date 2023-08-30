#pragma once

#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

struct memory_usage {
   size_t alloc;
   size_t current;
   size_t freed;
   size_t high;
   size_t low;
};

void start_memusage();
void stop_memusage();
void load_memusage(struct memory_usage *usage);
void clear_memusage();

#ifdef __cplusplus
}
#endif
