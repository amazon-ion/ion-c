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

/**@file */

#ifndef ION_DEBUG_H_
#define ION_DEBUG_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

//
// support routines for error handling
//

#define ION_ERROR_MESSAGE_MAX_LENGTH 1024


#ifdef DEBUG

    #define DEBUG_ERR(x)      fprintf(stderr,"\nERROR %d [%s] AT LINE %d IN %s\n" \
                                            , (int)(x), ion_error_to_str(x) \
                                            , (int)__LINE__, __location_display_name__)
    #define DEBUG_ERRMSG(x,m) fprintf(stderr,"\nERROR %d [%s] WITH MESSAGE '%s' AT LINE %d IN %s\n" \
                                            , (int)(x), ion_error_to_str(x) \
                                            , (char *)(m) \
                                            , (int)__LINE__, __location_display_name__)
    #define BREAK             ion_helper_breakpoint()
    #define ENTER(f,l,c)      ion_helper_enter(f, l, c)
    #define RETURN(f,l,c,e)   return ion_helper_return(f, l, c, e)
     #ifndef __func__
      #define __location_name__ __file__
      #define __location_display_name__ ion_helper_short_filename(__file__)
      #define ENTER_FILE      static /*const*/ char *__file__ = __FILE__
     #else
      #define __location_name__ __func__
      #define __location_display_name__ __func__
      #define ENTER_FILE      static const char *__file__ = __func__
     #endif
    #define FN_DEF            static long __count__ = 0; \
                              ENTER_FILE;  \
                              /* static const*/ int __line__ = __LINE__; \
                              long __temp__ = ENTER(__file__, __line__, __count__);
#else
    #define DEBUG_ERR(x)      /* nothing */
    #define DEBUG_ERRMSG(x,m) /* nothing */
    #define BREAK             ion_helper_breakpoint() /* nothing */
    #define ENTER(f,l,c)      /* nothing */
    #define RETURN(f,l,c,e)   return e
    #define FN_DEF            /* nothing */
#endif

#define iENTER             FN_DEF iERR err = IERR_OK
#define DONTFAILWITH(x)  { err = x; goto fail; }
#define FAILWITH(x)      { BREAK; DEBUG_ERR(x); err = x; goto fail; }
#define FAILWITHMSG(x,s) { BREAK; DEBUG_ERRMSG(x,s); err = x; goto fail; }
#define IONCHECK(x)      { err = x; if (err) goto fail; }
#define SUCCEED()        { err = IERR_OK; goto fail; }
#define iRETURN            fail: RETURN(__location_name__, __line__, __count__++, err)

/** The purpose of this Macro is enabling executing a list of functions while
 * keeping the first error encountered.
 *
 * Each function(x) to be executed must have return type of iERR, and need to be enclosed by this Macro.
 *
 * If error has already happened before, the error code of the current function will be ignored.
 * If there're no previous errors, the return code of the current function will be kept.
 */
#define UPDATEERROR(x)       { iERR errBackup = (x); if (err == IERR_OK) { err = errBackup; }}

#define ION_TIMESTAMP_STRING_LENGTH 55 /* does NOT include null terminator */

#define ION_VERSION_MARKER_LENGTH 4

/** DEPRECATED - use the accessor functions below. */
GLOBAL BOOL g_ion_debug_tracing INITTO(FALSE);

ION_API_EXPORT BOOL ion_debug_has_tracing();
ION_API_EXPORT void ion_debug_set_tracing(BOOL state);

#ifdef __cplusplus
}
#endif

#endif /* ION_DEBUG_H_ */
