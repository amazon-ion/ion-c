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

/**
 * Ion strings are length prefixed value encoded using UTF8
 * this struct provides an independent lifetime for these
 * where references into the current buffer or references to
 * external strings are unified as a single representation
 *
 * a length == 0 and value == NULL is a null string
 */



#ifndef ION_STRING_H_
#define ION_STRING_H_

#include <string.h>
#include "ion_platform_config.h"
#include "ion_types.h"

#ifdef __cplusplus
extern "C" {
#endif

struct _ion_string
{

    int32_t length; /**< The number of bytes in the value array. */


    BYTE  *value; /**< UTF-8 encoded text, not null-terminated. */
};

#define DEFAULT_STRING_LENGTH       8 /**< default for minimum alloc, 8 is average from IBM study */
// WAS: #define INIT_ION_STRING(x)  memset((x), 0, sizeof(*(x)))
// WAS: #define ION_STRING_INIT(x)  memset((x), 0, sizeof(*(x)))
#define ION_STRING_INIT(x)          (x)->length = 0; (x)->value = NULL
#define ION_STRING_ASSIGN(dst, src) (dst)->length = (src)->length; (dst)->value = (src)->value
#define ION_STRING_IS_NULL(x)       ((x) == NULL || ((x)->value == NULL))
#define ION_STRING_EQUALS(x, y)     (((x) == (y)) || (((x)->length == (y)->length) && (memcmp((x)->value, (y)->value, (x)->length) == 0)))
#define ION_STRING_CHAR_AT(str, ii) ((ii) < (str)->length ? (str)->value[ii] : -1)

ION_API_EXPORT void        ion_string_init         (ION_STRING *str);
ION_API_EXPORT void        ion_string_assign       (ION_STRING *dst, ION_STRING *src);  // assigns contents but doesn't move bytes
ION_API_EXPORT ION_STRING *ion_string_assign_cstr  (ION_STRING *str, char *val, SIZE len);
ION_API_EXPORT char       *ion_string_strdup       (ION_STRING *p_ionstring);
ION_API_EXPORT iERR        ion_string_copy_to_owner(hOWNER owner, ION_STRING *dst, ION_STRING *src);

/**
 * Gets the number of UTF-8 bytes held by the string.
 *
 * @param str must not be null.
 *
 * @return may be zero.
 */
ION_API_EXPORT int         ion_string_get_length(ION_STRING *str);

/**
 * Returns -1 is idx is out of range or str is null
 */
ION_API_EXPORT BYTE        ion_string_get_byte(ION_STRING *str, int idx);

/**
 * Gets a pointer to the UTF-8 bytes held by the string.
 * The number of bytes in the string is determined via ion_string_get_length().
 *
 * @param str must not be null.
 *
 * @return a pointer to the first UTF-8 byte in the string; may be null.
 *   The byte sequence is not null-terminated.
 */
ION_API_EXPORT BYTE       *ion_string_get_bytes(ION_STRING *str);

ION_API_EXPORT BOOL        ion_string_is_null  (ION_STRING *str);
ION_API_EXPORT BOOL        ion_string_is_equal (ION_STRING *str1, ION_STRING *str2);

#ifdef __cplusplus
}
#endif
#endif /* ION_STRING_H_ */
