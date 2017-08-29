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

//
// public functions to manipulate strings
//
// generally the macro version are recommended for
// internal use, which are defined in ion_internal.h
//

#include "ion_internal.h"

//
// ion strings are length prefixed value encoded using UTF8
// this struct provides an independant lifetime for these
// where references into the current buffer or references to
// external strings are unified as a single representation
//
// a length == 0 and value == NULL is a null string
//

void  ion_string_init(ION_STRING *str)
{
    ION_STRING_INIT(str);
}

// assigns contents but doesn't move bytes
void  ion_string_assign(ION_STRING *dst, ION_STRING *src)
{
    ION_STRING_ASSIGN(dst, src);
}

// assigns the contest of a c string to the ion_string
ION_STRING *ion_string_assign_cstr(ION_STRING *str, char *val, SIZE len)
{
    ASSERT(str);
    if (len > MAX_INT32) return NULL;

    if (len > 0) {
        if (!val) return NULL;
        str->length = (int32_t)len;
        str->value = (BYTE*)val;
    }
    else {
        str->length = 0;
        str->value = (BYTE*)val;
    }

    return str;
}

// assignment with ownership move
iERR  ion_string_copy_to_owner(hOWNER owner, ION_STRING *dst, ION_STRING *src)
{
    iENTER;

    ASSERT(dst != NULL);

    ION_STRING_INIT(dst);
    if (ION_STRING_IS_NULL(src)) SUCCEED();
    dst->value = ion_alloc_with_owner(owner, src->length);
    if (dst->value == NULL) FAILWITH(IERR_NO_MEMORY);
    memcpy(dst->value, src->value, src->length);
    dst->length = src->length;

    iRETURN;
}

// length in utf-8 bytes
int   ion_string_get_length(ION_STRING *str)
{
    return str->length;
}

// was: char *ion_str_dup_chars(ION_STRING *pionstring)
char *ion_string_strdup(ION_STRING *pionstring)
{
    char *str = ion_xalloc(pionstring->length + 1);
    if (!str) return NULL;

    memcpy(str, pionstring->value, pionstring->length);
    str[pionstring->length] = 0;

    return str;
}

// returns -1 is idx is out of range or str is null
BYTE ion_string_get_byte(ION_STRING *str, int idx)
{
    if (idx < 0)                 return -1;
    if (ION_STRING_IS_NULL(str)) return -1;
    if (idx >= str->length)      return -1;
    return *(str->value + idx);
}

BYTE *ion_string_get_bytes(ION_STRING *str)
{
    return str->value;
}

BOOL  ion_string_is_null(ION_STRING *str)
{
    return ION_STRING_IS_NULL(str);
}

BOOL  ion_string_is_equal(ION_STRING *str1, ION_STRING *str2)
{
    return ION_STRING_EQUALS(str1, str2);
}
