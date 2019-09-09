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

#ifndef ION_HELPERS_H_
#define ION_HELPERS_H_

#include <ionc/ion_types.h>
#include <ionc/ion_platform_config.h>
#include "ion_writer_impl.h"

#ifdef __cplusplus
extern "C" {
#endif

// helper functions in IonHelper.c
ION_API_EXPORT BOOL     ion_helper_is_ion_version_marker(BYTE *buffer, SIZE len);
ION_API_EXPORT int      ion_helper_get_tid_from_ion_type(ION_TYPE t);
ION_API_EXPORT ION_TYPE ion_helper_get_iontype_from_tid(int tid);

ION_API_EXPORT void ion_helper_breakpoint(void);
ION_API_EXPORT long ion_helper_enter(const char *filename, int line_number, long count);
ION_API_EXPORT iERR ion_helper_return(const char *filename, int line_number, long count, iERR err);

ION_API_EXPORT const char *ion_helper_short_filename(const char *filename);
ION_API_EXPORT const char *_ion_hack_bad_value_to_str(intptr_t val, char *msg);

// utf8 helpers
ION_API_EXPORT int32_t ion_makeUnicodeScalar(int32_t high_surrogate, int32_t low_surrogate);
ION_API_EXPORT int32_t ion_makeHighSurrogate(int32_t unicodeScalar);
ION_API_EXPORT int32_t ion_makeLowSurrogate(int32_t unicodeScalar);
ION_API_EXPORT BOOL    ion_isHighSurrogate(int32_t c);
ION_API_EXPORT BOOL    ion_isLowSurrogate(int32_t c);
ION_API_EXPORT BOOL    ion_isSurrogate(int32_t c);

// base64 encoding helpers
void _ion_writer_text_write_blob_make_base64_image(int triple, char *output);

// escape sequence helpers
char *_ion_writer_get_control_escape_string(int c);

//
// helpers to convert some of the public types to char *'s
// these return constants or a pointer to a singleton internal
// buffer - which is volitile and should be copied to local
// space if you want more than one of these - and are not
// thread safe.
ION_API_EXPORT const char *ion_type_to_str(ION_TYPE t);
ION_API_EXPORT const char *ion_error_to_str(iERR err);

// utility for portable integer to string, constrained to base-10
// NB dest must be large enough (MAX_INT32_LENGTH)
char *_ion_itoa_10(int32_t val, char *dst, SIZE len);
char *_ion_i64toa_10(int64_t val, char *dst, SIZE len);

// utility for portable strnlen
ION_API_EXPORT SIZE _ion_strnlen(const char *str, const SIZE maxlen);

/** Get the absolute value of the given integer.
 *
 */
uint32_t abs_int32(int32_t value);

/** Get the absolute value of the given integer.
 *
 */
uint64_t abs_int64(int64_t value);

/** Cast unsigned value with sign to signed value.
 *  NUMERIC_OVERFLOW if the uint value does not fit into a int
 */
iERR cast_to_int64(uint64_t unsignedInt64Value, BOOL is_negative, int64_t* int64Ptr);

#ifdef __cplusplus
}
#endif

#endif /* ION_HELPERS_H_ */
