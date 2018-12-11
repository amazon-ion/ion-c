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

#ifndef ION_ERRORS_
#define ION_ERRORS_

#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

/** define the Ion error code enumeration.
 *
 */
enum ion_error_code {
    IERR_NOT_IMPL = -1,

    #define ERROR_CODE(name, val)  name = val,
    #include "ion_error_codes.h"

    IERR_MAX_ERROR_CODE
};
// included in ion_error.h: #undef ERROR_CODE

typedef enum ion_error_code iERR;

/**
 * Gets a static string representation of an error code.
 */
ION_API_EXPORT const char *ion_error_to_str(iERR err);


#ifdef __cplusplus
}
#endif

#endif // ION_ERRORS_INCLUDED
