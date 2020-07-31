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

//
// Ion internal header for platform configurations
//

#ifndef ION_PLATFORM_CONFIG_H_
#define ION_PLATFORM_CONFIG_H_

// OS Macros

#ifdef	_WIN32
#define ION_PLATFORM_WINDOWS
#endif

#ifdef __CYGWIN__
#define ION_PLATFORM_CYGWIN
#endif

#ifdef __ANDROID__
#define ION_PLATFORM_ANDROID
#endif

// Ion Public API Export
// NB - for gcc/clang -fvisibility=hidden should be used, otherwise, all symbols are exported
#if (defined(ION_PLATFORM_WINDOWS) || defined(ION_PLATFORM_CYGWIN)) && defined(_WINDLL)
#define ION_API_EXPORT __declspec(dllexport)
#elif __GNUC__ >= 4
#define ION_API_EXPORT __attribute__ ((visibility("default")))
#else
#define ION_API_EXPORT
#endif

// Support for thread local storage across compilers
#ifdef __STDC_VERSION__ >= 201112L
#define THREAD_LOCAL_STORAGE _Thread_local
#elif __GNUC__
#define THREAD_LOCAL_STORAGE __thread
#elif defined(_MSC_VER)
#define THREAD_LOCAL_STORAGE __declspec(thread)
#else
#error "Compiler does not support thread local storage"
#endif

#endif