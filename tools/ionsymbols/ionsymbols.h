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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <ion.h>

#ifdef IZ_INITIALIZE
#define IZ_GLOBAL 
#define IZ_INITTO(x) = x
#endif

#ifndef IZ_GLOBAL
#define IZ_GLOBAL extern
#define IZ_INITTO(x)
#endif

#define APP_NAME "ionsymbols"

#define MAX_FILE_NAME_LEN 1024

typedef struct _str_node STR_NODE;
struct _str_node {
    char     *str;
    STR_NODE *next;
};

IZ_GLOBAL BOOL  g_include_counts IZ_INITTO(FALSE);

IZ_GLOBAL BOOL  g_verbose        IZ_INITTO(FALSE);
IZ_GLOBAL BOOL  g_print_help     IZ_INITTO(FALSE);
IZ_GLOBAL BOOL  g_debug          IZ_INITTO(FALSE);
IZ_GLOBAL BOOL  g_dump_args      IZ_INITTO(FALSE);
IZ_GLOBAL BOOL  g_timer          IZ_INITTO(FALSE);

IZ_GLOBAL char *g_update_symtab IZ_INITTO(NULL);

#define UNDEFINED_SYMTAB_NAME ("unnamed_symbol_table")
IZ_GLOBAL char *g_symtab_name   IZ_INITTO(UNDEFINED_SYMTAB_NAME);

#define UNDEFINED_VERSION_NUMBER (-1)
IZ_GLOBAL int   g_symtab_version IZ_INITTO(UNDEFINED_VERSION_NUMBER);

IZ_GLOBAL STR_NODE *g_catalogs  IZ_INITTO(NULL);


#define CHECKREADER(fn, msg, reader) \
    if ((err = (fn)) != IERR_OK ) { \
        err = report_error(err, msg, reader, __FILE__, __LINE__); \
        if (err) goto fail; \
    }

#define CHECK(fn, msg) \
    if ((err = (fn)) != IERR_OK ) { \
        err = report_error(err, msg, NULL, __FILE__, __LINE__); \
        if (err) goto fail; \
    }


// globals for "real" state we'll be reusing here and there
IZ_GLOBAL ION_STRING         g_symbol_counts_str;
IZ_GLOBAL hSYMTAB            g_hsymtab           IZ_INITTO(NULL);
IZ_GLOBAL hCATALOG           g_hcatalog          IZ_INITTO(NULL);
IZ_GLOBAL BOOL               g_ion_debug_timer   IZ_INITTO(FALSE);

IZ_GLOBAL ION_READER_OPTIONS g_reader_options;
IZ_GLOBAL ION_WRITER_OPTIONS g_writer_options;


//
// in ionsymbols.c
//
iERR process_filename(char *pathname, ION_READER_OPTIONS *options);
iERR process_one_file(char *filename, ION_READER_OPTIONS *options);
iERR process_input_reader(hREADER hreader);

iERR load_symbol_table(hSYMTAB *p_hsymtab, char *symtab_file_name);
iERR initialize_new_symbol_table(hSYMTAB *p_hsymtab);
iERR load_catalog_list(hCATALOG *p_catalog);
iERR symbol_table_fill( hREADER hreader );

iERR symbol_table_write( hWRITER hwriter );
int  compare_sids_by_count(const void *psid1, const void *psid2) ;

iERR report_error(iERR err, const char *msg, hREADER reader, const char *file, int line);

void start_timing(void);
void stop_timing(void);


//
// in ionsymbols_args.c
//
void process_args(int argc, char **argv, char **p_non_argv, int *p_non_argc);
void print_help(void);
void dump_arg_globals(void);

