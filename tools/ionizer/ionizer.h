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

#include <ionc/ion.h>
#include "ion_helpers.h"

#ifdef IZ_INITIALIZE
#define IZ_GLOBAL 
#define IZ_INITTO(x) = x
#endif

#ifndef IZ_GLOBAL
#define IZ_GLOBAL extern
#define IZ_INITTO(x)
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_FILE_NAME_LEN 1024

typedef struct _ionizer_str_node IONIZER_STR_NODE;
struct _ionizer_str_node {
    char             *str;
    IONIZER_STR_NODE *next;
};

IZ_GLOBAL BOOL g_ionizer_pretty         IZ_INITTO(TRUE);
IZ_GLOBAL BOOL g_ionizer_ascii_only     IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_no_ivm         IZ_INITTO(TRUE);
IZ_GLOBAL BOOL g_ionizer_write_binary   IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_write_symtab   IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_include_counts IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_include_type_counts IZ_INITTO(FALSE);

IZ_GLOBAL BOOL g_ionizer_flush_each     IZ_INITTO(FALSE);

IZ_GLOBAL BOOL g_ionizer_verbose        IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_print_help     IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_debug          IZ_INITTO(FALSE);
IZ_GLOBAL BOOL g_ionizer_dump_args      IZ_INITTO(FALSE);

IZ_GLOBAL int  g_ionizer_symtab_version IZ_INITTO(0);
IZ_GLOBAL int  g_ionizer_pool_page_size IZ_INITTO(-1);

// IZ_GLOBAL char g_ionizer_symtab_name[MAX_FILE_NAME_LEN + 1] IZ_INITTO({0});
// IZ_GLOBAL char g_ionizer_catalog[MAX_FILE_NAME_LEN + 1] IZ_INITTO({0});

IZ_GLOBAL char *g_ionizer_writer_symtab        IZ_INITTO(NULL);
IZ_GLOBAL char *g_ionizer_output_symtab_name   IZ_INITTO(NULL);
IZ_GLOBAL IONIZER_STR_NODE *g_ionizer_catalogs IZ_INITTO(NULL);

IZ_GLOBAL BOOL g_ionizer_error_reported        IZ_INITTO(FALSE);

struct read_helper {
    ION_STREAM *in;
    hREADER     hreader;
};
typedef struct read_helper FSTREAM_READER_STATE;

struct write_helper {
    ION_STREAM *out;
    hWRITER     hwriter;
};
typedef struct write_helper FSTREAM_WRITER_STATE;



iERR ionizer_clear_reported_error(void);

#undef iENTER
#define iENTER FN_DEF iERR err = ionizer_clear_reported_error()

#define CHECKREADER(fn, msg, reader) \
    if ((err = (fn)) != IERR_OK ) { \
        err = ionizer_report_error(err, msg, reader \
            , ion_helper_short_filename(__FILE__), __LINE__); \
        if (err) goto fail; \
    }

#define CHECK(fn, msg) \
    if ((err = (fn)) != IERR_OK ) { \
        err = ionizer_report_error(err, msg, NULL \
            , ion_helper_short_filename(__FILE__), __LINE__); \
        if (err) goto fail; \
    } \
    else if (ion_debug_has_tracing()) { \
        ionizer_report_error(err, msg, NULL \
      , ion_helper_short_filename(__FILE__), __LINE__); \
    }

    

// globals for "real" state we'll be reusing here and there
IZ_GLOBAL ION_STRING         g_symbol_counts_str;
IZ_GLOBAL hSYMTAB            g_hsymtab IZ_INITTO(NULL);
IZ_GLOBAL hSYMTAB            g_writer_hsymtab IZ_INITTO(NULL);
IZ_GLOBAL hCATALOG           g_hcatalog IZ_INITTO(NULL);
IZ_GLOBAL int                g_fldcount IZ_INITTO(0);
IZ_GLOBAL BOOL               g_ion_debug_timer IZ_INITTO(FALSE);
IZ_GLOBAL BOOL               g_ionizer_scan_only IZ_INITTO(FALSE);

IZ_GLOBAL ION_READER_OPTIONS g_reader_options;
IZ_GLOBAL ION_WRITER_OPTIONS g_writer_options;

iERR ionizer_process_filename(char *pathname, hWRITER hwriter, ION_READER_OPTIONS *options);
iERR ionizer_process_one_file(char *filename, hWRITER hwriter, ION_READER_OPTIONS *options);
iERR ionizer_process_input_reader(hREADER hreader, hWRITER hwriter);

iERR ionizer_scan_reader( hREADER hreader );

iERR ionizer_reader_next(hREADER hreader, ION_TYPE *p_value_type);
iERR ionizer_writer_write_all_values(hWRITER hwriter, hREADER hreader);
iERR ionizer_count_types(hREADER hreader, ION_TYPE value_type);
void ionizer_print_count_types(FILE *out);

iERR ionizer_load_symbol_table(void);
iERR ionizer_load_catalog_list(hCATALOG *p_catalog);
iERR ionizer_load_writer_symbol_table(hSYMTAB *p_hsymtab, char *symtab_file_name);

iERR ionizer_new_symbol_table_init(void);
iERR ionizer_new_symbol_table_fill( hREADER hreader );
iERR ionizer_new_symbol_table_write( hWRITER hwriter );
int  ionizer_compare_sids_by_count(const void *psid1, const void *psid2) ;

void ionizer_process_args(int argc, char **argv, char **p_non_argv, int *p_non_argc);
void ionizer_dump_arg_globals(void);
void ionizer_print_help(void);

void ionizer_start_timing(void);
void ionizer_stop_timing(void);

iERR ionizer_report_error(iERR err, const char *msg, hREADER reader, const char *file, int line);


iERR ionizer_reader_open_fstream( 
        FSTREAM_READER_STATE **p_read_helper, 
        FILE *fp_in, 
        ION_READER_OPTIONS *options 
);
iERR ionizer_reader_close_fstream( FSTREAM_READER_STATE *read_helper );

iERR ionizer_writer_open_fstream( 
        FSTREAM_WRITER_STATE **p_write_helper, 
        FILE *fp_out, 
        ION_WRITER_OPTIONS *options);
iERR ionizer_writer_close_fstream( FSTREAM_WRITER_STATE *write_helper );

#ifdef __cplusplus
}
#endif
