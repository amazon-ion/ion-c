/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
// header for quick test for ion.c
//

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <string.h>
#include <assert.h>

#include <ion.h>
#include <ion_platform_config.h>

#define MAX_TEMP_STRING (8*1024) /* used for files full path name */
#define BUF_SIZE (128*1024)

#ifdef ION_PLATFORM_WINDOWS
#define PATH_SEPARATOR_CHAR '\\'
#else
#define PATH_SEPARATOR_CHAR '/'
#endif

// 
//  globals cheap version
#ifdef TESTER_INIT
decContext deccontext;
#else
extern decContext deccontext;
#endif

extern BOOL  g_no_print;        // defined in tester.c
extern long  g_value_count;
extern long  g_failure_count;
extern char *g_iontests_path;   /** Path to IonTests package directory, so we can locate test data. */


void ion_helper_breakpoint(void);

//
// helpers
//

#define IONDEBUG(x,m) {                         \
        err = (x);                              \
        if (err) {                              \
            if (g_no_print == FALSE) {          \
                printf(" ERROR! AT %s line %d, err (%d) = %s\n", __FILE__, __LINE__, err, ion_error_to_str(err)); \
                printf("           %s\n", m);   \
            }                                   \
            ion_helper_breakpoint();            \
            goto fail;                          \
        }                                       \
        g_value_count++;                        \
    } err
/* required blank line to close iondebug() */

#define IONDEBUG2(x,m1,m2)                      \
    {                                           \
        err = (x);                              \
        if (err) {                              \
            if (g_no_print == FALSE) {          \
                printf(" ERROR! AT %s line %d, err (%d) = %s\n", __FILE__, __LINE__, err, ion_error_to_str(err)); \
                printf("           %s %s\n", m1, m2); \
            }                                   \
            ion_helper_breakpoint();            \
            goto fail;                          \
        }                                       \
        g_value_count++;                        \
    } err
/* required blank line to close iondebug() */


#define IF_PRINT if (g_no_print == FALSE) printf

//
//
//
typedef struct _test_file {
    FILE *in;
    int   block_size;
    BYTE *buffer;
} TEST_FILE;

typedef enum _test_file_type 
{
    FILETYPE_BINARY = 1,
    FILETYPE_TEXT,
    FILETYPE_ALL

} TEST_FILE_TYPE;

typedef iERR (*LOOP_FN)(hREADER r);

/** Returns true if file should be included. */
typedef BOOL (*FILE_PREDICATE_FN)(char* filename);


//
// tester.c
//

iERR test_reader_one_file               (char *filename);
iERR test_text_reader_good_files        (void);
iERR test_binary_reader_good_files      (void);
iERR test_reader_good_files             (void);
iERR test_text_reader_1                 (void);

iERR test_reader_read_all               (hREADER hreader);
iERR test_reader_read_value_print       (hREADER hreader, ION_TYPE t, char *type_name);
iERR test_reader_read_value_no_print    (hREADER hreader, ION_TYPE t);

iERR test_writer_1                      (void); // simple test writing to a text output stream
iERR test_writer_2                      (void); // clone of test_writer_1, but writing to a binary stream

char *test_get_type_name                (ION_TYPE t);
char *test_make_cstr                    (iSTRING str);
char *test_make_fullpathname            (char *localpathname);
char *test_concat_filename              (char *dst, int dstlen, char *path, char *name);
char *double_to_cstr                    (double d);

#define ION_BINARY_DUMP_LINE_LENGTH      10
#define ION_BINARY_DUMP_LINES_PER_BLOCK   0

void ion_dump_binary                    (BYTE *buffer, SIZE buffer_length);
void ion_dump_binary_line1              (int offset, BYTE *buffer, SIZE line_length);
void ion_dump_binary_line2              (int offset, BYTE *buffer, SIZE line_length);
void ion_dump_binary_line3              (int offset, BYTE *buffer, SIZE line_length);

SIZE copy_to_bytes                      (const char *image, BYTE *bytes, int limit);


//
// testfiles.c
//

iERR visit_files                                (char *parentpath
                                               , char *filename
                                               , FILE_PREDICATE_FN file_predicate
                                               , LOOP_FN fn
                                               , char *fn_name
);

iERR test_bulk_files                            (void);
iERR test_good_files                            (TEST_FILE_TYPE filetype);
iERR test_good_file                             (hREADER reader);
iERR test_bad_files                             ();
iERR test_bad_file                              (hREADER reader);
iERR test_loop                                  (char *path, char **filenames, LOOP_FN fn, char *fn_name);
iERR test_one_file                              (char *filepath, LOOP_FN fn, char *fn_name);
iERR test_reader_one_file_file_only             (char *filename);

// test_internal.c
iERR test_reader_read_all_just_file             (hREADER hreader);
iERR test_reader_read_all_just_file_byte_by_byte(hREADER hreader);
iERR test_stream_handler                        (struct _ion_user_stream *pstream);

iERR test_step_out_nested_s_expressions();
iERR test_reader_bad_files();
iERR test_open_string_reader(char* ion_text, hREADER* reader);
iERR expect_string(char* expected, hREADER reader);
iERR expect_type(ION_TYPE actual, ION_TYPE expected);
