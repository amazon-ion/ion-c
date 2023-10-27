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
//  command line program to manipulate ion files
//  this one is used to create and maintain symbol
//  tables from source ion files
//


#define IZ_INITIALIZE
#include "ionsymbols.h"

#include <assert.h>
#include "ion_helpers.h"
#ifdef WIN
#include "io.h"
#endif
#include "options.h"
#include <time.h>

int main(int argc, char **argv)
{
    iENTER;
    //FSTREAM_READER_STATE    *preader_state = NULL;
    //FSTREAM_WRITER_STATE    *pwriter_state = NULL;
    ION_STREAM              *output_stream = NULL;
    ION_STREAM              *input_stream = NULL;
    hWRITER                  hwriter = 0;
    hREADER                  hreader = 0;
    
    ION_STRING               temp;
    char                    *name = NULL;
    ION_TYPE                 t = (ION_TYPE)999;
    int32_t                  symbol_table_count = 0;
    int                      ii, non_argc = 0;
    char                   **non_argv = NULL;


    ION_STRING_INIT(&temp);

    // read the command line for any instruction the caller might have
    non_argv = (char**)malloc(argc * sizeof(char*));
    process_args(argc, argv, non_argv, &non_argc);
    if (g_print_help) SUCCEED();

    // clear the option structs and set them appropriately
    memset(&g_reader_options, 0, sizeof(g_reader_options));
    memset(&g_writer_options, 0, sizeof(g_writer_options));
    g_writer_options.pretty_print     = TRUE;
    
    // set up our debug options
    if (g_dump_args) dump_arg_globals();

    if (g_timer) {
        start_timing();
    }

    // read in the catalog, if there is one
    if (g_catalogs) {
        CHECK( load_catalog_list(&g_hcatalog), "load a catalog file" );
        CHECK(ion_catalog_get_symbol_table_count(g_hcatalog, &symbol_table_count), "get the symbol table count from the loaded catalog");
        fprintf(stderr, "Catalog loaded %d symbol tables\n", symbol_table_count);
    }
    else {
        CHECK( ion_catalog_open( &g_hcatalog ), "open empty catalog" );
    }
    g_reader_options.pcatalog = (ION_CATALOG *)g_hcatalog; // HACK - TODO - HOW SHOULD WE HANDLE THIS? - either the options
    g_writer_options.pcatalog = (ION_CATALOG *)g_hcatalog; // HACK - TODO - should have an hcatalog or we need a h to p fn


    // if we're updating an existing catalog load it
    if (g_update_symtab) {
        // load specified symbol table
        CHECK( load_symbol_table(&g_hsymtab, g_update_symtab), "load symbol table to update");
    }
    else {
        // othwerwise we need to init a symbol table to update
        CHECK( initialize_new_symbol_table(&g_hsymtab), "initialize a new (empty) symbol table to fill" );
    }

    // open the output stream writer attached to stdout
    // TODO: allow caller to specify the output file on the command line
    CHECK( ion_stream_open_stdout( &output_stream ), "ion stream open stdout failed");
    CHECK( ion_writer_open( &hwriter, output_stream, &g_writer_options), "ion writer open failed");
    //CHECK( ion_writer_open_fstream(&pwriter_state, stdout, &g_writer_options), "writer open failed");

    // now, do we process from stdin or from file names on the command line
    if (non_argc > 0) {
        // file names
        for (ii=0; ii<non_argc; ii++) {
            // open our input and output streams (reader and writer)
            CHECK( process_filename(non_argv[ii], &g_reader_options), "process filename failed" );
        }
    }
    else {
        // from stdin
        // open our input and output streams (reader and writer)
        CHECK( ion_stream_open_stdin( &input_stream ), "open stdin as an ION_STREAM failed");
        CHECK( ion_reader_open(&hreader, input_stream, &g_reader_options), "open stdin as an ion reader failed");
        //CHECK( ion_reader_open_fstream(&preader_state, stdin, &g_reader_options), "reader open stdin failed");
        CHECKREADER( process_input_reader(hreader), "process stdin", hreader );
        CHECK( ion_reader_close( hreader ), "closing the ion reader");
        CHECK( ion_stream_close( input_stream), "closing the input stream");
        // CHECK( ion_reader_close_fstream( preader_state ), "closing the reader");
    }

    // if we're emitting a symbol table we need to actually output the 
    // table now (we only collected the symbols from the input sources 
    // during the pass above
    CHECK( symbol_table_write( hwriter ), "emit the new symbol table we've built up" );

    // close up
    //CHECK( ion_writer_close_fstream( pwriter_state ), "closing the writer");
    CHECK( ion_writer_close( hwriter ), "closing the ion writer");
    CHECK( ion_stream_close( output_stream ), "closing the ion output stream");

    if (g_hsymtab)  CHECK(ion_symbol_table_close(g_hsymtab), "closing the temp symbol table");
    if (g_hcatalog) CHECK(ion_catalog_close(g_hcatalog), "closing the catalog");
    SUCCEED();
    
fail:// this is iRETURN expanded so I can set a break point on it
    if (g_debug) {
        fprintf(stderr, "\nionsymbols finished, returning err [%d] = \"%s\", %ld\n", err, ion_error_to_str(err), (intptr_t)t);
    }

    if (g_timer) {
        stop_timing();
    }

    if (non_argv) free(non_argv);
    return err;
}

iERR process_filename(char *pathname, ION_READER_OPTIONS *options)
{
    iENTER;

#ifdef WIN
    OPT_MSG *next, *node, *head = NULL;

    if (g_debug) {
      fprintf(stderr, "expanding wildcard %s\n", pathname);
    }

    if (strchr(pathname, '*') != NULL || strchr(pathname, '?') != NULL) {
        head = opt_decode_wildcards(head, pathname);
        for (node=head; node; node=next) {
            next = node->next; // because we're going to free it before we get to the loop
            err = process_one_file(node->msg, options);
            if (err != IERR_OK) {
                fprintf(stderr, "WARNING: err [%d: '%s'] processing file '%s'\n", err, ion_error_to_str(err), node->msg);
                err = IERR_OK;
            }
            free(node->msg);
            free(node);
        }
        CHECK(err, "ERROR: processing wildcard expanded files\n");
    }
    else {
        CHECK( process_one_file(pathname, options), "process the one file");
    }
#else
    if (g_debug) {
      fprintf(stderr, "DON'T expand wildcard %s\n", pathname);
    }
    CHECK( process_one_file(pathname, options), "process the one file");
#endif

    iRETURN;
}

iERR process_one_file(char *filename, ION_READER_OPTIONS *options)
{
    iENTER;
    //FSTREAM_READER_STATE *preader_state;
    FILE       *fp = NULL;
    ION_STREAM *input_stream = NULL;
    hREADER     hreader = 0;

    if (g_debug) {
        fprintf(stderr, "MSG: processing file '%s'\n", filename);
    }

    fp = fopen(filename, "rb");
    if (!fp) {
        CHECK(IERR_CANT_FIND_FILE, filename);
    }

    // CHECK( ion_reader_open_fstream(&preader_state, fp, options), "reader open 1 file failed");
    CHECK( ion_stream_open_file_in(fp, &input_stream), "open 1 file as stream failed");
    CHECK( ion_reader_open( &hreader, input_stream, options), "ion reader open 1 file failed");

    CHECKREADER( process_input_reader(hreader), "process 1 file", hreader);

    //CHECK( ion_reader_close_fstream( preader_state ), "closing the reader");
    CHECK( ion_reader_close( hreader ), "closing the ion reader");
    CHECK( ion_stream_close( input_stream ), "closing the stream for 1 file");
    // close_fstream closes the file: fclose(fp);

    iRETURN;
}

iERR process_input_reader(hREADER hreader)
{
    iENTER;
    CHECKREADER( symbol_table_fill( hreader ), "ionizer_read_for_symbols", hreader);
    iRETURN;
}


//
// catalog routines - load an additional catalog file (file with multiple shared 
//                    symbol tables in it) also the "load output (writer's)
//                    shared symbol table for binary encoding.
//
iERR load_catalog_list(hCATALOG *p_catalog)
{
    iENTER;
    FILE                 *f_catalog;
    STR_NODE             *str_node;
    //FSTREAM_READER_STATE *reader;
    ION_STREAM           *input_stream;
    hCATALOG              catalog;
    hREADER               hreader;
    ION_TYPE              t;
    BOOL                  is_symtab;
    hSYMTAB               hsymtab;
    ION_STRING            annotion_name;

    ION_STRING_INIT(&annotion_name);
    ion_string_assign_cstr(&annotion_name, ION_SYS_SYMBOL_SHARED_SYMBOL_TABLE, ION_SYS_STRLEN_SHARED_SYMBOL_TABLE);

    CHECK(ion_catalog_open(&catalog), "create empty catalog");

    for (str_node=g_catalogs; str_node; str_node = str_node->next) 
    {
        // open the file, then open the catalog
        f_catalog = fopen(str_node->str, "rb");
        if (!f_catalog) {
            fprintf(stderr, "ERROR: can't open the catalog file: %s\n", str_node->str);
            CHECK(IERR_CANT_FIND_FILE, "can't open the catalog file");
        }
        //CHECK(ion_reader_open_fstream(&reader, f_catalog, NULL), "catalog reader open failed");
        CHECK(ion_stream_open_file_in(f_catalog, &input_stream), "catalog file stream open failed");
        CHECK(ion_reader_open(&hreader, input_stream, NULL), "catalog ion reader open failed");

        // read the file and load all structs with the ion_shared_symbol_table
        // annotation into the hcatalog
        // no longer needed: hreader = reader->hreader; // just because it's shorter
        for (;;) {
            CHECK(ion_reader_next(hreader, &t), "look for the next symtab in the catalog");
            if (t == tid_EOF) break;
            if (t != tid_STRUCT) continue; // symbol tables are always structs we just ignore anything else
            CHECK(ion_reader_has_annotation(hreader, &annotion_name, &is_symtab), "checking annotation");
            if (is_symtab) {
                CHECK(ion_symbol_table_load(hreader, catalog, &hsymtab), "loading a symbol table for the catalog");
                CHECK(ion_catalog_add_symbol_table(catalog, hsymtab), "adding a symbol table to the catalog");
            }
        }
        // CHECK(ion_reader_close_fstream(reader), "closing a catalog reader");
        CHECK(ion_reader_close(hreader), "closing a catalog ion reader");
        CHECK(ion_stream_close(input_stream), "closing a catalog input stream");
        fclose(f_catalog);
    }

    *p_catalog = catalog;
    iRETURN;
}


//
// build symbol table routines, including load, init, fill, update and write
//
iERR initialize_new_symbol_table(hSYMTAB *p_hsymtab)
{
    iENTER;
    hSYMTAB    hsymtab;
    int        version = g_symtab_version;
    ION_STRING temp;

    ION_STRING_INIT(&temp);

    CHECK( ion_symbol_table_open(&hsymtab, NULL), "init empty symbol table" );

    // set the name to either the default or the users suggested name
    ion_string_assign_cstr(&temp, g_symtab_name, strlen(g_symtab_name));
    CHECK( ion_symbol_table_set_name(hsymtab, &temp), "set new symbol table name" );
    
    // set the version number to either 1 or the users suggested value
    if (version == UNDEFINED_VERSION_NUMBER) {
        version = 1;
    }
    CHECK( ion_symbol_table_set_version(hsymtab, version), "set new symbol table version" );

    // return the table handle 
    *p_hsymtab = hsymtab;

    iRETURN;
}

iERR load_symbol_table(hSYMTAB *p_hsymtab, char *symtab_file_name)
{
    iENTER;
    FILE                 *f_symtab;
    //FSTREAM_READER_STATE *reader;
    ION_STREAM           *input_stream;
    hREADER               hreader;
    hSYMTAB               hsymtab;
    ION_TYPE              type;
    int                   version;
    BOOL                  update_version = FALSE;
    ION_STRING            temp;

    ION_STRING_INIT(&temp);

    // open the file, then open the catalog
    f_symtab = fopen(symtab_file_name, "rb");
    if (!f_symtab) {
        fprintf(stderr, "ERROR: can't open the catalog file: %s\n", symtab_file_name);
        CHECK(IERR_CANT_FIND_FILE, "can't open the catalog file");
    }
    //CHECK(ion_reader_open_fstream(&reader, f_symtab, &g_reader_options), "symbol table reader open failed");
    CHECK(ion_stream_open_file_in(f_symtab, &input_stream), "symbol table input stream open failed");
    CHECK(ion_reader_open(&hreader, input_stream, &g_reader_options), "symbol table ion reader open failed");

    CHECK(ion_reader_next(hreader, &type), "reading to the first value in the symbol table file");

    // hack to skip a leading $ion_1_0 - if there's a symbol first we'll just ignore it
    if (type == tid_SYMBOL) {
        CHECK(ion_reader_next(hreader, &type), "reading to the first value in the symbol table file");
    }
    if (type != tid_STRUCT) {
        fprintf(stderr, "ERROR - specified symbol table file doesn't start with a struct\n");
        CHECK(IERR_INVALID_SYMBOL_TABLE, "specified symbol table file MUST start with a struct");
    }

    // load it
    CHECK(ion_symbol_table_load(hreader, g_hcatalog, &hsymtab), "loading a symbol table for the catalog");

    // close up
    //CHECK(ion_reader_close_fstream(reader), "closing a catalog reader");
    CHECK(ion_reader_close(hreader), "closing a catalog ion reader");
    CHECK(ion_stream_close(input_stream), "closing a catalog input stream");
    fclose(f_symtab);

    // first we get the current version number because we might
    // want to step on it if the user overrode the name and version
    // on the command line
    CHECK( ion_symbol_table_get_version(hsymtab, &version), "get previous symbol table version" );

    // update the name, if necessary (i.e. requested by the command line flag)
    if (g_symtab_name != UNDEFINED_SYMTAB_NAME) {
        // the user overrode the name on the command line so we'll change
        // the symbol table name
        ion_string_assign_cstr(&temp, g_symtab_name, strlen(g_symtab_name));
        CHECK( ion_symbol_table_set_name(hsymtab, &temp), "set (override) the symbol table name" );

        // if we've overridden the name then we either user the command line
        // version or don't do anything (should be set the version to 1?)
        if (g_symtab_version != UNDEFINED_VERSION_NUMBER) {
            version = g_symtab_version;
            update_version = TRUE;
        }
    }
    else {
        // if we don't override the name then we either use the command line
        // version or increment the current version number otherwise
        if (g_symtab_version != UNDEFINED_VERSION_NUMBER) {
            version = g_symtab_version;
        }
        else {
            version++;
        }
        update_version = TRUE;
    }

    // update the version number if the name wasn't changed
    if (update_version) {
        CHECK( ion_symbol_table_set_version(hsymtab, version), "set (update) the symbol table version" );
    }

    // return the table handle 
    *p_hsymtab = hsymtab;

    iRETURN;
}

iERR symbol_table_fill( hREADER hreader )
{
    iENTER;
    ION_TYPE   type;
    ION_STRING str;
    int        ii, count;
    SID        sid;
    BOOL       has_annotations, is_null;
    int        next_counter = 0;  // just to track our progress, esp while debugging
    
    ION_STRING_INIT(&str);
    for (;;) {
        CHECKREADER(ion_reader_next(hreader, &type), "reader next", hreader);
        if (type == tid_EOF) break;
        next_counter++;

        // try for a field name
        CHECKREADER(ion_reader_get_field_name(hreader, &str), "get field name", hreader);
        if (!ION_STRING_IS_NULL(&str)) {
            CHECKREADER(ion_symbol_table_add_symbol(g_hsymtab, &str, &sid), "add fieldname", hreader);
        }

        // check for and read any annotations
        CHECKREADER(ion_reader_has_any_annotations(hreader, &has_annotations),"any annotations", hreader);
        if (has_annotations) {
            CHECKREADER(ion_reader_get_annotation_count(hreader, &count), "annotation count", hreader);
            for (ii=0; ii<count; ii++) {
                CHECKREADER(ion_reader_get_an_annotation(hreader, ii, &str), "get annotation", hreader);
                CHECKREADER(ion_symbol_table_add_symbol(g_hsymtab, &str, &sid), "add annotation", hreader);
            }
        }

        CHECKREADER(ion_reader_is_null(hreader, &is_null), "is null", hreader);
        if (is_null) continue; // no data in a null

        if (type == tid_SYMBOL) {
            CHECKREADER(ion_reader_read_string(hreader, &str), "read symbol type", hreader);
            CHECKREADER(ion_symbol_table_add_symbol(g_hsymtab, &str, &sid), "add symbol scalar", hreader);
        }
        else if (type == tid_LIST || type == tid_STRUCT || type == tid_SEXP) {
            CHECKREADER(ion_reader_step_in(hreader), "step in", hreader);
            CHECKREADER(symbol_table_fill(hreader), "read for symbols", hreader);
            CHECKREADER(ion_reader_step_out(hreader), "step out", hreader);
        }
    }

    iRETURN;
}

iERR symbol_table_write( hWRITER hwriter )
{
    iENTER;
    ION_STRING  temp;
    ION_SYMBOL *sym;
    SID         ii, sorted_count, sid, max_sid;
    SID        *sidlist = NULL;


    if (!g_hsymtab) {
        fprintf(stderr, "No symbol table found to write.");
        SUCCEED();
    }

    // first we prefix this with an ion version marker
    CHECK(ion_writer_write_symbol(hwriter, &ION_SYMBOL_VTM_STRING), "write ion version marker");

    // now we write it out one way or another
    CHECK(ion_symbol_table_unload(g_hsymtab, hwriter), "writing the symbol table");

    if (g_include_counts) {
        // we write the counts after the symbol table in an annotated struct
        IONCHECK(ion_writer_add_annotation(hwriter, &g_symbol_counts_str));

        IONCHECK(ion_writer_start_container(hwriter, tid_LIST));
        IONCHECK(ion_symbol_table_get_max_sid(g_hsymtab, &max_sid));

        // allocate and initialize the array of sid so we can sort the symbols
        // in the list.  This isn't particularly efficient since we'll call 
        // ion_symbol_table_get_local_symbol twice (for the two symbols we're
        // comparing) to get the symbol data to compare with.  We could use
        // a struct (or an array of symbols structs) to do this, but this was
        // easy and it's not exactly a critical path operation.
        sidlist = (int *)malloc(max_sid * sizeof(int));
        if (!sidlist) {
            FAILWITH(IERR_NO_MEMORY);
        }
        sorted_count = 0;
        for (sid=1; sid<=max_sid; sid++) {
            IONCHECK(ion_symbol_table_get_local_symbol(g_hsymtab, sid, &sym)); // get symbol to see if it's a local symbol
            if (!sym) continue;
            sidlist[sorted_count++] = sid;
        }

        // we might want to sort by name some time later (perhaps even an option)
        qsort(sidlist, (size_t)sorted_count, sizeof(int), compare_sids_by_count);

        // now, with the sidlist as a level of indirection, we read the symbols
        // again in order to output the symbols with their use counts
        for (ii=0; ii<sorted_count; ii++) {
            sid = sidlist[ii];
            IONCHECK(ion_symbol_table_get_local_symbol(g_hsymtab, sid, &sym)); // get symbols by sid, iterate from 1 to max_sid - returns all symbols
            if (!sym) continue; // not all symbols are local to this table
            IONCHECK(ion_writer_start_container(hwriter, tid_STRUCT));
            IONCHECK(ion_writer_write_field_name(hwriter, ion_string_assign_cstr(&temp, "sid", 3)));
            IONCHECK(ion_writer_write_int(hwriter, sym->sid));
            IONCHECK(ion_writer_write_field_name(hwriter, ion_string_assign_cstr(&temp, "name", 4)));
            IONCHECK(ion_writer_write_string(hwriter, &sym->value));
            IONCHECK(ion_writer_write_field_name(hwriter, ion_string_assign_cstr(&temp, "count", 5)));
            IONCHECK(ion_writer_write_int(hwriter, sym->add_count));
            IONCHECK(ion_writer_finish_container(hwriter));
        }
        IONCHECK(ion_writer_finish_container(hwriter));
    }

    iRETURN;
}

int compare_sids_by_count(const void *psid1, const void *psid2) 
{
    iERR        err1, err2;
    SID         sid1 = *(int *)psid1;
    SID         sid2 = *(int *)psid2;
    int         len, ret = 0;
    ION_SYMBOL *sym1, *sym2;

    err1 = ion_symbol_table_get_local_symbol(g_hsymtab, sid1, &sym1);
    err2 = ion_symbol_table_get_local_symbol(g_hsymtab, sid2, &sym2);
    if (err1 || err2) return (sid1 - sid2);  // we can't really do anything with these errors but we want the sort to complete rationally

    // first by count
    ret = sym1->add_count - sym2->add_count;
    ret = -ret; // large numbers at the top, a descending sort
    if (!ret) {
        // then by string name (compare to short length first)
        len = sym1->value.length;
        if (len < sym2->value.length) len = sym2->value.length;
        ret = memcmp(sym1->value.value, sym2->value.value, len);
        if (!ret) {
            // since prefix chars are the same, the longer string is larger
            ret = sym1->value.length - sym2->value.length;
        }
    }
    return ret;
}

// option processing 

// TODO: why does the MSC stdlib version page fault??
//char *strncpy(char *dst, char *src, int len)
//{
//    char *ret = dst;
//    while (len-- && *src) {
//        *dst++ = *src++;
//    }
//    return ret;
//}

iERR report_error(iERR err, const char *msg, hREADER hreader, const char *file, int line)
{
    int64_t bytes;
    int32_t lines, offset;

    fprintf(stderr
            ,"ERROR::{msg:\"%s\", err:[%d, \"%s\"], file:\"%s\", line:%d}\n"
            ,msg
            ,err
            ,ion_error_to_str(err)
            ,file
            ,line
    );
    if (hreader) {
        ion_reader_get_position(hreader, &bytes, &lines, &offset);
        fprintf(stderr
                ,"FILE_POSITION::{line:%d, offset:%d, bytes_read:%ld}\n"
                ,lines
                ,offset
                ,bytes
        );
    }

    return err;
}

clock_t start_time;

void start_timing(void)
{
    start_time = clock();
    fprintf(stderr, "Timer started: %ld (ticks)\n", (long)start_time);
}

void stop_timing(void)
{
    clock_t finish_time = clock();
    long    elapsed = finish_time - start_time;
    
    fprintf(stderr, "Timer stopped: %ld (ticks)\n", (long)finish_time);
    fprintf(stderr, "      elapsed: %ld (ticks)\n", elapsed);
    fprintf(stderr, "      elapsed: %f (secs)\n", (double)elapsed/(double)CLOCKS_PER_SEC);
}
