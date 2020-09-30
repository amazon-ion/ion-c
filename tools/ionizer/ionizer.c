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
//

#define IZ_INITIALIZE
#include "ionizer.h"
#include "options.h"
#ifdef WIN
#ifdef SCAN_STREAM_ENABLED
// note that this takes more work than just defining SCAN_STREAM_ENABLED
#include "ionc\ion_stream.h"
#endif
#include "io.h"
#endif
#include  <assert.h>
#include  <time.h>

#define IONIZER_DEFAULT_BUFFER_SIZE 67108860

int main(int argc, char **argv)
{
    iENTER;
    FSTREAM_READER_STATE    *preader_state = NULL;
    FSTREAM_WRITER_STATE    *pwriter_state = NULL;
    ION_STRING               temp;
    char                    *name = NULL;
    ION_TYPE                 t = (ION_TYPE)999;
    int32_t                  symbol_table_count = 0;
    int                      ii, non_argc = 0;
    char                   **non_argv = NULL;


    ION_STRING_INIT(&temp);

    // read the command line for any instruction the caller might have
    non_argv = (char**)malloc(argc * sizeof(char*));
    ionizer_process_args(argc, argv, non_argv, &non_argc);
    if (g_ionizer_print_help) SUCCEED();

    // clear the option structs and set them appropriately
    memset(&g_reader_options, 0, sizeof(g_reader_options));
    memset(&g_writer_options, 0, sizeof(g_writer_options));
    g_writer_options.pretty_print     = g_ionizer_pretty;
    g_writer_options.output_as_binary = g_ionizer_write_binary;
    g_writer_options.escape_all_non_ascii = g_ionizer_ascii_only;

    g_reader_options.symbol_threshold = IONIZER_DEFAULT_BUFFER_SIZE;
    g_reader_options.max_annotation_buffered = IONIZER_DEFAULT_BUFFER_SIZE;
    g_reader_options.allocation_page_size = IONIZER_DEFAULT_BUFFER_SIZE;
    g_reader_options.user_value_threshold = IONIZER_DEFAULT_BUFFER_SIZE;
    g_reader_options.chunk_threshold = IONIZER_DEFAULT_BUFFER_SIZE;
    g_writer_options.allocation_page_size = IONIZER_DEFAULT_BUFFER_SIZE;
    g_writer_options.temp_buffer_size = IONIZER_DEFAULT_BUFFER_SIZE;

    if (g_ionizer_pool_page_size > 0) {
        ion_initialize_page_pool(g_ionizer_pool_page_size, 10);
    }

    // set up our debug options
    if (g_ionizer_flush_each) g_writer_options.flush_every_value = TRUE;
    if (g_ionizer_dump_args)  ionizer_dump_arg_globals();

    if (g_ion_debug_timer) {
        ionizer_start_timing();
    }

    // read in the catalog, if there is one
    if (g_ionizer_catalogs) {
        
        CHECK(ionizer_load_catalog_list(&g_hcatalog), "load a catalog file");
        g_reader_options.pcatalog = (ION_CATALOG *)g_hcatalog; // HACK - TODO - HOW SHOULD WE HANDLE THIS?
        g_writer_options.pcatalog = (ION_CATALOG *)g_hcatalog; // HACK - TODO - HOW SHOULD WE HANDLE THIS?
        CHECK(ion_catalog_get_symbol_table_count(g_hcatalog, &symbol_table_count), "get the symbol table count from the loaded catalog");
        fprintf(stderr, "Catalog loaded %d symbol tables\n", symbol_table_count);
    }

    // if there's a "writer symbol table" specified look in the catalog for it
    if (g_ionizer_writer_symtab && *g_ionizer_writer_symtab) {
        CHECK(ionizer_load_symbol_table(), "load the symbol table from the catalog or a file");
    }

    // open the output stream writer attached to stdout
    // TODO: allow caller to specify the output file on the command line
    CHECK( ionizer_writer_open_fstream(&pwriter_state, stdout, &g_writer_options), "writer open failed");

    // now, do we process from stdin or from file names on the command line
    if (non_argc > 0) {
        // file names
        for (ii=0; ii<non_argc; ii++) {
            // open our input and output streams (reader and writer)
            CHECK(ionizer_process_filename(non_argv[ii], pwriter_state->hwriter, &g_reader_options), "process filename failed");
        }
    }
    else {
        // from stdin
        // open our input and output streams (reader and writer)
        CHECK( ionizer_reader_open_fstream(&preader_state, stdin, &g_reader_options), "reader open stdin failed");
        CHECKREADER( ionizer_process_input_reader(preader_state->hreader, pwriter_state->hwriter), "process stdin", preader_state->hreader);
        CHECK( ionizer_reader_close_fstream( preader_state ), "closing the reader");
    }

    // if we're emitting a symbol table we need to actually output the 
    // table now (we only collected the symbols from the input sources 
    // during the pass above
    if (g_ionizer_write_symtab) {
        CHECK( ionizer_new_symbol_table_write( pwriter_state->hwriter ), "emit the new symbol table we've built up");
    }

    // if the user wanted us to print out the type counts we do it here
    if (g_ionizer_include_type_counts) {
        ionizer_print_count_types(stdout);
    }

    // close up
    CHECK( ionizer_writer_close_fstream( pwriter_state ), "closing the writer");
    if (g_hsymtab)  CHECK(ion_symbol_table_close(g_hsymtab), "closing the temp symbol table");
    if (g_hcatalog) CHECK(ion_catalog_close(g_hcatalog), "closing the catalog");
    SUCCEED();
    
fail:// this is iRETURN expanded so I can set a break point on it
    if (g_ionizer_debug) {
        fprintf(stderr, "\nionizer finished, returning err [%d] = \"%s\", %d\n", err, ion_error_to_str(err), (intptr_t)t);
    }

    if (g_ion_debug_timer) {
        ionizer_stop_timing();
    }

    ion_writer_options_close_shared_imports(&g_writer_options);
    if (non_argv) free(non_argv);
    return err;
}

iERR ionizer_process_filename(char *pathname, hWRITER hwriter, ION_READER_OPTIONS *options)
{
    iENTER;

#ifdef WIN
    OPT_MSG *next, *node, *head = NULL;

    if (g_ionizer_debug) {
      fprintf(stderr, "expanding wildcard %s\n", pathname);
    }

    if (strchr(pathname, '*') != NULL || strchr(pathname, '?') != NULL) {
        head = opt_decode_wildcards(head, pathname);
        for (node=head; node; node=next) {
            next = node->next; // because we're going to free it before we get to the loop
            err = ionizer_process_one_file(node->msg, hwriter, options);
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
        CHECK(ionizer_process_one_file(pathname, hwriter, options), "process the one file");
    }
#else
    if (g_ionizer_debug) {
      fprintf(stderr, "DON'T expand wildcard %s\n", pathname);
    }
    CHECK(ionizer_process_one_file(pathname, hwriter, options), "process the one file");
#endif

    iRETURN;
}


#ifdef SCAN_STREAM_ENABLED
iERR scan_stream(ION_INPUT_STREAM *fstream)
{
    iENTER;
    int c, bytes = 0;

    for (;;) {
        ION_READ2( fstream, c );
        if (c < 0) break;
        bytes++;
    }
    printf("bytes read = %d\n", fstream->bytes_read);

    SUCCEED();
    iRETURN;
}
#endif



iERR ionizer_process_one_file(char *filename, hWRITER hwriter, ION_READER_OPTIONS *options)
{
    iENTER;
    FSTREAM_READER_STATE *preader_state;
    FILE *fp;

    if (g_ionizer_debug) {
        fprintf(stderr, "MSG: processing file '%s'\n", filename);
    }

    fp = fopen(filename, "rb");
    if (!fp) {
        CHECK(IERR_CANT_FIND_FILE, filename);
    }

    CHECK( ionizer_reader_open_fstream(&preader_state, fp, options), "reader open 1 file failed");

    CHECKREADER( ionizer_process_input_reader(preader_state->hreader, hwriter), "process 1 file", preader_state->hreader);

    CHECK( ionizer_reader_close_fstream( preader_state ), "closing the reader");
    // close_fstream closes the file: fclose(fp);

    iRETURN;
}


iERR ionizer_process_input_reader(hREADER hreader, hWRITER hwriter)
{
    iENTER;
    // execute one of our three options - symbol table or ion output or no output (scan only)
    if (g_ionizer_scan_only) {
        CHECKREADER( ionizer_scan_reader( hreader ), "start scanning reader", hreader);
    }
    else if (g_ionizer_write_symtab) {
        CHECK( ionizer_new_symbol_table_init(), "init new symbol table");
        CHECKREADER( ionizer_new_symbol_table_fill( hreader ), "ionizer_read_for_symbols", hreader);
    }
    else {
        CHECKREADER( ionizer_writer_write_all_values( hwriter, hreader), "writing from reader", hreader);
    }
    iRETURN;
}

#ifdef DEBUG
static long g_scan_counter = 0;
static long g_scan_stack[100];
static int  g_scan_stack_top = 0;
#define INTERESTING_SCAN_VALUE 0x3b
#endif
iERR ionizer_scan_reader( hREADER hreader )
{
    iENTER;
    ION_TYPE   type;
    BOOL       is_null;
    
    for (;;) {
        #ifdef DEBUG
        g_scan_counter++;
        if (g_scan_counter == (INTERESTING_SCAN_VALUE)) {
            g_scan_counter = (g_scan_counter++) - 1;  // something to set a break point on
        }
        #endif

        CHECKREADER(ionizer_reader_next(hreader, &type), "reader next", hreader);
        if (type == tid_EOF) break;

        CHECKREADER(ion_reader_is_null(hreader, &is_null), "is null", hreader);
        if (is_null) continue; // no data in a null

        if (type == tid_LIST || type == tid_STRUCT || type == tid_SEXP) {
            #ifdef DEBUG
            g_scan_stack[g_scan_stack_top++] = g_scan_counter;
            #endif
            CHECKREADER(ion_reader_step_in(hreader), "step in", hreader);
            CHECKREADER(ionizer_scan_reader(hreader), "read for scan only", hreader);
            CHECKREADER(ion_reader_step_out(hreader), "step out", hreader);
            #ifdef DEBUG
            g_scan_stack[g_scan_stack_top] = g_scan_counter; // so we can peek back to see when we stepped out (as well as in)
            g_scan_stack_top--;
            #endif
        }
    }

    iRETURN;
}

iERR ionizer_reader_next(hREADER hreader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_TYPE t;

    CHECKREADER(ion_reader_next(hreader, &t),"read next helper that counts values", hreader);
    if (g_ionizer_include_type_counts) {
        CHECKREADER(ionizer_count_types(hreader, t), "count types", hreader);
    }
    *p_value_type = t;

    iRETURN;
}

iERR ionizer_writer_write_all_values( hWRITER hwriter, hREADER hreader )
{
    iENTER;
    ION_TYPE t;
    BOOL     is_null;

    if (g_ionizer_include_type_counts == FALSE) {
        // just pass through and get out of the way
        CHECKREADER( ion_writer_write_all_values( hwriter, hreader ), "write all values, don't count", hreader );
    }
    else {
       // if we have to count, we have to traverse the values here
        for (;;) {
            CHECKREADER(ionizer_reader_next(hreader, &t),"read next helper that counts values", hreader);

            switch(ION_TYPE_INT(t)) {

            case (intptr_t)tid_EOF:
                SUCCEED(); // we're done at this level

            case (intptr_t)tid_NULL:      // also handled in the previous switch
            case (intptr_t)tid_BOOL:
            case (intptr_t)tid_INT:
            case (intptr_t)tid_FLOAT:
            case (intptr_t)tid_DECIMAL:
            case (intptr_t)tid_TIMESTAMP:
            case (intptr_t)tid_STRING:
            case (intptr_t)tid_SYMBOL:
            case (intptr_t)tid_CLOB:
            case (intptr_t)tid_BLOB:
                CHECKREADER(ion_writer_write_one_value(hwriter, hreader), "write one value", hreader);
                break;

            case (intptr_t)tid_STRUCT:
            case (intptr_t)tid_LIST:
            case (intptr_t)tid_SEXP:
            CHECKREADER(ion_reader_is_null(hreader, &is_null), "is null", hreader);
            if (is_null) {   
                CHECKREADER(ion_writer_write_one_value(hwriter, hreader), "write one value", hreader);
            }
            else {
                    CHECKREADER(ion_reader_step_in(hreader), "step in", hreader);
                    CHECKREADER(ionizer_writer_write_all_values(hwriter, hreader), "write container", hreader);
                    CHECKREADER(ion_reader_step_out(hreader), "step out", hreader);
                    SUCCEED();
            }

            default:
                FAILWITH(IERR_INVALID_STATE);
            }
        }
    }

    iRETURN;
}

static long g_eof_count = 0;
static long g_annotation_count = 0;
static long g_fieldname_count = 0;

#define ION_TYPE_INDEX(t) ((int)(((intptr_t)(t)) >> 8)) /* 0x0-0xD, or 0-14 */
#define ION_TYPE_INDEX_MAX 14
#define ION_TYPE_INVALID   14

// +1 to convert from max to count, +1 to allow for invalid
static long g_null_counts[ION_TYPE_INDEX_MAX + 2] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
static long g_non_null_counts[ION_TYPE_INDEX_MAX + 2] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
static char *g_type_names[ION_TYPE_INDEX_MAX + 2] = {
    "null",
    "bool",
    "int",
    "float",
    "decimal",
    "timestamp",
    "symbol",
    "string",
    "clob",
    "blob",
    "struct",
    "list",
    "sexp",
    "invalid_type"
};


iERR ionizer_count_types(hREADER hreader, ION_TYPE value_type)
{
    iENTER;
    BOOL has_annotations, is_null;
    SIZE annotation_count;
    ION_STRING fieldname;

    ION_STRING_INIT(&fieldname);

    if (value_type == tid_EOF) {
        g_eof_count++;
        SUCCEED();
    }
    CHECKREADER(ion_reader_has_any_annotations(hreader, &has_annotations), "has annoations", hreader);
    if (has_annotations) {
        CHECKREADER(ion_reader_get_annotation_count(hreader, &annotation_count), "annoation count", hreader);
        g_annotation_count += annotation_count;
    }
    CHECKREADER(ion_reader_get_field_name(hreader, &fieldname), "get fieldname", hreader);
    if (ION_STRING_IS_NULL(&fieldname) == FALSE) {
        g_fieldname_count++;
    }
    CHECKREADER(ion_reader_is_null(hreader, &is_null), "is null value", hreader);
    if (is_null) {
        switch(ION_TYPE_INT(value_type)) {
        case tid_NULL_INT:
        case tid_BOOL_INT:
        case tid_INT_INT:
        case tid_FLOAT_INT:
        case tid_DECIMAL_INT:
        case tid_TIMESTAMP_INT:
        case tid_SYMBOL_INT:
        case tid_STRING_INT:
        case tid_CLOB_INT:
        case tid_BLOB_INT:
        case tid_STRUCT_INT:
        case tid_LIST_INT:
        case tid_SEXP_INT:
            g_null_counts[ION_TYPE_INDEX(value_type)]++;
            break;
        default:
            g_null_counts[ION_TYPE_INVALID]++;
            break;
        }
    }
    else {
        switch(ION_TYPE_INT(value_type)) {
        case tid_NULL_INT:
        case tid_BOOL_INT:
        case tid_INT_INT:
        case tid_FLOAT_INT:
        case tid_DECIMAL_INT:
        case tid_TIMESTAMP_INT:
        case tid_SYMBOL_INT:
        case tid_STRING_INT:
        case tid_CLOB_INT:
        case tid_BLOB_INT:
        case tid_STRUCT_INT:
        case tid_LIST_INT:
        case tid_SEXP_INT:
            g_non_null_counts[ION_TYPE_INDEX(value_type)]++;
            break;
        default:
            g_non_null_counts[ION_TYPE_INVALID]++;
            break;
        }
    }
    iRETURN;
}

void ionizer_print_count_types(FILE *out) {
    int t;
    long total_values = 0;

    for (t = 0; t<ION_TYPE_INDEX_MAX+1; t++) {
        total_values += g_non_null_counts[t];
        total_values += g_null_counts[t];
    }

    fprintf(out, "\n");
    fprintf(out, "//\n");
    fprintf(out, "// count of value types encountered\n");
    fprintf(out, "//\n");
    fprintf(out, "{\n");
    fprintf(out, "  total_values: %ld\n", total_values);
    fprintf(out, "  field_names: %ld\n", g_fieldname_count);
    fprintf(out, "  annotations: %ld\n", g_annotation_count);
    fprintf(out, "  by_type: [\n");
    fprintf(out, "    // type, non_null, null\n");
  //fprintf(out, "    [ %s, %ld, %ld ]\n",
    for (t = 0; t<ION_TYPE_INDEX_MAX+1; t++) {
        if (g_non_null_counts[t] || g_null_counts[t]) {
            fprintf(out, "    [ %s, %ld, %ld ]\n",
                g_type_names[t],
                g_non_null_counts[t],
                g_null_counts[t]
            );
        }
    }
    fprintf(out, "  ]\n");
    fprintf(out, "}\n");
}


//
// catalog routines - load an additional catalog file (file with multiple shared 
//                    symbol tables in it) also the "load output (writer's)
//                    shared symbol table for binary encoding.
//
iERR ionizer_load_catalog_list(hCATALOG *p_catalog)
{
    iENTER;
    FILE                 *f_catalog;
    IONIZER_STR_NODE     *str_node;
    FSTREAM_READER_STATE *reader;
    hCATALOG              catalog;
    hREADER               hreader;
    ION_TYPE              t;
    BOOL                  is_symtab;
    hSYMTAB               hsymtab;
    ION_STRING            annotion_name;

    ION_STRING_INIT(&annotion_name);
    ion_string_assign_cstr(&annotion_name, ION_SYS_SYMBOL_SHARED_SYMBOL_TABLE, ION_SYS_STRLEN_SHARED_SYMBOL_TABLE);

    CHECK(ion_catalog_open(&catalog), "create empty catalog");

    for (str_node=g_ionizer_catalogs; str_node; str_node = str_node->next) 
    {
        // open the file, then open the catalog
        f_catalog = fopen(str_node->str, "rb");
        if (!f_catalog) {
            fprintf(stderr, "ERROR: can't open the catalog file: %s\n", str_node->str);
            FAILWITH(IERR_CANT_FIND_FILE);
        }
        CHECK(ionizer_reader_open_fstream(&reader, f_catalog, &g_reader_options), "catalog reader open failed");

        // read the file and load all structs with the ion_shared_symbol_table
        // annotation into the hcatalog
        hreader = reader->hreader; // just because it's shorter
        for (;;) {
            CHECK(ionizer_reader_next(hreader, &t), "look for the next symtab in the catalog");
            if (t == tid_EOF) break;
            if (t != tid_STRUCT) continue; // symbol tables are always structs
            CHECK(ion_reader_has_annotation(hreader, &annotion_name, &is_symtab), "checking annotation");
            if (is_symtab) {
                CHECK(ion_symbol_table_load(hreader, catalog, &hsymtab), "loading a symbol table for the catalog");
                CHECK(ion_catalog_add_symbol_table(catalog, hsymtab), "adding a symbol table to the catalog");
            }
        }
        CHECK(ionizer_reader_close_fstream(reader), "closing a catalog reader");
        // ion_reader_close_fstream closes this already: fclose(f_catalog);
    }

    *p_catalog = catalog;
    iRETURN;
}

iERR ionizer_load_writer_symbol_table(hSYMTAB *p_hsymtab, char *symtab_file_name)
{
    iENTER;
    FILE                 *f_symtab;
    FSTREAM_READER_STATE *reader;
    hSYMTAB               hsymtab;
    ION_TYPE              type;

    // open the file, then open the catalog
    f_symtab = fopen(symtab_file_name, "rb");
    if (!f_symtab) {
        fprintf(stderr, "ERROR: can't open the catalog file: %s\n", symtab_file_name);
        FAILWITH(IERR_CANT_FIND_FILE);
    }
    CHECK(ionizer_reader_open_fstream(&reader, f_symtab, NULL), "symbol table reader open failed");

    CHECK(ionizer_reader_next(reader->hreader, &type), "reading to the first value in the symbol table file");
    if (type != tid_STRUCT) {
        fprintf(stderr, "ERROR - specified symbol table file doesn't start with a struct\n");
        FAILWITH(IERR_INVALID_SYMBOL_TABLE);
    }

    // load it
    CHECK(ion_symbol_table_load(reader->hreader, g_hcatalog, &hsymtab), "loading a symbol table for the catalog");

    // close up
    CHECK(ionizer_reader_close_fstream(reader), "closing a catalog reader");
    fclose(f_symtab);

    // return the table handle 
    *p_hsymtab = hsymtab;

    iRETURN;
}



//
// build symbol table routines, including load, init, fill, update and write
//
iERR ionizer_load_symbol_table(void)
{
    iENTER;
    ION_STRING temp;
    SIZE       len;

    if (!g_writer_options.pcatalog) {
        ionizer_print_help();
        fprintf(stderr, "\nthe writer symbol table (%s) must be included in the catalog\n", g_ionizer_writer_symtab);
    }
    len = _ion_strnlen(g_ionizer_writer_symtab, (size_t)1024);    // TODO, this is wrong-ish, what the max size of an command line arg?
    ion_string_assign_cstr(&temp, g_ionizer_writer_symtab, len);
    CHECK(ion_catalog_find_best_match(g_hcatalog
            , &temp
            , 0
            ,&g_writer_hsymtab
    ), "finding the writer symbol table in the catalog");

    // if we didn't find the symbol name in the catalog try to open it in a file
    if (!g_writer_hsymtab) {
        CHECK(ionizer_load_writer_symbol_table(&g_writer_hsymtab, g_ionizer_writer_symtab), "load writer symbol table from a file");
    }
    if (!g_writer_hsymtab) {
        fprintf(stderr, "ERROR - couldn't find the symbol table \"%s\" in the catalog or as a file\n", g_ionizer_writer_symtab);
        FAILWITH(IERR_CANT_FIND_FILE);
    }
    IONCHECK(ion_writer_options_initialize_shared_imports(&g_writer_options));
    IONCHECK(ion_writer_options_add_shared_imports_symbol_tables(&g_writer_options, &g_writer_hsymtab, 1));

    iRETURN;
}

iERR ionizer_new_symbol_table_init(void)
{
    iENTER;
    ION_SYMBOL_TABLE_TYPE type;
    ION_STRING  name;
    ION_SYMBOL *sym;
    SID         sid, max_id, new_sid;

    ION_STRING_INIT(&name);

    // check to see if this has already been initialized
    // and if so, bail (there's only 1 output symtab to build)
    if (g_hsymtab) SUCCEED();

    ION_STRING_INIT(&g_symbol_counts_str);
    g_symbol_counts_str.value = (BYTE *)"symbol_counts";
    g_symbol_counts_str.length = (int32_t)strlen((char *)g_symbol_counts_str.value);
    CHECK( ion_symbol_table_open(&g_hsymtab, NULL), "ion_symbol_table_open (alloc symtab)");

    if (g_writer_hsymtab) {
        // was:CHECK( ion_symbol_table_import_symbol_table( g_hsymtab, g_writer_hsymtab ), "import the writer symbol table into the output table" );

        CHECK( ion_symbol_table_get_type(g_writer_hsymtab, &type), "get type");
        switch (type) {
        case ist_SHARED:
            if (!g_ionizer_output_symtab_name || !g_ionizer_output_symtab_name[0]) {
                CHECK( ion_symbol_table_get_name(g_writer_hsymtab, &name), "get prev name");
                g_ionizer_output_symtab_name = ion_string_strdup(&name);
            }
            if (g_ionizer_symtab_version < 1) {
                CHECK( ion_symbol_table_get_version(g_writer_hsymtab, &g_ionizer_symtab_version), "get prev version");
                g_ionizer_symtab_version++; // and add one, this is a new version after all
            }
            break;
        default:
            break;
        }

        CHECK( ion_symbol_table_get_max_sid(g_writer_hsymtab, &max_id), "get max id for interation");
        for (sid = 1; sid <= max_id; sid++) {
            IONCHECK(ion_symbol_table_get_local_symbol(g_writer_hsymtab, sid, &sym)); // get symbols by sid, iterate from 1 to max_sid - returns all symbols
            if (!sym) continue; // not all symbols are local to this table
            CHECK( ion_symbol_table_add_symbol( g_hsymtab, &sym->value, &new_sid ), "add old to new");
        }
    }

    iRETURN;
}

iERR ionizer_new_symbol_table_fill( hREADER hreader )
{
    iENTER;
    ION_TYPE   type;
    ION_STRING str;
    int        ii, count;
    SID        sid;
    BOOL       has_annotations, is_null;
    
    ION_STRING_INIT(&str);
    for (;;) {
        CHECKREADER(ionizer_reader_next(hreader, &type), "reader next", hreader);
        if (type == tid_EOF) break;

        g_fldcount++; // for debugging

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
            CHECKREADER(ionizer_new_symbol_table_fill(hreader), "read for symbols", hreader);
            CHECKREADER(ion_reader_step_out(hreader), "step out", hreader);
        }
    }

    iRETURN;
}

iERR ionizer_new_symbol_table_write( hWRITER hwriter )
{
    iENTER;
    ION_STRING  name, temp;
    ION_SYMBOL *sym;
    SID         ii, sorted_count, sid, max_sid;
    SID        *sidlist = NULL;
    int         version, len;

    if (!g_hsymtab) {
        fprintf(stderr, "No symbols found.");
        SUCCEED();
    }

    // if we got a name on the command line we'll make
    // this a named shared symbol table
    if (g_ionizer_output_symtab_name && g_ionizer_output_symtab_name[0]) {
        assert(sizeof(g_ionizer_output_symtab_name) < INT32_MAX);
        len = (int)strlen(g_ionizer_output_symtab_name);
        IONCHECK(ion_symbol_table_set_name(g_hsymtab, ion_string_assign_cstr(&name, g_ionizer_output_symtab_name, len)));
        
        // we always put a version if the user specified a name
        version = 1;
        if (g_ionizer_symtab_version > 0) version = g_ionizer_symtab_version;
        IONCHECK(ion_symbol_table_set_version(g_hsymtab, version));
    }
    else if (g_ionizer_symtab_version > 0) {
        fprintf(stderr, "WARNING - symbol table version ignored without a symbol table name\n");
    }

    // now we write it out one way or another
    CHECK(ion_symbol_table_unload(g_hsymtab, hwriter), "writing the symbol table");

    if (g_ionizer_include_counts) {
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
        qsort(sidlist, (size_t)sorted_count, sizeof(int), ionizer_compare_sids_by_count);

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

int ionizer_compare_sids_by_count(const void *psid1, const void *psid2) 
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
char *ionizer_strncpy(char *dst, char *src, int len)
{
    char *ret = dst;
    while (len-- && *src) {
        *dst++ = *src++;
    }
    return ret;
}

iERR ionizer_clear_reported_error(void)
{
    g_ionizer_error_reported = FALSE;
    return IERR_OK;
}

iERR ionizer_report_error(iERR err, const char *msg, hREADER hreader, const char *file, int line)
{
    int64_t bytes;
    int32_t lines, offset;

    if (g_ionizer_error_reported == FALSE) {
        
        file = ion_helper_short_filename(file);

        if (err) {
            fprintf(stderr
                    ,"\nERROR::{msg:\"%s\", err:[%d, \"%s\"], file:\"%s\", line:%d}\n"
                    ,msg
                    ,err
                    ,ion_error_to_str(err)
                    ,file
                    ,line
            );
        }
        else {
            fprintf(stderr
                    ,"\npassed::{msg:\"%s\", file:\"%s\", line:%d}"
                    ,msg
                    ,file
                    ,line
            );
        }
        if (hreader) {
            ion_reader_get_position(hreader, &bytes, &lines, &offset);
            fprintf(stderr
                    ,"FILE_POSITION::{line:%d, offset:%d, bytes_read:%ld}"
                    ,lines
                    ,offset
                    ,bytes
            );
        }
        fprintf(stderr, "\n");

        g_ionizer_error_reported = TRUE;
    }
    return err;
}

clock_t start_time;

void ionizer_start_timing(void)
{
    start_time = clock();
    fprintf(stderr, "Timer started: %ld (ticks)\n", (long)start_time);
}

void ionizer_stop_timing(void)
{
    clock_t finish_time = clock();
    long    elapsed = finish_time - start_time;
    
    fprintf(stderr, "Timer stopped: %ld (ticks)\n", (long)finish_time);
    fprintf(stderr, "      elapsed: %ld (ticks)\n", elapsed);
    fprintf(stderr, "      elapsed: %f (secs)\n", (double)elapsed/(double)CLOCKS_PER_SEC);
}
