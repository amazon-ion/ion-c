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
// Ion Writer interfaces. Takes a byte buffer and length which
// will contain the text or binary content, returns handle to a writer.
//

#include "ion_internal.h"
#include "ion_writer_impl.h"

#define IONCLOSEpWRITER(x)   { if (x != NULL)  { UPDATEERROR(_ion_writer_close_helper(x)); x = NULL;}}

iERR ion_writer_options_initialize_shared_imports(ION_WRITER_OPTIONS *options)
{
    iENTER;
    hOWNER owner = ion_alloc_owner(sizeof(int)); // Dummy allocation to create an owning pool for the collection.
    if (owner == NULL) FAILWITH(IERR_NO_MEMORY);
    _ion_collection_initialize(owner, &options->encoding_psymbol_table, sizeof(ION_SYMBOL_TABLE_IMPORT));
    iRETURN;
}

iERR ion_writer_options_add_shared_imports(ION_WRITER_OPTIONS *options, ION_COLLECTION *imports)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *user_import, *option_import;
    ION_COLLECTION_CURSOR import_cursor;
    ASSERT(options != NULL);
    ASSERT(imports != NULL);

    ION_COLLECTION_OPEN(imports, import_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(import_cursor, user_import);
        if (!user_import) break;
        option_import = _ion_collection_append(&options->encoding_psymbol_table);
        if (!option_import) FAILWITH(IERR_NO_MEMORY);
        memset(option_import, 0, sizeof(ION_SYMBOL_TABLE_IMPORT));
        if (options->encoding_psymbol_table._owner == imports->_owner) {
            IONCHECK(_ion_symbol_table_local_import_copy_same_owner(NULL, option_import, user_import, sizeof(ION_SYMBOL_TABLE_IMPORT)));
        }
        else {
            IONCHECK(_ion_symbol_table_local_import_copy_new_owner(options->encoding_psymbol_table._owner,
                                                                   option_import, user_import, sizeof(ION_SYMBOL_TABLE_IMPORT)));
        }
        ASSERT(option_import->shared_symbol_table == user_import->shared_symbol_table);
        if (option_import->shared_symbol_table) {
            if (option_import->shared_symbol_table->owner != options->encoding_psymbol_table._owner) {
                IONCHECK(ion_symbol_table_clone_with_owner(option_import->shared_symbol_table,
                                                           &option_import->shared_symbol_table,
                                                           options->encoding_psymbol_table._owner));
            }
        }
    }
    ION_COLLECTION_CLOSE(import_cursor);
    iRETURN;
}

iERR ion_writer_options_add_shared_imports_symbol_tables(ION_WRITER_OPTIONS *options, ION_SYMBOL_TABLE **imports, SIZE imports_count)
{
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import;
    int i;
    for (i = 0; i < imports_count; i++) {
        import = _ion_collection_append(&options->encoding_psymbol_table);
        if (!import) FAILWITH(IERR_NO_MEMORY);
        if (imports[i] == NULL) FAILWITH(IERR_INVALID_ARG);
        memset(import, 0, sizeof(ION_SYMBOL_TABLE_IMPORT));
        import->descriptor.max_id = imports[i]->max_id;
        import->descriptor.version = imports[i]->version;
        IONCHECK(ion_string_copy_to_owner(options->encoding_psymbol_table._owner, &import->descriptor.name, &imports[i]->name));
        if (imports[i]->owner != options->encoding_psymbol_table._owner) {
            IONCHECK(ion_symbol_table_clone_with_owner(imports[i],
                                                       &import->shared_symbol_table,
                                                       options->encoding_psymbol_table._owner));
        }
        else {
            import->shared_symbol_table = imports[i];
        }

    }
    iRETURN;
}

iERR ion_writer_options_close_shared_imports(ION_WRITER_OPTIONS *options)
{
    if (options->encoding_psymbol_table._owner) {
        ion_free_owner(options->encoding_psymbol_table._owner);
        options->encoding_psymbol_table._owner = NULL;
    }
    return IERR_OK;
}

iERR ion_writer_open_buffer(
         hWRITER *p_hwriter
        ,BYTE *buffer
        ,SIZE buf_length
        ,ION_WRITER_OPTIONS *p_options // NULL == all defaults
) {
    iENTER;
    ION_WRITER *pwriter = NULL;

    if (!p_hwriter)     FAILWITH(IERR_INVALID_ARG);
    if (!buffer)        FAILWITH(IERR_INVALID_ARG);
    if (buf_length < 0) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_open_buffer_helper(&pwriter, buffer, buf_length, p_options));
    *p_hwriter = PTR_TO_HANDLE(pwriter);

    iRETURN;
}

iERR _ion_writer_open_buffer_helper(
         ION_WRITER **p_pwriter
        ,BYTE *buffer
        ,SIZE buf_length
        ,ION_WRITER_OPTIONS *p_options // NULL == all defaults
)
{
    iENTER;
    ION_WRITER *pwriter = NULL;
    ION_STREAM *stream = NULL;

    ASSERT(p_pwriter);
    ASSERT(buffer);
    ASSERT(buf_length >= 0);

    IONCHECK(ion_stream_open_buffer(buffer, buf_length, buf_length, FALSE, &stream));
    IONCHECK(_ion_writer_open_helper(&pwriter, stream, p_options));
    pwriter->writer_owns_stream = TRUE;

    *p_pwriter = pwriter;
    return err;
//  iRETURN;

fail:
    IONCLOSEpWRITER(pwriter);

    *p_pwriter = pwriter;
    return err;
}

iERR ion_writer_open_stream(hWRITER *p_hwriter
                                ,ION_STREAM_HANDLER fn_output_handler
                                ,void *handler_state
                                ,ION_WRITER_OPTIONS *p_options
) {
    iENTER;
    ION_WRITER *pwriter = NULL;
    ION_STREAM *pstream = NULL;
    if (!p_hwriter) FAILWITH(IERR_INVALID_ARG);
    IONCHECK(ion_stream_open_handler_out( fn_output_handler, handler_state, &pstream ));
    IONCHECK(_ion_writer_open_helper(&pwriter, pstream, p_options));
    *p_hwriter = PTR_TO_HANDLE(pwriter);
    iRETURN;
}
iERR ion_writer_open(
        hWRITER *p_hwriter
        ,ION_STREAM *stream
        ,ION_WRITER_OPTIONS *p_options // NULL == all defaults
) {
    iENTER;
    ION_WRITER *pwriter = NULL;

    if (!p_hwriter)       FAILWITH(IERR_INVALID_ARG);
    
    IONCHECK(_ion_writer_open_helper(&pwriter, stream, p_options));
    *p_hwriter = PTR_TO_HANDLE(pwriter);

    iRETURN;
}

iERR _ion_writer_initialize_local_symbol_table(ION_WRITER *pwriter)
{
    iENTER;
    ION_SYMBOL_TABLE *psymtab, *system;
    ION_SYMBOL_TABLE_IMPORT *import;
    ION_COLLECTION_CURSOR import_cursor;

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    ASSERT( pwriter->symbol_table == NULL || pwriter->symbol_table == system );

    IONCHECK(_ion_symbol_table_open_helper(&psymtab, pwriter->_temp_entity_pool, system));

    ION_COLLECTION_OPEN(&pwriter->_imported_symbol_tables, import_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(import_cursor, import);
        if (!import) break;
        if (ION_STRING_EQUALS(&ION_SYMBOL_ION_STRING, &import->descriptor.name)) {
            // Do nothing; every local symbol table implicitly imports the system symbol table.
        }
        else {
            IONCHECK(_ion_symbol_table_import_symbol_table_helper(psymtab, import->shared_symbol_table,
                                                                  &import->descriptor.name, import->descriptor.version,
                                                                  import->descriptor.max_id));
        }
    }
    ION_COLLECTION_CLOSE(import_cursor);

    pwriter->symbol_table = psymtab;
    pwriter->_local_symbol_table = TRUE;
    iRETURN;
}

iERR _ion_writer_initialize_imports(ION_WRITER *pwriter) {
    iENTER;
    ION_SYMBOL_TABLE_IMPORT     *import;
    ION_COLLECTION_CURSOR       import_cursor;
    ION_COPY_FN                 copy_fn;
    copy_fn = pwriter->options.encoding_psymbol_table._owner != pwriter ? _ion_symbol_table_local_import_copy_new_owner
                                                                        : _ion_symbol_table_local_import_copy_same_owner;
    IONCHECK(_ion_collection_copy(&pwriter->_imported_symbol_tables, &pwriter->options.encoding_psymbol_table, copy_fn, pwriter));
    ION_COLLECTION_OPEN(&pwriter->_imported_symbol_tables, import_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(import_cursor, import);
        if (!import) break;
        if (import->shared_symbol_table) {
            IONCHECK(ion_symbol_table_clone_with_owner(import->shared_symbol_table, &import->shared_symbol_table,
                                                       pwriter));
        }
    }
    ION_COLLECTION_CLOSE(import_cursor);

    iRETURN;
}
    
iERR _ion_writer_open_helper(ION_WRITER **p_pwriter, ION_STREAM *stream, ION_WRITER_OPTIONS *p_options)
{
    iENTER;
    ION_WRITER         *pwriter = NULL;
    ION_OBJ_TYPE        writer_type;

    pwriter = ion_alloc_owner(sizeof(ION_WRITER));
    if (!pwriter) FAILWITH(IERR_NO_MEMORY);
    *p_pwriter = pwriter;

    memset(pwriter, 0, sizeof(ION_WRITER));
    pwriter->type = ion_type_unknown_writer;
    pwriter->_has_local_symbols = TRUE;

    // if we have options copy them here so we have our own copy
    ASSERT(sizeof(ION_WRITER_OPTIONS) == sizeof(pwriter->options));
    if (p_options) {
        memcpy(&(pwriter->options), p_options, sizeof(ION_WRITER_OPTIONS));
    }
    else {
        memset(&(pwriter->options), 0, sizeof(ION_WRITER_OPTIONS));
    }
    _ion_writer_initialize_option_defaults(&(pwriter->options));

    // initialize decimal context
    if (pwriter->options.decimal_context == NULL) {
        decContextDefault(&pwriter->deccontext, DEC_INIT_DECQUAD);
    }
    else {
        memcpy(&pwriter->deccontext, pwriter->options.decimal_context, sizeof(decContext));
    }

    pwriter->pcatalog = pwriter->options.pcatalog;

    // our default is unknown, so if the option says "binary" we need to 
    // change the underlying writer's obj type we'll use the presence of 
    // unknown to trigger approprate initialization (unknown indicating 
    // we're not properly initialized yet.
    if (p_options && p_options->output_as_binary) {
        writer_type = ion_type_binary_writer;
    }
    else {
        writer_type = ion_type_text_writer;
    }

    IONCHECK(ion_temp_buffer_init(pwriter, &pwriter->temp_buffer, ION_WRITER_TEMP_BUFFER_DEFAULT));

    // allocate a temp pool we can reset from time to time
    IONCHECK( _ion_writer_allocate_temp_pool( pwriter ));

    // set the output stream
    pwriter->output = stream;

    // initialize the typed portion of the writer (based on
    // the type of the requested writer
    pwriter->type = writer_type;
    switch (writer_type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_initialize(pwriter));
        IONCHECK(_ion_writer_text_initialize_stack(pwriter));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_initialize(pwriter));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    _ion_collection_initialize(pwriter, &pwriter->_imported_symbol_tables, sizeof(ION_SYMBOL_TABLE_IMPORT));

    if (!ION_COLLECTION_IS_EMPTY(&pwriter->options.encoding_psymbol_table)) {
        IONCHECK(_ion_writer_initialize_imports(pwriter));
        IONCHECK(_ion_writer_initialize_local_symbol_table(pwriter));
    }

    return err;

    //    iRETURN;
fail:
    IONCLOSEpWRITER(pwriter);
    *p_pwriter = NULL;
    return err;
}

void _ion_writer_initialize_option_defaults(ION_WRITER_OPTIONS *p_options)
{
    ASSERT(p_options);

    // most options are set to the correct default by memset(0)
    // but for size values we'll want to replace the 0's with
    // actual values so we don't have to test them all the time

    // sets the default indent amount (default is 2)
    if (!p_options->indent_size) {
        p_options->indent_size = 2;
    }

    // the temp buffer is used to hold temp strings (etc) default is 1024
    if (!p_options->temp_buffer_size) {
        p_options->temp_buffer_size = ION_WRITER_TEMP_BUFFER_DEFAULT;
    }

    // the max container depth defaults to 10
    if (!p_options->max_container_depth) {
        p_options->max_container_depth = DEFAULT_WRITER_STACK_DEPTH;
    }

    // the max number of annotations on 1 value, defaults to 10
    if (!p_options->max_annotation_count) {
        p_options->max_annotation_count = DEFAULT_ANNOTATION_LIMIT;
    }

    // memory is allocated in pages owned by the primary entities it's default size is 4096
    if (!p_options->allocation_page_size) {
        p_options->allocation_page_size = DEFAULT_BLOCK_SIZE;
    }

    return;
}


iERR ion_writer_get_depth(hWRITER hwriter, SIZE *p_depth)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_depth) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_depth_helper(pwriter, p_depth));

    iRETURN;
}

iERR _ion_writer_get_depth_helper(ION_WRITER *pwriter, SIZE *p_depth)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(p_depth);

    *p_depth = pwriter->depth;
    SUCCEED();

    iRETURN;
}

iERR ion_writer_set_temp_size(hWRITER hwriter, SIZE size_of_temp_space)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_set_temp_size_helper(pwriter, size_of_temp_space));

    iRETURN;
}

iERR _ion_writer_set_temp_size_helper(ION_WRITER *pwriter, SIZE size_of_temp_space)
{
    iENTER;

    ASSERT(pwriter);

    // TODO: unwrap this, we have to initialize the temp buffer and use
    //       it during the initial initialization, so here it's too late
    //       to shrink it.  We can make it larger, but not smaller.
    //       and we just waste the initial allocation as well.
    if (size_of_temp_space < ION_WRITER_TEMP_BUFFER_DEFAULT) SUCCEED();

    IONCHECK(ion_temp_buffer_init(pwriter, &pwriter->temp_buffer, size_of_temp_space));

    iRETURN;
}

iERR ion_writer_set_max_annotation_count(hWRITER hwriter, SIZE annotation_limit)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (annotation_limit < 0) FAILWITH(IERR_INVALID_ARG);

    // we should only initialize this once! (or something's wrong somewhere)
    if (pwriter->annotations) FAILWITH(IERR_INVALID_STATE);

    IONCHECK(_ion_writer_set_max_annotation_count_helper(pwriter, annotation_limit));

    iRETURN;
}

iERR _ion_writer_set_max_annotation_count_helper(ION_WRITER *pwriter, SIZE annotation_limit)
{
    iENTER;
    void *ptemp = NULL;

    ASSERT(pwriter);
    ASSERT(annotation_limit >= 0);
    ASSERT(!pwriter->annotations);

    IONCHECK(ion_temp_buffer_alloc(&pwriter->temp_buffer, annotation_limit * sizeof(ION_STRING), &ptemp));

    pwriter->annotation_count = annotation_limit;
    pwriter->annotation_curr = 0;
    pwriter->annotations = (ION_STRING *)ptemp;
    pwriter->annotation_sids = (SID *)ptemp;    // id's are always smaller than the ion_string struct
                                                // and only one of these can be in use at a time

    iRETURN;
}

iERR ion_writer_set_catalog(hWRITER hwriter, hCATALOG hcatalog)
{
    iENTER;
    ION_CATALOG *pcatalog;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    if (hcatalog) {
        pcatalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);
    }
    else {
        pcatalog = NULL;
    }

    IONCHECK(_ion_writer_set_catalog_helper(pwriter, pcatalog));

    iRETURN;
}

iERR _ion_writer_set_catalog_helper(ION_WRITER *pwriter, ION_CATALOG *pcatalog)
{
    iENTER;
    
    ASSERT(pwriter);

    // TODO: it really seems like this shouldn't be done willy-nilly
    //       some state adjustement and state validation should take
    //       place right about here ... hmmm.
    pwriter->pcatalog = pcatalog;
    SUCCEED();

    iRETURN;
}

iERR ion_writer_get_catalog(hWRITER hwriter, hCATALOG *p_hcatalog)
{
    iENTER;
    ION_WRITER *pwriter;
    ION_CATALOG *pcatalog;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_hcatalog) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_catalog_helper(pwriter, &pcatalog));
    *p_hcatalog = PTR_TO_HANDLE(pcatalog);

    iRETURN;
}

iERR _ion_writer_get_catalog_helper(ION_WRITER *pwriter, ION_CATALOG **p_pcatalog)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(p_pcatalog);

    // TODO: should we create a catalog if one doesn't already exist?

    *p_pcatalog = pwriter->pcatalog;
    SUCCEED();

    iRETURN;
}

iERR ion_writer_set_symbol_table(hWRITER hwriter, hSYMTAB hsymtab)
{
    iENTER;
    ION_WRITER *pwriter;
    ION_SYMBOL_TABLE *psymtab;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!hsymtab) FAILWITH(IERR_INVALID_ARG);
    psymtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);

    IONCHECK(_ion_writer_set_symbol_table_helper(pwriter, psymtab));

    iRETURN;
}

iERR _ion_writer_set_symbol_table_helper(ION_WRITER *pwriter, ION_SYMBOL_TABLE *psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE_TYPE   table_type = ist_EMPTY;
    ION_SYMBOL_TABLE       *plocal, *system;
    
    ASSERT(pwriter);
    ASSERT(psymtab);

    IONCHECK(_ion_symbol_table_get_type_helper(psymtab, &table_type));
    switch (table_type) {
    default:
    case ist_EMPTY:
    case ist_SYSTEM:
        FAILWITH(IERR_INVALID_SYMBOL_TABLE);
        break;
    case ist_SHARED:
        // create a local symbol table and add the requested (shared) symbol table as an import
        IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
        IONCHECK(_ion_symbol_table_open_helper(&plocal, pwriter->_temp_entity_pool, system));
        IONCHECK(_ion_symbol_table_local_incorporate_symbols(plocal, psymtab, psymtab->max_id));
        psymtab = plocal;
        break;
    case ist_LOCAL:
        // just checking - just set it
        break;
    }

    // before assigning a new symtab, free a local one if we allocated it
    // in reality this should only happen in the case of a local table
    IONCHECK( _ion_writer_free_local_symbol_table( pwriter ));

    pwriter->symbol_table = psymtab;

    iRETURN;
}


iERR ion_writer_get_symbol_table(hWRITER hwriter, hSYMTAB *p_hsymtab)
{
    iENTER;
    ION_WRITER *pwriter;
    ION_SYMBOL_TABLE *psymtab = NULL;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_hsymtab) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_symbol_table_helper(pwriter, &psymtab));

    *p_hsymtab = PTR_TO_HANDLE(psymtab);

    iRETURN;
}

iERR _ion_writer_get_symbol_table_helper(ION_WRITER *pwriter, ION_SYMBOL_TABLE **p_psymtab)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(p_psymtab);

    if (!pwriter->symbol_table)
    {
        IONCHECK(_ion_symbol_table_get_system_symbol_helper(&pwriter->symbol_table, ION_SYSTEM_VERSION));
    }

    *p_psymtab = pwriter->symbol_table;
    SUCCEED();

    iRETURN;
}

iERR _ion_writer_get_local_symbol_table(ION_WRITER *pwriter, ION_SYMBOL_TABLE **p_psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE *psymtab;

    ASSERT(pwriter);
    ASSERT(p_psymtab);

    if (!pwriter->symbol_table)
    {
        IONCHECK(_ion_symbol_table_get_system_symbol_helper(&psymtab, ION_SYSTEM_VERSION));
    }
    else {
        psymtab = pwriter->symbol_table;
    }
    *p_psymtab = psymtab;


    iRETURN;
}


iERR ion_writer_write_field_name(hWRITER hwriter, iSTRING name)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_field_name_helper(pwriter, name));

    iRETURN;
}

iERR _ion_writer_write_field_name_helper(ION_WRITER *pwriter, ION_STRING *name)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(name);
    ASSERT(!ION_STRING_IS_NULL(name));
    ASSERT(name->length >= 0);

    // we make our own copy of the string so we don't depend on the callers copy

    // TODO: should make this the callers responsibility?  since internally 
    // we'll often have strings (perhaps always) that we already own
    // IONCHECK(ion_strdup(pwriter, &pwriter->field_name, name));
    ION_STRING_ASSIGN(&pwriter->field_name, name);

    // clear the sid since we've set the string
    pwriter->field_name_sid = UNKNOWN_SID;

    // remember what sort of field name we've been told about
    pwriter->field_name_type = tid_STRING;

    SUCCEED();

    iRETURN;
}

iERR ion_writer_write_field_sid(hWRITER hwriter, SID sid)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (sid < 0)  FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_write_field_sid_helper(pwriter, sid));

    iRETURN;
}

iERR _ion_writer_write_field_sid_helper(ION_WRITER *pwriter, SID sid)
{
    iENTER;

    ASSERT(pwriter);

    IONCHECK(_ion_writer_validate_symbol_id(pwriter, sid));

    pwriter->field_name_sid = sid;

    // clear the string if we set the sid
    ION_STRING_INIT(&pwriter->field_name);

    // remember what sort of field name we've been told about
    pwriter->field_name_type = tid_INT;

    SUCCEED();

    iRETURN;
}

iERR ion_writer_add_annotation(hWRITER hwriter, iSTRING annotation)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!annotation || !annotation->value) FAILWITH(IERR_INVALID_ARG);
    if (annotation->length < 0)  FAILWITH(IERR_INVALID_ARG);

    // if the caller (or someone) has started with int
    // annotations (sids) then they have to stick with them
    // no mix and match here
    if (pwriter->annotations_type == tid_INT) FAILWITH(IERR_INVALID_STATE);


    IONCHECK(_ion_writer_add_annotation_helper(pwriter, annotation));

    iRETURN;
}

iERR _ion_writer_add_annotation_helper(ION_WRITER *pwriter, ION_STRING *annotation)
{
    iENTER;

    ASSERT(pwriter)
    ASSERT(annotation);
    ASSERT(!ION_STRING_IS_NULL(annotation));
    ASSERT(annotation->length >= 0);
    ASSERT(pwriter->annotations_type != tid_INT);

    if (!pwriter->annotations) {
        IONCHECK(_ion_writer_set_max_annotation_count_helper(pwriter, DEFAULT_ANNOTATION_LIMIT));
    }
    else if (pwriter->annotation_curr >= pwriter->annotation_count) FAILWITH(IERR_TOO_MANY_ANNOTATIONS);

    // so we'll be handling string annoations here, not sids
    pwriter->annotations_type = tid_STRING;

    IONCHECK(ion_strdup(pwriter->_temp_entity_pool, &pwriter->annotations[pwriter->annotation_curr], annotation));

    pwriter->annotation_curr++;

    iRETURN;
}

iERR ion_writer_add_annotation_sid(hWRITER hwriter, SID sid)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    // if the caller (or someone) has started with int
    // annotations (sids) then they have to stick with them
    // no mix and match here
    if (pwriter->annotations_type == tid_STRING) FAILWITH(IERR_INVALID_STATE);

    IONCHECK(_ion_writer_add_annotation_sid_helper(pwriter, sid));


    iRETURN;
}

iERR _ion_writer_add_annotation_sid_helper(ION_WRITER *pwriter, SID sid)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(pwriter->annotations_type != tid_STRING);

    IONCHECK(_ion_writer_validate_symbol_id(pwriter, sid));

    if (!pwriter->annotations) {
        IONCHECK(_ion_writer_set_max_annotation_count_helper(pwriter, DEFAULT_ANNOTATION_LIMIT));
    }
    else if (pwriter->annotation_curr >= pwriter->annotation_count) FAILWITH(IERR_TOO_MANY_ANNOTATIONS);

    // so we'll be handling int (SID annotations here, not strings
    pwriter->annotations_type = tid_INT;

    pwriter->annotation_sids[pwriter->annotation_curr] = sid;
    pwriter->annotation_curr++;

    iRETURN;
}

iERR ion_writer_write_annotations(hWRITER hwriter, iSTRING *p_annotations, int32_t count)
{
    iENTER;
    int32_t ii;
    ION_STRING *pstr;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (count < 0)      FAILWITH(IERR_INVALID_ARG);
    if (count > 0 && !p_annotations) FAILWITH(IERR_INVALID_ARG);

    if (pwriter->annotations_type == tid_INT) FAILWITH(IERR_INVALID_STATE);

    // let's just make sure the caller is passing in strings
    // that at least have a chance of being valie annotations
    for (ii = 0; ii<count; ii++) {
        pstr = p_annotations[ii];
        if (!pstr) FAILWITH(IERR_INVALID_ARG);
        if (ION_STRING_IS_NULL(pstr)) FAILWITH(IERR_INVALID_ARG);
        if (pstr->length < 0) FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_writer_write_annotations_helper(pwriter, p_annotations, count));

    iRETURN;
}

iERR _ion_writer_write_annotations_helper(ION_WRITER *pwriter, ION_STRING **p_annotations, int32_t count)
{
    iENTER;
    int32_t ii;

    ASSERT(pwriter);
    // if the count is positive there better be some annotations on hand
    // but if the count is 0 then it's ok to not even have an annotation pointer
    // but in any event negative counts are not ok.
    ASSERT((p_annotations != NULL) ? (count > 0) : (count == 0));

    if (!pwriter->annotations) {
        IONCHECK(_ion_writer_set_max_annotation_count_helper(pwriter, DEFAULT_ANNOTATION_LIMIT));
    }
    if (pwriter->annotation_curr + count > pwriter->annotation_count) FAILWITH(IERR_TOO_MANY_ANNOTATIONS);

    // a cheesy way to handle it, but it doesn't seem worth optimizing at this point
    // if we wanted to "assume" the users pointers were stable until we need them
    // later (when we go to write out the annotations) we could save our temp array
    // and hang onto the users, but i'd rather be safer than faster in this case
    for (ii = 0; ii<count; ii++) {
        IONCHECK(_ion_writer_add_annotation_helper(pwriter, p_annotations[ii]));
    }

    iRETURN;
}

iERR ion_writer_write_annotation_sids(hWRITER hwriter, int32_t *p_sids, int32_t count)
{
    iENTER;
    int32_t      ii;
    SID         sid;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (count < 0)      FAILWITH(IERR_INVALID_ARG);
    if (!p_sids)        FAILWITH(IERR_INVALID_ARG);
    if (pwriter->annotations_type == tid_STRING) FAILWITH(IERR_INVALID_STATE);

    // Verify that all of the SIDs are valid before adding any of them; otherwise, reject the whole set to avoid partial
    // success.
    for (ii = 0; ii<count; ii++) {
        sid = p_sids[ii];
        IONCHECK(_ion_writer_validate_symbol_id(pwriter, sid));
    }

    IONCHECK(_ion_writer_write_annotation_sids_helper(pwriter, p_sids, count));

    iRETURN;
}

iERR _ion_writer_write_annotation_sids_helper(ION_WRITER *pwriter, int32_t *p_sids, int32_t count)
{
    iENTER;
    int32_t ii;

    ASSERT(pwriter);
    ASSERT(count >= 0);
    ASSERT(p_sids);

    if (!pwriter->annotations) {
        IONCHECK(_ion_writer_set_max_annotation_count_helper(pwriter, DEFAULT_ANNOTATION_LIMIT));
    }
    if (pwriter->annotation_curr + count > pwriter->annotation_count) FAILWITH(IERR_TOO_MANY_ANNOTATIONS);

    // a cheesy way to handle it, but it doesn't seem worth optimizing at this point
    // if we wanted to "assume" the users pointers were stable until we need them
    // later (when we go to write out the annotations) we could save our temp array
    // and hang onto the users, but i'd rather be safer than faster in this case
    for (ii = 0; ii<count; ii++) {
        IONCHECK(_ion_writer_add_annotation_sid_helper(pwriter, p_sids[ii]));
    }

    iRETURN;
}

iERR ion_writer_write_null(hWRITER hwriter)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    // yeah, we skip 1 layer of indirection here since it's otherwise silly
    IONCHECK(_ion_writer_write_typed_null_helper(pwriter, tid_NULL));

    iRETURN;
}
// there is no _ion_writer_write_null_helper, just call _ion_writer_write_typed_null_helper

iERR ion_writer_write_typed_null(hWRITER hwriter, ION_TYPE type)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (type < tid_NULL || type > tid_SEXP) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_write_typed_null_helper(pwriter, type));

    iRETURN;
}

iERR _ion_writer_write_typed_null_helper(ION_WRITER *pwriter, ION_TYPE type)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_typed_null(pwriter, type));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, type));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_bool(hWRITER hwriter, BOOL value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_bool_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_bool_helper(ION_WRITER *pwriter, BOOL value)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_bool(pwriter, value));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_bool(pwriter, value));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}


iERR ion_writer_write_int32(hWRITER hwriter, int32_t value)
{
    return ion_writer_write_int64(hwriter, value);
}

iERR _ion_writer_write_int32_helper(ION_WRITER *pwriter, int32_t value)
{
    iENTER;
    IONCHECK(_ion_writer_write_int64_helper(pwriter, value));
    iRETURN;
}

iERR ion_writer_write_int(hWRITER hwriter, int value)
{
    iENTER;
    int64_t int64Value = value;
    if (value == int64Value) {
        IONCHECK(ion_writer_write_int64(hwriter, value));
    }
    else {
        // IONC-36: IONC does not support read/write integer of greater than 64 bit
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR ion_writer_write_int64(hWRITER hwriter, int64_t value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_int64_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_int64_helper(ION_WRITER *pwriter, int64_t value)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_int64(pwriter, value));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_int64(pwriter, value));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_ion_int(hWRITER hwriter, ION_INT *value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_ion_int_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_ion_int_helper(ION_WRITER *pwriter, ION_INT *value) {
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
        case ion_type_text_writer:
            IONCHECK(_ion_writer_text_write_ion_int(pwriter, value));
            break;
        case ion_type_binary_writer:
            IONCHECK(_ion_writer_binary_write_ion_int(pwriter, value));
            break;
        default:
            FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR _ion_writer_write_mixed_int_helper(ION_WRITER *pwriter, ION_READER *preader)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        if (preader->_int_helper._is_ion_int) {
            IONCHECK(_ion_writer_text_write_ion_int(pwriter, &preader->_int_helper._as_ion_int));
        }
        else {
            IONCHECK(_ion_writer_text_write_int64(pwriter, preader->_int_helper._as_int64));
        }
        break;
    case ion_type_binary_writer:
        if (preader->_int_helper._is_ion_int) {
            IONCHECK(_ion_writer_binary_write_ion_int(pwriter, &preader->_int_helper._as_ion_int));
        }
        else {
            IONCHECK(_ion_writer_binary_write_int64(pwriter, preader->_int_helper._as_int64));
        }
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_long(hWRITER hwriter, long value)
{
    iENTER;
    int64_t int64Value = value;
    if (value == int64Value) {
        IONCHECK(ion_writer_write_int64(hwriter, value));
    }
    else {
        // IONC-36: IONC does not support read/write integer of greater than 64 bit
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR ion_writer_write_double(hWRITER hwriter, double value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_double_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_double_helper(ION_WRITER *pwriter, double value)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_double(pwriter, value));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_double(pwriter, value));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_decimal(hWRITER hwriter, decQuad *value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_decimal_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_decimal_helper(ION_WRITER *pwriter, decQuad *value)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_decimal_quad(pwriter, value));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_decimal_quad(pwriter, value));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_ion_decimal(hWRITER hwriter, ION_DECIMAL *value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_ion_decimal_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_ion_decimal_helper(ION_WRITER *pwriter, ION_DECIMAL *value)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
        case ion_type_text_writer:
            switch(value->type) {
                case ION_DECIMAL_TYPE_QUAD:
                    IONCHECK(_ion_writer_text_write_decimal_quad(pwriter, &value->value.quad_value));
                    break;
                case ION_DECIMAL_TYPE_NUMBER_OWNED:
                case ION_DECIMAL_TYPE_NUMBER:
                    IONCHECK(_ion_writer_text_write_decimal_number(pwriter, value->value.num_value));
                    break;
                default:
                    FAILWITH(IERR_INVALID_STATE);
            }
            break;
        case ion_type_binary_writer:
            switch(value->type) {
                case ION_DECIMAL_TYPE_QUAD:
                    IONCHECK(_ion_writer_binary_write_decimal_quad(pwriter, &value->value.quad_value));
                    break;
                case ION_DECIMAL_TYPE_NUMBER_OWNED:
                case ION_DECIMAL_TYPE_NUMBER:
                    IONCHECK(_ion_writer_binary_write_decimal_number(pwriter, value->value.num_value));
                    break;
                default:
                FAILWITH(IERR_INVALID_STATE);
            }
            break;
        default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_timestamp(hWRITER hwriter, iTIMESTAMP value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_timestamp_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_timestamp_helper(ION_WRITER *pwriter, ION_TIMESTAMP *value)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_timestamp(pwriter, value));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_timestamp(pwriter, value));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR _ion_writer_validate_symbol_id(ION_WRITER *pwriter, SID sid) {
    iENTER;
    ION_SYMBOL_TABLE *symtab;

    IONCHECK(_ion_writer_get_symbol_table_helper(pwriter, &symtab));
    if (sid > symtab->max_id || sid <= UNKNOWN_SID) {
        FAILWITHMSG(IERR_INVALID_SYMBOL, "Attempted to write out-of-range symbol ID.");
    }
    iRETURN;
}

iERR ion_writer_write_symbol_sid(hWRITER hwriter, SID value)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_symbol_id_helper(pwriter, value));

    iRETURN;
}

iERR _ion_writer_write_symbol_id_helper(ION_WRITER *pwriter, SID value)
{
    iENTER;

    ASSERT(pwriter);

    IONCHECK(_ion_writer_validate_symbol_id(pwriter, value));

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_symbol_id(pwriter, value));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_symbol_id(pwriter, value));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_symbol(hWRITER hwriter, iSTRING symbol)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_symbol_helper(pwriter, symbol));

    iRETURN;
}

iERR _ion_writer_write_symbol_helper(ION_WRITER *pwriter, ION_STRING *symbol)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(symbol);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_symbol(pwriter, symbol));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_symbol(pwriter, symbol));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_string(hWRITER hwriter, iSTRING pstr)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_write_string_helper(pwriter, pstr));

    iRETURN;
}

iERR _ion_writer_write_string_helper(ION_WRITER *pwriter, ION_STRING *pstr)
{
    iENTER;

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_string(pwriter, pstr));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_string(pwriter, pstr));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_clob(hWRITER hwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter)   FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    // p_buf can be null for a null value
    if (length < 0) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_write_clob_helper(pwriter, p_buf, length));

    iRETURN;
}

iERR _ion_writer_write_clob_helper(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;

    ASSERT(pwriter);
    // p_buf can be null for a null value
    ASSERT(length >= 0);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_clob(pwriter, p_buf, length));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_clob(pwriter, p_buf, length));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_write_blob(hWRITER hwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    // p_buf can be null for a null value
    if (length < 0) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_write_blob_helper(pwriter, p_buf, length));

    iRETURN;
}

iERR _ion_writer_write_blob_helper(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    
    ASSERT(pwriter);
    // p_buf can be null for a null value
    ASSERT(length >= 0);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_write_blob(pwriter, p_buf, length));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_write_blob(pwriter, p_buf, length));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_start_lob(hWRITER hwriter, ION_TYPE lob_type)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (lob_type != tid_BLOB && lob_type != tid_CLOB) FAILWITH(IERR_BAD_HANDLE);

    IONCHECK(_ion_writer_start_lob_helper(pwriter, lob_type));

    iRETURN;
}

iERR _ion_writer_start_lob_helper(ION_WRITER *pwriter, ION_TYPE lob_type)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(lob_type == tid_BLOB || lob_type == tid_CLOB);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_start_lob(pwriter, lob_type));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_start_lob(pwriter, lob_type));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_append_lob(hWRITER hwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_buf) FAILWITH(IERR_INVALID_ARG);
    if (length < 0) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_append_lob_helper(pwriter, p_buf, length));

    iRETURN;
}

iERR _ion_writer_append_lob_helper(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(p_buf);
    ASSERT(length >= 0);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_append_lob(pwriter, p_buf, length));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_append_lob(pwriter, p_buf, length));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_finish_lob(hWRITER hwriter)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_finish_lob_helper(pwriter));

    iRETURN;
}

iERR _ion_writer_finish_lob_helper(ION_WRITER *pwriter)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_finish_lob(pwriter));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_finish_lob(pwriter));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    iRETURN;
}

iERR ion_writer_start_container(hWRITER hwriter, ION_TYPE container_type)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    //let's just make user they're really starting a *container*
    switch ((intptr_t)container_type) {
    case (intptr_t)tid_STRUCT:
    case (intptr_t)tid_LIST:
    case (intptr_t)tid_SEXP:
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_writer_start_container_helper(pwriter, container_type));

    iRETURN;
}

iERR _ion_writer_start_container_helper(ION_WRITER *pwriter, ION_TYPE container_type)
{
    iENTER;
    
    ASSERT(pwriter);
    ASSERT(container_type == tid_STRUCT || container_type == tid_LIST || container_type == tid_SEXP);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_start_container(pwriter, container_type));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_start_container(pwriter, container_type));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }
    pwriter->depth++;
    iRETURN;
}

iERR ion_writer_finish_container(hWRITER hwriter)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_finish_container_helper(pwriter));

    iRETURN;
}

iERR _ion_writer_finish_container_helper(ION_WRITER *pwriter)
{
    iENTER;

    ASSERT(pwriter);

    switch (pwriter->type) {
    case ion_type_text_writer:
        IONCHECK(_ion_writer_text_finish_container(pwriter));
        break;
    case ion_type_binary_writer:
        IONCHECK(_ion_writer_binary_finish_container(pwriter));
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }
    pwriter->depth--;
    iRETURN;
}

iERR ion_writer_write_one_value(hWRITER hwriter, hREADER hreader)
{
    iENTER;
    ION_WRITER *pwriter;
    ION_READER *preader;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!hreader) FAILWITH(IERR_BAD_HANDLE);
    preader = HANDLE_TO_PTR(hreader, ION_READER);

    switch (pwriter->type) {
    case ion_type_text_writer:
    case ion_type_binary_writer:
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    // we let a helper (also used by write all values) to the 
    // real work without as much checking
    IONCHECK(_ion_writer_write_one_value_helper(pwriter, preader));

    iRETURN;
}

#define TEMP_BUFFER_SIZE (1024*128)
iERR _ion_writer_copy_lop(ION_WRITER *pwriter, ION_READER *preader)
{
    iENTER;
    int32_t read;
    BYTE    tmpByteBuffer[TEMP_BUFFER_SIZE];  

    for (;;) {
        IONCHECK(_ion_reader_read_lob_bytes_helper(preader, TRUE, tmpByteBuffer, TEMP_BUFFER_SIZE , &read));
        if (read < 1) break;
        IONCHECK(_ion_writer_append_lob_helper(pwriter, tmpByteBuffer, read));
    }
    SUCCEED();

    iRETURN;
}

iERR _ion_writer_write_one_value_helper(ION_WRITER *pwriter, ION_READER *preader)
{
    iENTER;

    ION_TYPE      type;
    ION_STRING   *fld_name, string_value;
    ION_SYMBOL    symbol_value;
    SID           sid;
    int32_t       count, ii;
    BOOL          is_null, bool_value, is_in_struct;
    double        double_value;
    ION_DECIMAL   decimal_value;
    ION_TIMESTAMP timestamp_value;


    ASSERT(pwriter);
    ASSERT(preader);

    IONCHECK(_ion_reader_get_type_helper(preader, &type));
    switch((intptr_t)type) {
    case (intptr_t)tid_EOF:
        // nothing to do here - unless we want to consider this an error ??
        SUCCEED();
    default:
        FAILWITH(IERR_INVALID_STATE);
    case (intptr_t)tid_NULL:
    case (intptr_t)tid_BOOL:
    case (intptr_t)tid_INT:
    case (intptr_t)tid_FLOAT:
    case (intptr_t)tid_DECIMAL:
    case (intptr_t)tid_TIMESTAMP:
    case (intptr_t)tid_STRING:
    case (intptr_t)tid_SYMBOL:
    case (intptr_t)tid_CLOB:
    case (intptr_t)tid_BLOB:
    case (intptr_t)tid_STRUCT:
    case (intptr_t)tid_LIST:
    case (intptr_t)tid_SEXP:
        break;
    }

    IONCHECK(ion_reader_is_in_struct(preader, &is_in_struct));
    if (is_in_struct) {
        IONCHECK(_ion_reader_get_field_name_helper(preader, &fld_name));
        if (ION_STRING_IS_NULL(fld_name)) {
            IONCHECK(_ion_reader_get_field_sid_helper(preader, &sid));
            IONCHECK(_ion_writer_write_field_sid_helper(pwriter, sid));
        } else {
            IONCHECK(_ion_writer_write_field_name_helper(pwriter, fld_name));
        }
    }

    IONCHECK(_ion_reader_get_annotation_count_helper(preader, &count));
    if (count > 0) {
        ION_STRING_INIT(&string_value);
        for (ii=0; ii<count; ii++) {
            IONCHECK(_ion_reader_get_an_annotation_helper(preader, ii, &string_value));
            if (ION_STRING_IS_NULL(&string_value)) {
                IONCHECK(_ion_reader_get_an_annotation_sid_helper(preader, ii, &sid));
                IONCHECK(_ion_writer_add_annotation_sid_helper(pwriter, sid));
            }
            else {
                IONCHECK(_ion_writer_add_annotation_helper(pwriter, &string_value));
            }
        }
    }

    switch((intptr_t)type) {
    case (intptr_t)tid_NULL:
        IONCHECK(_ion_writer_write_typed_null_helper(pwriter, tid_NULL));
        SUCCEED();
    case (intptr_t)tid_BOOL:
    case (intptr_t)tid_INT:
    case (intptr_t)tid_FLOAT:
    case (intptr_t)tid_DECIMAL:
    case (intptr_t)tid_TIMESTAMP:
    case (intptr_t)tid_STRING:
    case (intptr_t)tid_SYMBOL:
    case (intptr_t)tid_CLOB:
    case (intptr_t)tid_BLOB:
    case (intptr_t)tid_STRUCT:
    case (intptr_t)tid_LIST:
    case (intptr_t)tid_SEXP:
        IONCHECK(_ion_reader_is_null_helper(preader, &is_null));
        if (is_null) {
            IONCHECK(_ion_writer_write_typed_null_helper(pwriter, type));
            SUCCEED();
        }
        break;
    case (intptr_t)tid_EOF:       // handled in the previous switch
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    switch((intptr_t)type) {
    case (intptr_t)tid_BOOL:
        IONCHECK(_ion_reader_read_bool_helper(preader, &bool_value));
        IONCHECK(_ion_writer_write_bool_helper(pwriter, bool_value));
        break;
    case (intptr_t)tid_INT:
        IONCHECK(_ion_reader_read_mixed_int_helper(preader));
        IONCHECK(_ion_writer_write_mixed_int_helper(pwriter, preader));
        break;
    case (intptr_t)tid_FLOAT:
        IONCHECK(_ion_reader_read_double_helper(preader, &double_value));
        IONCHECK(_ion_writer_write_double_helper(pwriter, double_value));
        break;
    case (intptr_t)tid_DECIMAL:
        IONCHECK(_ion_reader_read_ion_decimal_helper(preader, &decimal_value));
        IONCHECK(_ion_writer_write_ion_decimal_helper(pwriter, &decimal_value));
        IONCHECK(ion_decimal_free(&decimal_value));
        break;
    case (intptr_t)tid_TIMESTAMP:
        IONCHECK(_ion_reader_read_timestamp_helper(preader, &timestamp_value));
        IONCHECK(_ion_writer_write_timestamp_helper(pwriter, &timestamp_value));
        break;
    case (intptr_t)tid_STRING:
        ION_STRING_INIT(&string_value);
        IONCHECK(_ion_reader_read_string_helper(preader,  &string_value));
        IONCHECK(_ion_writer_write_string_helper(pwriter, &string_value));
        break;
    case (intptr_t)tid_SYMBOL:
        IONCHECK(_ion_reader_read_symbol_helper(preader, &symbol_value));
        if (ION_STRING_IS_NULL(&symbol_value.value)) {
            IONCHECK(_ion_writer_write_symbol_id_helper(pwriter, symbol_value.sid));
        }
        else {
            IONCHECK(_ion_writer_write_symbol_helper(pwriter, &symbol_value.value));
        }
        break;
    case (intptr_t)tid_CLOB:
    case (intptr_t)tid_BLOB:
        IONCHECK(_ion_writer_start_lob_helper(pwriter, type));
        IONCHECK(_ion_writer_copy_lop(pwriter, preader));
        IONCHECK(_ion_writer_finish_lob_helper(pwriter));
        break;
    case (intptr_t)tid_STRUCT:
    case (intptr_t)tid_LIST:
    case (intptr_t)tid_SEXP:
        IONCHECK(_ion_reader_step_in_helper(preader));
        IONCHECK(_ion_writer_start_container_helper(pwriter, type));
        IONCHECK(_ion_writer_write_all_values_helper(pwriter, preader));
        IONCHECK(_ion_writer_finish_container_helper(pwriter));
        IONCHECK(_ion_reader_step_out_helper(preader));
        SUCCEED();
    case (intptr_t)tid_EOF:       // handled in the previous switch
    case (intptr_t)tid_NULL:      // also handled in the previous switch
    default:
        FAILWITH(IERR_INVALID_STATE);

    }

    iRETURN;
}

iERR ion_writer_write_all_values(hWRITER hwriter, hREADER hreader)
{
    iENTER;
    ION_WRITER *pwriter;
    ION_READER *preader;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!hreader) FAILWITH(IERR_BAD_HANDLE);
    preader = HANDLE_TO_PTR(hreader, ION_READER);

    switch (pwriter->type) {
    case ion_type_text_writer:
    case ion_type_binary_writer:
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_writer_write_all_values_helper(pwriter, preader));

    iRETURN;
}

iERR _ion_writer_write_all_values_helper(ION_WRITER *pwriter, ION_READER *preader)
{
    iENTER;
    ION_TYPE type;
    uint32_t count = 0;

    ASSERT(pwriter);
    ASSERT(preader);

    // no need for separate versions, these all work the same
    // although there's an oppertunity for a pseudo-safe optimization
    // if the reader and writer have the same data format - this
    // could simply be a byte copy.  But for now let's force them to
    // parse the data.
    for (;;) {
        IONCHECK(_ion_reader_next_helper(preader, &type));
        if (type == tid_EOF) break;
        count++;
        // TODO support hand-off of shared imports from the reader to the writer as the reader changes symbol table
        // contexts.
        IONCHECK(_ion_writer_write_one_value_helper(pwriter, preader));
    }

    iRETURN;
}

iERR ion_writer_flush(hWRITER hwriter, SIZE *p_bytes_flushed)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_flush_helper(pwriter, p_bytes_flushed));

    iRETURN;
}

iERR ion_writer_finish(hWRITER hwriter, SIZE *p_bytes_flushed)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_flush_helper(pwriter, p_bytes_flushed));
    IONCHECK(_ion_writer_free_local_symbol_table(pwriter));
    IONCHECK(_ion_writer_reset_temp_pool(pwriter));
    switch (pwriter->type) {
        case ion_type_binary_writer:
            pwriter->_typed_writer.binary._version_marker_written = FALSE;
            break;
        case ion_type_text_writer:
            IONCHECK(_ion_writer_text_initialize(pwriter));
            break;
        default:
            FAILWITH(IERR_INVALID_STATE);
    }
    // The writer may be reused. Therefore, its local symbol table (which may include imports provided by the user)
    // needs to be reinitialized.
    IONCHECK(_ion_writer_initialize_local_symbol_table(pwriter));
    pwriter->_has_local_symbols      = FALSE;
    iRETURN;
}

iERR _ion_writer_flush_helper(ION_WRITER *pwriter, SIZE *p_bytes_flushed)
{
    iENTER;
    int64_t start, finish;

    ASSERT(pwriter);

    if (pwriter->depth != 0) {
        FAILWITHMSG(IERR_INVALID_STATE, "Cannot flush a writer that is not at the top level.");
    }

    ASSERT(!pwriter->_in_struct);

    switch (pwriter->type) {
    case ion_type_text_writer:
        if (pwriter->_typed_writer.text._top > 0) {
            FAILWITHMSG(IERR_INVALID_STATE, "Cannot flush a text writer with a value in progress.");
        }
        // The text writer does not need to buffer data.
        start = 0;
        finish = ion_stream_get_position(pwriter->output);
        if (pwriter->depth == 0) {
            IONCHECK(ion_temp_buffer_reset(&pwriter->temp_buffer));
            IONCHECK(_ion_writer_text_initialize_stack(pwriter));
        }
        break;
    case ion_type_binary_writer:
        if (pwriter->_typed_writer.binary._lob_in_progress != tid_none) {
            FAILWITHMSG(IERR_INVALID_STATE, "Cannot flush a binary writer with a lob in progress.");
        }
        start =  ion_stream_get_position(pwriter->output);
        if (pwriter->depth == 0) {
            IONCHECK(_ion_writer_binary_flush_to_output(pwriter));
            IONCHECK(ion_temp_buffer_reset(&pwriter->temp_buffer));
        }
        finish = ion_stream_get_position(pwriter->output);
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(ion_stream_flush(pwriter->output));
    if (p_bytes_flushed) *p_bytes_flushed = (SIZE)(finish - start);    // TODO - this needs 64bit care

    iRETURN;
}

iERR ion_writer_close(hWRITER hwriter)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_close_helper(pwriter));

    iRETURN;
}

iERR _ion_writer_close_helper(ION_WRITER *pwriter)
{
    iENTER;

    ASSERT(pwriter);

    if (pwriter->depth != 0) {
        FAILWITHMSG(IERR_INVALID_STATE, "Cannot close writer that is not at the top level.");
    }

    // all the local resources are allocated in the parent
    // (at least at the present time)
    // we really should try to keep that way!
    // first we close the specialized stuff (that ought to be nothing, but
    // there's no percentage in not checking
    switch (pwriter->type) {
    case ion_type_unknown_writer:
        break;
    case ion_type_text_writer:
        if (pwriter->_typed_writer.text._top > 0) {
            FAILWITHMSG(IERR_INVALID_STATE, "Cannot flush a text writer with a value in progress.");
        }
        UPDATEERROR(_ion_writer_text_close(pwriter));
        break;
    case ion_type_binary_writer:
        if (pwriter->_typed_writer.binary._lob_in_progress != tid_none) {
            FAILWITHMSG(IERR_INVALID_STATE, "Cannot flush a binary writer with a lob in progress.");
        }
        UPDATEERROR(_ion_writer_binary_close(pwriter));
        break;
    default:
        UPDATEERROR(IERR_INVALID_ARG);
    }

    // now we close our base members
    UPDATEERROR(_ion_writer_free_local_symbol_table( pwriter ));
    UPDATEERROR( _ion_writer_free_temp_pool( pwriter ));

    // if we allocated the stream we need to free it
    if (pwriter->writer_owns_stream) {
        UPDATEERROR(ion_stream_close(pwriter->output));
    }

    // then we free ourselves :) all associated memory, for
    // which we (pwriter) are the owner
    ion_free_owner(pwriter);

    iRETURN;
}


iERR _ion_writer_free_local_symbol_table( ION_WRITER *pwriter )
{
    iENTER;
    ASSERT(pwriter);

    if (pwriter->_local_symbol_table) {
        ASSERT(pwriter->symbol_table);
        // local symbol tables are owned by the _temp_entity_pool, which is freed upon flush and close.
        pwriter->symbol_table = NULL;
        pwriter->_local_symbol_table = FALSE;
    }
    
    SUCCEED();

    iRETURN;
}

iERR _ion_writer_make_symbol_helper(ION_WRITER *pwriter, ION_STRING *pstr, SID *p_sid)
{
    iENTER;
    SID               sid = UNKNOWN_SID;
    SID               old_max_id;
    ION_SYMBOL_TABLE *psymtab;

    ASSERT(pwriter);
    ASSERT(pstr);
    ASSERT(p_sid);

    // first we make sure there a reasonable local symbol table
    // in case we need to add this symbol to the list
    psymtab = pwriter->symbol_table;
    if (!psymtab || psymtab->is_locked) 
    {
        IONCHECK(_ion_writer_initialize_local_symbol_table(pwriter));
        psymtab = pwriter->symbol_table;
    }

    // we'll remember what the top symbol is to see if add_symbol changes it
    old_max_id = psymtab->max_id;
    IONCHECK( _ion_symbol_table_add_symbol_helper( psymtab, pstr, &sid));

    // see if this symbol ended up changing the symbol list (if it already
    // was present the max_id doesn't change and we don't reuse 
    if (old_max_id != psymtab->max_id) {
        pwriter->_has_local_symbols = TRUE;
    }

    *p_sid = sid;
    iRETURN;
}

iERR ion_writer_clear_field_name(hWRITER hwriter)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_clear_field_name_helper(pwriter));

    iRETURN;
}


iERR _ion_writer_clear_field_name_helper(ION_WRITER *pwriter)
{
    iENTER;

    ASSERT(pwriter);

    ION_STRING_INIT(&pwriter->field_name);
    pwriter->field_name_sid = UNKNOWN_SID;
    SUCCEED();

    iRETURN;
}

iERR ion_writer_get_field_name_as_string(hWRITER hwriter, ION_STRING *p_str)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_str)   FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_field_name_as_string_helper(pwriter, p_str));

    iRETURN;
}

iERR _ion_writer_get_field_name_as_string_helper(ION_WRITER *pwriter, ION_STRING *p_str)
{
    iENTER;
    ION_STRING *pstr;
    ION_SYMBOL_TABLE *psymtab;

    ASSERT(pwriter);
    ASSERT(p_str);

    switch ((intptr_t)pwriter->field_name_type) {
    case (intptr_t)tid_STRING:
        ION_STRING_ASSIGN(p_str, &pwriter->field_name);
        break;
    case (intptr_t)tid_INT:
        IONCHECK(_ion_writer_get_local_symbol_table(pwriter, &psymtab));
        assert(psymtab != NULL);
        if (pwriter->field_name_sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);
        IONCHECK(_ion_symbol_table_find_by_sid_force(psymtab, pwriter->field_name_sid, &pstr));
        ION_STRING_ASSIGN(p_str, pstr);
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_writer_get_field_name_as_sid(hWRITER hwriter, SID *p_sid)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_sid)   FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_field_name_as_sid_helper(pwriter, p_sid));

    iRETURN;
}

iERR _ion_writer_get_field_name_as_sid_helper(ION_WRITER *pwriter, SID *p_sid)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(p_sid);

    switch ((intptr_t)pwriter->field_name_type) {
    case (intptr_t)tid_STRING:
        IONCHECK(_ion_writer_make_symbol_helper( pwriter, &pwriter->field_name, p_sid ));
        break;
    case (intptr_t)tid_INT:
        *p_sid = pwriter->field_name_sid;
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_writer_clear_annotations(hWRITER hwriter)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);

    IONCHECK(_ion_writer_clear_annotations_helper(pwriter));

    iRETURN;
}

iERR _ion_writer_clear_annotations_helper(ION_WRITER *pwriter)
{
    ASSERT(pwriter);

    // we'll clear and reset this (even if there are no annotations)
    pwriter->annotations_type = tid_none;
    pwriter->annotation_curr = 0;

    return IERR_OK;
}

iERR ion_writer_get_annotation_count(hWRITER hwriter, int32_t *p_count)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_count) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_annotation_count_helper(pwriter, p_count));

    iRETURN;
}

iERR _ion_writer_get_annotation_count_helper(ION_WRITER *pwriter, int32_t *p_count)
{
    *p_count = pwriter->annotation_curr;
    return IERR_OK;
}


iERR ion_writer_get_annotation_as_string(hWRITER hwriter, int32_t idx, ION_STRING *p_str)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_str)   FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_annotation_as_string_helper(pwriter, idx, p_str));

    iRETURN;
}

iERR _ion_writer_get_annotation_as_string_helper(ION_WRITER *pwriter, int32_t idx, ION_STRING *p_str)
{
    iENTER;
    ION_STRING       *pstr;
    ION_SYMBOL_TABLE *psymtab;
    SID               sid;

    ASSERT(pwriter);
    ASSERT(p_str);

    if (idx >= pwriter->annotation_curr) FAILWITH(IERR_INVALID_ARG);

    switch ((intptr_t)pwriter->annotations_type) {
    case (intptr_t)tid_STRING:
        ION_STRING_ASSIGN(p_str, &(pwriter->annotations[idx]));
        break;
    case (intptr_t)tid_INT:
        IONCHECK(_ion_writer_get_local_symbol_table(pwriter, &psymtab));
        assert(psymtab != NULL);
        sid = pwriter->annotation_sids[idx];
        if (sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);
        IONCHECK(_ion_symbol_table_find_by_sid_force(psymtab, sid, &pstr));
        ION_STRING_ASSIGN(p_str, pstr);
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_writer_get_annotation_as_sid(hWRITER hwriter, int32_t idx, SID *p_sid)
{
    iENTER;
    ION_WRITER *pwriter;

    if (!hwriter) FAILWITH(IERR_BAD_HANDLE);
    pwriter = HANDLE_TO_PTR(hwriter, ION_WRITER);
    if (!p_sid) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_writer_get_annotation_as_sid_helper(pwriter, idx, p_sid));

    iRETURN;
}

iERR _ion_writer_get_annotation_as_sid_helper(ION_WRITER *pwriter, int32_t idx, SID *p_sid)
{
    iENTER;

    ASSERT(pwriter);
    ASSERT(p_sid);

    if (idx >= pwriter->annotation_curr) FAILWITH(IERR_INVALID_ARG);

    switch ((intptr_t)pwriter->annotations_type) {
    case (intptr_t)tid_STRING:
        IONCHECK(_ion_writer_make_symbol_helper( pwriter, &(pwriter->annotations[idx]), p_sid ));
        break;
    case (intptr_t)tid_INT:
        *p_sid = pwriter->annotation_sids[idx];
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

// these are helper routines for the temp buffer the writers may need to use

iERR ion_temp_buffer_init(hOWNER owner, ION_TEMP_BUFFER *temp_buffer, SIZE size_of_temp_space)
{
    iENTER;
    BYTE *buf;

    if (!owner) FAILWITH(IERR_BAD_HANDLE);
    if (!temp_buffer || size_of_temp_space < 0) FAILWITH(IERR_INVALID_ARG);

    buf = (BYTE *)ion_alloc_with_owner(owner, size_of_temp_space);
    if (!buf) FAILWITH(IERR_NO_MEMORY);

    temp_buffer->base     = buf;
    temp_buffer->position = buf;
    temp_buffer->limit    = buf + size_of_temp_space;

    iRETURN;
}

iERR ion_temp_buffer_alloc(ION_TEMP_BUFFER *temp_buffer, SIZE needed, void **p_ptr)
{
    iENTER;
    BYTE *buf;

    if (!temp_buffer) FAILWITH(IERR_INVALID_ARG);
    if (!p_ptr || needed < 0) FAILWITH(IERR_INVALID_ARG);
    if (temp_buffer->position + needed >= temp_buffer->limit) FAILWITH(IERR_NO_MEMORY);

    buf = temp_buffer->position;
    temp_buffer->position += needed;
    *p_ptr = buf;

    iRETURN;
}

iERR ion_temp_buffer_make_utf8_string(ION_TEMP_BUFFER *temp_buffer, char *cstr, SIZE length, void **p_ptr, SIZE *p_utf8_length)
{
    iENTER;
    BYTE *buf;

    if (!temp_buffer) FAILWITH(IERR_INVALID_ARG);
    IONCHECK(ion_temp_buffer_alloc(temp_buffer, length, (void*)&buf));

    // TODO: actual cstring to utf 8 conversion - 
    //       what is the character encoding in C?
    //       (since it's controled by the compiler 
    //       and various "support" libraries
    memcpy(buf, cstr, length);

    // return the buf and what we wrote into it
    *p_ptr = buf;
    *p_utf8_length = length;

    iRETURN;
}

iERR ion_temp_buffer_make_string_copy(ION_TEMP_BUFFER *temp_buffer, ION_STRING *pdst, ION_STRING *psrc)
{
    iENTER;
    if (!temp_buffer) FAILWITH(IERR_INVALID_ARG);
    if (!pdst)        FAILWITH(IERR_INVALID_ARG);
    if (!psrc)        FAILWITH(IERR_INVALID_ARG);

    ION_STRING_INIT(pdst);

    IONCHECK(ion_temp_buffer_alloc(temp_buffer, psrc->length, (void**)&pdst->value));
    memcpy(pdst->value, psrc->value, psrc->length);
    pdst->length = psrc->length;
    SUCCEED();

    iRETURN;
}

iERR ion_temp_buffer_reset(ION_TEMP_BUFFER *temp_buffer)
{
    iENTER;

    if (!temp_buffer) FAILWITH(IERR_INVALID_ARG);
    temp_buffer->position = temp_buffer->base;

    iRETURN;
}

iERR _ion_writer_allocate_temp_pool( ION_WRITER *pwriter )
{
    iENTER;
    void *temp_owner;

    temp_owner = ion_alloc_owner(sizeof(int)); // this is a fake allocation to hold the pool
    if (temp_owner == NULL) {
        FAILWITH(IERR_NO_MEMORY);
    }
    pwriter->_temp_entity_pool = temp_owner;

    iRETURN;
}
iERR _ion_writer_reset_temp_pool( ION_WRITER *pwriter )
{
    iENTER;

    IONCHECK( _ion_writer_free_temp_pool( pwriter ));
    if (pwriter->_temp_entity_pool == NULL) {
        IONCHECK( _ion_writer_allocate_temp_pool( pwriter ));
    }
    SUCCEED();

    iRETURN;
}
iERR _ion_writer_free_temp_pool( ION_WRITER *pwriter )
{
    iENTER;

    if ((pwriter->_temp_entity_pool != NULL)) {
        ion_free_owner( pwriter->_temp_entity_pool );
        pwriter->_temp_entity_pool = NULL;
    }
    SUCCEED();

    iRETURN;
}
