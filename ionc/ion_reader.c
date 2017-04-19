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

#include "ion_internal.h"
#define IONCLOSEpREADER(x)  {   if (x != NULL)                                  \
                                {                                               \
                                    UPDATEERROR(_ion_reader_close_helper(x));   \
                                    x = NULL;                                   \
                                }                                               \
                            }

iERR ion_reader_open_buffer(hREADER *p_hreader, BYTE *buffer, SIZE buf_length, ION_READER_OPTIONS *p_options)
{
    iENTER;
    ION_READER *preader = NULL;

    if (p_hreader == NULL) FAILWITH(IERR_INVALID_ARG);
    if (buffer == NULL) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_open_buffer_helper(&preader, buffer, buf_length, p_options));
    *p_hreader = PTR_TO_HANDLE(preader);

    iRETURN;
}

iERR _ion_reader_open_buffer_helper(ION_READER **p_preader, BYTE *buffer, SIZE buf_length, ION_READER_OPTIONS *p_options)
{
    iENTER;
    ION_READER *preader = NULL;

    ASSERT(p_preader != NULL);
    ASSERT(buffer != NULL);

    // allocates and starts initialization of the base (unified) reader
    // this also sets, validates and defaults the options
    IONCHECK(_ion_reader_make_new_reader(p_options, &preader));

    // set up the stream for use
    IONCHECK(ion_stream_open_buffer(buffer, buf_length, buf_length, TRUE, &preader->istream));
    preader->_reader_owns_stream = TRUE;

    // since the user gave use the whole input in a single buffer this makes it available
    // TODO: since it's intrinsically in the stream do we need it here at all??
    preader->has_static_buffer = TRUE;

    // initialize the remaining shared variables and call to have
    // the correct typed reader initialized
    IONCHECK(_ion_reader_initialize(preader, buffer, buf_length));

    *p_preader = preader;
    return err;
    // iRETURN;
fail:
    IONCLOSEpREADER(preader);
    *p_preader = NULL;
    return err;
}



iERR ion_reader_reset_stream_with_length(hREADER   *p_hreader
                                         ,void     *handler_state
                                         ,ION_STREAM_HANDLER fn_input_handler
                                         ,POSITION  length
) {

    iENTER;
    POSITION local_end;
    ION_STREAM    *pstream = NULL;

    if(!p_hreader)        FAILWITH(IERR_INVALID_ARG);
    if(!fn_input_handler) FAILWITH(IERR_INVALID_ARG);	

    pstream = ((*p_hreader)->istream);
    ion_stream_close(pstream);
    pstream = NULL;

    // here we reset what little state the reader need to address directly
    IONCHECK(_ion_reader_reset_temp_pool(*p_hreader));

    if ((*p_hreader)->_local_symtab_pool != NULL) {
        ion_free_owner( (*p_hreader)->_local_symtab_pool );
        (*p_hreader)->_local_symtab_pool = NULL;
    }

    // initialize given stream with handler
    ion_stream_open_handler_in(fn_input_handler, handler_state, &pstream);
    (*p_hreader)->istream = pstream;

    memset(&((*p_hreader)->_int_helper), 0, sizeof((*p_hreader)->_int_helper));

    if (length >= 0) {
        local_end = length;
    }
    else {
        local_end = -1;
    }

    // and then we reset the underlying parser (text or binary) with the given length.
    // Parser will subsequently return EOF when "length" number of bytes is reached.
    switch((*p_hreader)->type) {
        case ion_type_text_reader:
            IONCHECK(_ion_reader_text_close(*p_hreader));
            IONCHECK(_ion_reader_text_open(*p_hreader));
            //IONCHECK(_ion_reader_text_reset((*p_hreader), tid_DATAGRAM, local_end));
            break;
        case ion_type_binary_reader:
            _ion_reader_binary_reset((*p_hreader), TID_DATAGRAM, local_end);
            break;
        case ion_type_unknown_reader:
        default:

            FAILWITH(IERR_INVALID_STATE);
    }

    (*p_hreader)->_eof = FALSE;
    iRETURN;
}


iERR ion_reader_reset_stream(hREADER *p_hreader, void *handler_state, ION_STREAM_HANDLER fn_input_handler)
{
    iENTER;
    ION_STREAM *pstream;
    BYTE  ivm_buffer[ION_VERSION_MARKER_LENGTH];
    BOOL is_binary_stream;
    int  b, pos, ii;

    if(!p_hreader)        FAILWITH(IERR_INVALID_ARG);
    if(!fn_input_handler) FAILWITH(IERR_INVALID_ARG);

    pstream = ((*p_hreader)->istream);
    ion_stream_close(pstream);
    pstream = NULL;

    IONCHECK(_ion_reader_reset_temp_pool(*p_hreader));
    if ((*p_hreader)->_local_symtab_pool != NULL) {
        ion_free_owner( (*p_hreader)->_local_symtab_pool );
        (*p_hreader)->_local_symtab_pool = NULL;
    }

    // initialize given stream with handler
    ion_stream_open_handler_in(fn_input_handler, handler_state, &pstream);
    (*p_hreader)->istream = pstream;
    (*p_hreader)->_reader_owns_stream = TRUE;
    
    b = 0;
    for (pos = 0; pos < ION_VERSION_MARKER_LENGTH; pos++) {
        ION_GET(pstream, b);
        if (b < 0) {
            IONCHECK(ion_stream_unread_byte(pstream, b));
            break;
        }
        ivm_buffer[pos] = (BYTE)b;
    }
    
    // now unread any bytes we happen to have read in (so we'll start
    // reading at the beginning of the buffer - esp important for text :)
    ii = pos;
    while (ii--) {
        IONCHECK(ion_stream_unread_byte(pstream, ivm_buffer[ii]));
    }
    
    // ensure whether the reader type is same as before & reinitialize the parsers
    // catalog, symbol table, dec_context & other reader defaults would be reused
    // @see _ion_reader_initialize impl.
    is_binary_stream = ion_helper_is_ion_version_marker(ivm_buffer, pos);
    if ( is_binary_stream && ((*p_hreader)->type == ion_type_binary_reader)) {
        IONCHECK(_ion_reader_binary_close(*p_hreader));
        IONCHECK(_ion_reader_binary_open(*p_hreader));
    }
    else if ( (!is_binary_stream) && ((*p_hreader)->type == ion_type_text_reader) ){
        IONCHECK(_ion_reader_text_close(*p_hreader));
        IONCHECK(_ion_reader_text_open(*p_hreader));
    }
    else {
        FAILWITH(IERR_INVALID_STATE);
    }
    // Mark the eof flag to false, since reader is provided with new stream
    (*p_hreader)->_eof = FALSE;
    iRETURN;
}

iERR ion_reader_open_stream(hREADER *p_hreader
                           ,void *handler_state
                           ,ION_STREAM_HANDLER fn_input_handler
                           ,ION_READER_OPTIONS *p_options
) {
    iENTER;
    ION_READER    *preader = NULL;
    ION_STREAM    *pstream = NULL;

    if(!p_hreader) FAILWITH(IERR_INVALID_ARG);
    if(!p_hreader) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(ion_stream_open_handler_in( fn_input_handler, handler_state, &pstream ));
    IONCHECK(_ion_reader_open_stream_helper( &preader, pstream, p_options ));
    preader->_reader_owns_stream = TRUE;

    *p_hreader = PTR_TO_HANDLE(preader);

    iRETURN;
}

iERR ion_reader_open(
         hREADER            *p_hreader
        ,ION_STREAM         *stream
        ,ION_READER_OPTIONS *p_options
) {
    iENTER;
    ION_READER    *preader = NULL;

    if(!p_hreader) FAILWITH(IERR_INVALID_ARG);
    if(!stream)    FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_open_stream_helper(&preader, stream, p_options));
    preader->_reader_owns_stream = FALSE;

    *p_hreader = PTR_TO_HANDLE(preader);

    iRETURN;
}

iERR _ion_reader_open_stream_helper(
         ION_READER        **p_preader
        ,ION_STREAM         *p_stream
        ,ION_READER_OPTIONS *p_options
)
{
    iENTER;
    ION_READER *preader = NULL;
    BYTE        ivm_buffer[ION_VERSION_MARKER_LENGTH];
    int         b, pos, ii; // UNUSED 12 dec 2012: , buflen;

    ASSERT(p_preader);
    ASSERT(p_stream);

    // allocates and starts initialization of the base (unified) reader
    // this also sets, validates and defaults the options
    IONCHECK(_ion_reader_make_new_reader(p_options, &preader));

    // initialize the input stream so that we can check for a
    // version marker before we create the particular reader
    preader->istream = p_stream;

    // read the possible ion version marker, but we may hit EOF
    b = 0;
    for (pos = 0; pos < ION_VERSION_MARKER_LENGTH; pos++) {
        ION_GET(p_stream, b);
        if (b < 0) break;
        ivm_buffer[pos] = (BYTE)b;
    }
    if (b < 0) {
        IONCHECK(ion_stream_unread_byte(p_stream, b));
    }

    // now unread any bytes we happen to have read in (so we'll start
    // reading at the beginning of the buffer - esp important for text :)
    ii = pos;
    while (ii--) {
        IONCHECK(ion_stream_unread_byte(p_stream, ivm_buffer[ii]));
    }

    // initialize the remaining shared variables and call to have
    // the correct typed reader initialized
    IONCHECK(_ion_reader_initialize(preader, ivm_buffer, pos));

    *p_preader = preader;
    return err;
    // iRETURN;
fail:
    IONCLOSEpREADER(preader);
    *p_preader = preader;
    return err;
}

iERR _ion_reader_make_new_reader(ION_READER_OPTIONS *p_options, ION_READER **p_reader)
{
    iENTER;
    ION_READER *preader = NULL;
    SIZE        len;

    ASSERT(p_reader);
    // and p_options might or might not be NULL

    // we allocate and initialize the unified reader here and initialize
    // the stream.  Later we'll initialize typed portion of the reader
    // once we know what format we're going to be processing
    len = sizeof(ION_READER);
    preader = (ION_READER *)ion_alloc_owner(len);
    *p_reader = preader;
    if (!preader) {
        FAILWITH(IERR_NO_MEMORY);
    }
    memset(preader, 0, len);

    preader->type = ion_type_unknown_reader;
    IONCHECK(_ion_reader_set_options(preader, p_options));


    return err;
    // iRETURN;

fail:
    IONCLOSEpREADER(preader);
    *p_reader = NULL;
    return err;
}

iERR _ion_reader_set_options(ION_READER *preader, ION_READER_OPTIONS* p_options)
{
    iENTER;
    ASSERT(preader != NULL);

    // if we have options copy them here so we have our own copy
    if (p_options) {
        memcpy(&(preader->options), p_options, sizeof(preader->options));
    }

    // after we have our own copy, set the defaults for any property not specified
    _ion_reader_initialize_option_defaults(&(preader->options));

    IONCHECK(_ion_reader_validate_options(&(preader->options)));

    iRETURN;
}

void _ion_reader_initialize_option_defaults(ION_READER_OPTIONS* p_options)
{
    ASSERT(p_options != NULL);

    // most options are set to the correct default by memset(0)
    // but for size values we'll want to replace the 0's with
    // actual values so we don't have to test them all the time

    // what new line character should we be using
    // TODO: is this being used any longer? (it doesn't seem to be)
    //       removing it would require users who used it to fix code
    //       they have to recompile in any event for possible new structs
    //       but they generally shouldn't have to change any code.
    if (!p_options->new_line_char) {
        p_options->new_line_char = '\n';
    }

    // the max container depth defaults to 10
    if (!p_options->max_container_depth) {
        p_options->max_container_depth = DEFAULT_WRITER_STACK_DEPTH;
    }

    // the max number of annotations on 1 value, defaults to 10
    if (!p_options->max_annotation_count) {
        p_options->max_annotation_count = DEFAULT_ANNOTATION_LIMIT;
    }


    /** The max number number of bytes the annotations on a single value. This 
     *  is an total. How the bytes are divided among the annotations is irrelevant
     *  (i.e. 1 large, or 100 small may have the same total space requirements). 
     *  defaults to user_value_threshold (or 4096).
     *
     */
    if (!p_options->max_annotation_buffered) {
        p_options->max_annotation_buffered = DEFAULT_ANNOTATION_BUFFER_LIMIT;
    }

    // the size maximum size allowed for symbols, 512 bytes is the default
    if (!p_options->symbol_threshold) {
        p_options->symbol_threshold = DEFAULT_SYMBOL_THRESHOLD;
    }

    // the size maximum for allocations on behalf of the user for returned values
    if (!p_options->user_value_threshold) {
        p_options->user_value_threshold = DEFAULT_USER_ALLOC_THRESHOLD;
    }

    // the size over which long values are returned as chunks
    if (!p_options->chunk_threshold) {
        p_options->chunk_threshold = DEFAULT_CHUNK_THRESHOLD;
    }

    // memory is allocated in pages owned by the primary entities it's default size is 4096
    if (!p_options->allocation_page_size) {
        p_options->allocation_page_size = DEFAULT_BLOCK_SIZE;
    }

    return;
}

iERR _ion_reader_validate_options(ION_READER_OPTIONS* p_options)
{
    iENTER;
    char *msg;
    ASSERT(p_options != NULL);

    // the values are set either by the user or from the default,
    // but when all that's said and done we still need to see that
    // these are rational

    // the max number of annotations on 1 value, defaults to 10
    if (p_options->max_annotation_count < MIN_ANNOTATION_LIMIT) {
        msg = "max annotation count below min of " STR(MIN_ANNOTATION_LIMIT);
        FAILWITHMSG(IERR_INVALID_ARG, msg);
    }

      // the max container depth defaults to 10
    if (p_options->max_container_depth < MIN_WRITER_STACK_DEPTH) {
        msg = "max container depth below min of " STR(MIN_WRITER_STACK_DEPTH);
        FAILWITHMSG(IERR_INVALID_ARG, msg);
    }

    // the size maximum size allowed for symbols, 512 bytes is the defaul
    if (p_options->symbol_threshold < MIN_SYMBOL_THRESHOLD) {
        msg = "symbol threshold below min of " STR(MIN_SYMBOL_THRESHOLD);
        FAILWITHMSG(IERR_INVALID_ARG, msg);
    }

    // the size over which long values are returned as chunks
    if (p_options->chunk_threshold == 0) {
        p_options->chunk_threshold = DEFAULT_CHUNK_THRESHOLD;
    }
    if (p_options->chunk_threshold < MIN_CHUNK_THRESHOLD) {
        msg = "chunk threshold below min of " STR(MIN_CHUNK_THRESHOLD);
        FAILWITHMSG(IERR_INVALID_ARG, msg);
    }

    // memory is allocated in pages owned by the primary entities it's default size is 4096
    if (p_options->allocation_page_size < MIN_ION_ALLOCATION_BLOCK_SIZE) {
        msg = "page size below min of " STR(MIN_ION_ALLOCATION_BLOCK_SIZE);
        FAILWITHMSG(IERR_INVALID_ARG, msg);
    }

    if (p_options->allocation_page_size < p_options->chunk_threshold) {
        msg = "page size must be greater than chunk threshold";
        FAILWITHMSG(IERR_INVALID_ARG, msg);
    }

    iRETURN;
}

iERR _ion_reader_initialize(ION_READER *preader, BYTE *version_buffer, SIZE version_length)
{
    iENTER;
    hCATALOG            hcatalog;
    ION_SYMBOL_TABLE   *system;

    ASSERT(preader);
    ASSERT(version_buffer);

    IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
    ASSERT(system != NULL);

    // initialize the catalog (readers generally need catalogs)
    if (preader->options.pcatalog == NULL) {
        IONCHECK(ion_catalog_open_with_owner(&hcatalog, (hOWNER)preader));
    }
    else {
        hcatalog = PTR_TO_HANDLE(preader->options.pcatalog);
    }
    preader->_catalog = HANDLE_TO_PTR(hcatalog, ION_CATALOG);

    // initialize decimal context
    decContextDefault(&preader->_deccontext, DEC_INIT_DECQUAD);

    // we start our symbol table out with the system symbol table
    preader->_current_symtab = system;

    // keep the readers copy of depth up to date
    preader->_depth = 0;

    // now we can check the binary Ion Version Marker
    // we'll have to "unread" these bytes 
    if (ion_helper_is_ion_version_marker(version_buffer, version_length)) {
        preader->type = ion_type_binary_reader;
        IONCHECK(_ion_reader_binary_open(preader));
    }
    else {
        preader->type = ion_type_text_reader;
        // initialize input stream, parser and scanner
        IONCHECK(_ion_reader_text_open(preader));
    }

    iRETURN;
}

iERR ion_reader_get_catalog(hREADER hreader, hCATALOG *p_hcatalog)
{
    iENTER;
    ION_CATALOG *pcatalog;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_hcatalog) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_catalog_helper(preader, &pcatalog));
    *p_hcatalog = PTR_TO_HANDLE(pcatalog);

    iRETURN;
}

iERR _ion_reader_get_catalog_helper(ION_READER *preader, ION_CATALOG **p_pcatalog)
{
    iENTER;
    ION_CATALOG *pcatalog;

    ASSERT(preader);
    ASSERT(p_pcatalog);

    switch(preader->type) {
    case ion_type_text_reader:
    case ion_type_binary_reader:
        pcatalog = preader->_catalog;
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_pcatalog = pcatalog;

    iRETURN;
}

iERR ion_reader_next(hREADER hreader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value_type) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_next_helper(preader, p_value_type));

    iRETURN;
}

iERR _ion_reader_next_helper(ION_READER *preader, ION_TYPE *p_value_type)
{
    iENTER;
    
    ASSERT(preader);
    ASSERT(p_value_type);
    
    // we reset the temp value pool at the beginning of each top level value
    if ( preader->_depth == 0 ) {
        IONCHECK( _ion_reader_reset_temp_pool( preader ));
    }

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_next(preader, p_value_type));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_next(preader, p_value_type));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_step_in(hREADER hreader)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);

    IONCHECK(_ion_reader_step_in_helper(preader));

    iRETURN;
}

iERR _ion_reader_step_in_helper(ION_READER *preader)
{
    iENTER;

    ASSERT(preader);

    switch(preader->type) {
    case ion_type_text_reader:
        // IONCHECK(_ion_reader_text_step_in(preader));
        IONCHECK(_ion_reader_text_step_in(preader));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_step_in(preader));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    // we keep the readers own depth which is used to control the
    // lifetime of the local allocation pool
    preader->_depth++;

    iRETURN;
}

iERR ion_reader_step_out(hREADER hreader)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);

    IONCHECK(_ion_reader_step_out_helper(preader));

    iRETURN;
}

iERR _ion_reader_step_out_helper(ION_READER *preader)
{
    iENTER;

    ASSERT(preader);

    switch(preader->type) {
    case ion_type_text_reader:
        // IONCHECK(_ion_reader_text_step_out((ION_READER_TEXT *)preader));
        IONCHECK(_ion_reader_text_step_out(preader));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_step_out(preader));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    // keep the readers copy of depth up to date
    preader->_depth--;

    iRETURN;
}

iERR ion_reader_get_depth(hREADER hreader, SIZE *p_depth)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_depth) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_depth_helper(preader, p_depth));

    iRETURN;
}

iERR _ion_reader_get_depth_helper(ION_READER *preader, SIZE *p_depth)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_depth);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_depth(preader, p_depth));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_depth(preader, p_depth));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_get_type(hREADER hreader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value_type) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_type_helper(preader, p_value_type));

    iRETURN;
}

iERR _ion_reader_get_type_helper(ION_READER *preader, ION_TYPE *p_value_type)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value_type);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_type(preader, p_value_type));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_type(preader, p_value_type));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_has_any_annotations(hREADER hreader, BOOL *p_has_annotations)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_has_annotations) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_has_any_annotations_helper(preader, p_has_annotations));

    iRETURN;
}

iERR _ion_reader_has_any_annotations_helper(ION_READER *preader, BOOL *p_has_annotations)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_has_annotations);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_has_any_annotations(preader, p_has_annotations));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_has_any_annotations(preader, p_has_annotations));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_has_annotation(hREADER hreader, iSTRING annotation, BOOL *p_annotation_found)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!annotation)                    FAILWITH(IERR_INVALID_ARG);
    if (ION_STRING_IS_NULL(annotation)) FAILWITH(IERR_INVALID_ARG);
    if (annotation->length < 1)         FAILWITH(IERR_INVALID_ARG);
    if (!p_annotation_found)            FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_has_annotation_helper(preader, annotation, p_annotation_found));

    iRETURN;
}

iERR _ion_reader_has_annotation_helper(ION_READER *preader, ION_STRING *annotation, BOOL *p_annotation_found)
{
    iENTER;

    ASSERT(preader);
    ASSERT(annotation);
    ASSERT(p_annotation_found);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_has_annotation(preader, annotation, p_annotation_found));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_has_annotation(preader, annotation, p_annotation_found));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_get_annotation_count(hREADER hreader, int32_t *p_count)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_count) FAILWITH(IERR_INVALID_ARG);
    
    
    IONCHECK(_ion_reader_get_annotation_count_helper(preader, p_count));

    iRETURN;
}

iERR _ion_reader_get_annotation_count_helper(ION_READER *preader, int32_t *p_count)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_count);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_annotation_count(preader, p_count));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_annotation_count(preader, p_count));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_get_an_annotation(hREADER hreader, int idx, iSTRING p_str)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (idx < 0) FAILWITH(IERR_INVALID_ARG);
    if (!p_str) FAILWITH(IERR_INVALID_ARG);
    
    
    IONCHECK(_ion_reader_get_an_annotation_helper(preader, idx, p_str));

    iRETURN;
}

iERR _ion_reader_get_an_annotation_helper(ION_READER *preader, int32_t idx, ION_STRING *p_str)
{
    iENTER;

    ASSERT(preader);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_an_annotation(preader, idx, p_str));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_an_annotation(preader, idx, p_str));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_is_null(hREADER hreader, BOOL *p_is_null)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_is_null) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_is_null_helper(preader, p_is_null));

    iRETURN;
}

iERR _ion_reader_is_null_helper(ION_READER *preader, BOOL *p_is_null)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_is_null);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_is_null(preader, p_is_null));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_is_null(preader, p_is_null));
        break;
    }

    iRETURN;
}

iERR ion_reader_get_field_name(hREADER hreader, iSTRING p_str)
{
    iENTER;
    ION_STRING *pstr;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_str)   FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_field_name_helper(preader, &pstr));
    if (!pstr || ION_STRING_IS_NULL(pstr)) {
        ION_STRING_INIT(p_str);
    }
    else {
        // IONCHECK(ion_string_copy_to_owner(hreader, p_str, pstr));
        ION_STRING_ASSIGN(p_str, pstr);
    }

    iRETURN;
}

iERR _ion_reader_get_field_name_helper(ION_READER *preader, ION_STRING **p_pstr)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_pstr);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_field_name(preader, p_pstr));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_field_name(preader, p_pstr));
        break;
    }

    iRETURN;
}

iERR ion_reader_get_field_sid(hREADER hreader, SID *p_sid)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_sid) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_field_sid_helper(preader, p_sid));

    iRETURN;
}

iERR _ion_reader_get_field_sid_helper(ION_READER *preader, SID *p_sid)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_sid);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_field_sid(preader, p_sid));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_field_sid(preader, p_sid));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    if (*p_sid <= UNKNOWN_SID) {
        FAILWITH(IERR_INVALID_SYMBOL);
    }

    iRETURN;
}

iERR ion_reader_get_annotations(hREADER hreader, iSTRING p_strs, SIZE max_count, SIZE *p_count)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_strs) FAILWITH(IERR_INVALID_ARG);
    if (!p_count) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_annotations_helper(preader, p_strs, max_count, p_count));

    iRETURN;
}

iERR _ion_reader_get_annotations_helper(ION_READER *preader, ION_STRING *p_strs, SIZE max_count, SIZE *p_count)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_strs);
    ASSERT(p_count);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_annotations(preader, p_strs, max_count, p_count));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_annotations(preader, p_strs, max_count, p_count));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_get_annotation_sids(hREADER hreader, SID *p_sids, SIZE max_count, SIZE *p_count)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_sids) FAILWITH(IERR_INVALID_ARG);
    if (!p_count) FAILWITH(IERR_INVALID_ARG);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_annotation_sids(preader, p_sids, max_count, p_count));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_annotation_sids(preader, p_sids, max_count, p_count));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_null(hREADER hreader, ION_TYPE *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_null_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_null_helper(ION_READER *preader, ION_TYPE *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_null(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_null(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_bool(hREADER hreader, BOOL *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_bool_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_bool_helper(ION_READER *preader, BOOL *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_bool(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_bool(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_int(hREADER hreader, int *p_value)
{
    iENTER;
    int64_t int64Value = 0;
    // May return NUMERIC_OVERFLOW
    IONCHECK(_ion_reader_read_int64_helper(hreader, &int64Value));
    *p_value = (int)int64Value;
    if (*p_value != int64Value) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR ion_reader_read_int32(hREADER hreader, int32_t *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_int32_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_int32_helper(ION_READER *preader, int32_t *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_int32(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_int32(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_int64(hREADER hreader, int64_t *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_int64_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_int64_helper(ION_READER *preader, int64_t *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_int64(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_int64(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR _ion_reader_read_mixed_int_helper(ION_READER *preader)
{
    iENTER;

    ASSERT(preader);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_mixed_int_helper(preader));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_binary_read_mixed_int_helper(preader));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_long(hREADER hreader, long *p_value)
{
    iENTER;
    int64_t int64Value = 0;
    // May return NUMERIC_OVERFLOW
    IONCHECK(_ion_reader_read_int64_helper(hreader, &int64Value));
    *p_value = (long)int64Value;
    if (*p_value != int64Value) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR ion_reader_read_ion_int(hREADER hreader, ION_INT *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_ion_int_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_ion_int_helper(ION_READER *preader, ION_INT *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_ion_int_helper(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_ion_int(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_double(hREADER hreader, double *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_double_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_double_helper(ION_READER *preader, double *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_double(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_double(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_decimal(hREADER hreader, decQuad *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_decimal_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_decimal_helper(ION_READER *preader, decQuad *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_decimal(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_decimal(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_timestamp(hREADER hreader, iTIMESTAMP p_value)
{
    iENTER;
    ION_READER    *preader;
    ION_TIMESTAMP *users_copy, temp_timestamp;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_timestamp_helper(preader, &temp_timestamp));
    if (!p_value) {
        users_copy = (ION_TIMESTAMP *)ion_alloc_with_owner(hreader, sizeof(ION_TIMESTAMP));
        if (!users_copy) FAILWITH(IERR_NO_MEMORY);
    }
    else {
        users_copy = p_value;
    }
    memcpy(users_copy, &temp_timestamp, sizeof(ION_TIMESTAMP));

    iRETURN;
}

iERR _ion_reader_read_timestamp_helper(ION_READER *preader, ION_TIMESTAMP *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_timestamp(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_timestamp(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_symbol_sid(hREADER hreader, SID *p_value)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_symbol_sid_helper(preader, p_value));

    iRETURN;
}

iERR _ion_reader_read_symbol_sid_helper(ION_READER *preader, SID *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_symbol_sid(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_symbol_sid(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_get_string_length(hREADER hreader, SIZE *p_length)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_length) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_string_length_helper(preader, p_length));

    iRETURN;
}

iERR _ion_reader_get_string_length_helper(ION_READER *preader, SIZE *p_length)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_length);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_string_length(preader, p_length));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_string_length(preader, p_length));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_string(hREADER hreader, iSTRING p_value)
{
    iENTER;
    ION_READER *preader;
    ION_STRING  str;

    ION_STRING_INIT(&str);

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_value) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_string_helper(preader, &str));
    if (ION_STRING_IS_NULL(&str)) {
        FAILWITH(IERR_NULL_VALUE);
    }
    else {
        ION_STRING_ASSIGN(p_value, &str);
    }

    iRETURN;
}

iERR _ion_reader_read_string_helper(ION_READER *preader, ION_STRING *p_value)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_value);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_string(preader, p_value));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_string(preader, p_value));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_partial_string (hREADER hreader, BYTE *p_buf, SIZE buf_max, SIZE *p_length)
{
    iENTER;
    ION_READER *preader = HANDLE_TO_PTR(hreader, ION_READER);
    SIZE        read_length;

    if (!hreader)    FAILWITH(IERR_INVALID_ARG);
    if (!p_buf)      FAILWITH(IERR_INVALID_ARG);
    if (buf_max < 0) FAILWITH(IERR_INVALID_ARG);
    if (!p_length)   FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_partial_string_helper(preader, TRUE, p_buf, buf_max, &read_length));
    *p_length = read_length;

    iRETURN;
}

iERR _ion_reader_read_partial_string_helper(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length) 
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_buf);
    ASSERT(buf_max);
    ASSERT(p_length);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_string_bytes(preader, accept_partial, p_buf, buf_max, p_length));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_string_bytes(preader, accept_partial, p_buf, buf_max, p_length));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }
    iRETURN;
}

iERR ion_reader_get_lob_size(hREADER hreader, SIZE *p_length)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_length) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_get_lob_size_helper(preader, p_length));

    iRETURN;
}

iERR _ion_reader_get_lob_size_helper(ION_READER *preader, SIZE *p_length)
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_length);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_lob_size(preader, p_length));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_lob_size(preader, p_length));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR ion_reader_read_lob_bytes(hREADER hreader, BYTE *p_buf, SIZE buf_max, SIZE *p_length)
{
    iENTER;
    ION_READER *preader = HANDLE_TO_PTR(hreader, ION_READER);

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    if (!p_buf) FAILWITH(IERR_INVALID_ARG);
    if (buf_max < 0) FAILWITH(IERR_INVALID_ARG);
    if (!p_length) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_lob_bytes_helper(preader, FALSE, p_buf, buf_max, p_length));

    iRETURN;
}

iERR ion_reader_read_lob_partial_bytes(hREADER hreader, BYTE *p_buf, SIZE buf_max, SIZE *p_length)
{
    iENTER;
    ION_READER *preader = HANDLE_TO_PTR(hreader, ION_READER);
    SIZE        read_length;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    if (!p_buf) FAILWITH(IERR_INVALID_ARG);
    if (buf_max < 0) FAILWITH(IERR_INVALID_ARG);
    if (!p_length) FAILWITH(IERR_INVALID_ARG);

    IONCHECK(_ion_reader_read_lob_bytes_helper(preader, TRUE, p_buf, buf_max, &read_length));

    *p_length = read_length;

    iRETURN;
}

iERR _ion_reader_read_lob_bytes_helper(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length) 
{
    iENTER;

    ASSERT(preader);
    ASSERT(p_buf);
    ASSERT(buf_max);
    ASSERT(p_length);

    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_read_lob_bytes(preader, accept_partial, p_buf, buf_max, p_length));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_read_lob_bytes(preader, accept_partial, p_buf, buf_max, p_length));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }
    iRETURN;
}

iERR ion_reader_close(hREADER hreader)
{
    iENTER;
    ION_READER *preader;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);

    IONCHECK(_ion_reader_close_helper(preader));

    iRETURN;
}

iERR _ion_reader_close_helper(ION_READER *preader)
{
    iENTER;

    ASSERT(preader);

    switch(preader->type) {
    case ion_type_text_reader:
        UPDATEERROR(_ion_reader_text_close(preader));
        break;
    case ion_type_binary_reader:
        UPDATEERROR(_ion_reader_binary_close(preader));
        break;
    case ion_type_unknown_reader:
    default:
        UPDATEERROR(IERR_INVALID_STATE);
    }

    // in both cases we need to release the stream and
    // then free whatever memory we have attached to the reader
    // Close stream.
    if (preader->_reader_owns_stream) {
        ion_stream_close(preader->istream);
    }
    preader->istream = NULL;

    if (preader->_temp_entity_pool != NULL) {
        ion_free_owner( preader->_temp_entity_pool );
        preader->_temp_entity_pool = NULL;
    }

    if (preader->_local_symtab_pool != NULL) {
        ion_free_owner( preader->_local_symtab_pool );
        preader->_local_symtab_pool = NULL;
    }

    ion_free_owner(preader);
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_reset_temp_pool( ION_READER *preader )
{
    iENTER;

    if ((preader->_temp_entity_pool != NULL)
     && ((ION_READER *)(preader->_temp_entity_pool) != preader)
    ) {
        ion_free_owner( preader->_temp_entity_pool );
        preader->_temp_entity_pool = NULL;
    }

    // alloc _temp_entity_pool here
    preader->_temp_entity_pool = ion_alloc_owner(sizeof(int));  // this is a fake allocation to hold the pool
    if (preader->_temp_entity_pool == NULL) {
        FAILWITH(IERR_NO_MEMORY);
    }

    iRETURN;
}

iERR _ion_reader_get_new_local_symbol_table_owner(ION_READER *preader, void **p_owner )
{
    iENTER;
    void *owner;

    // recycle the old symtab if there is one
    IONCHECK(_ion_reader_reset_local_symbol_table(preader));

    // allocate a pool, save it as our local symbol table pool and return it
    owner = ion_alloc_owner(sizeof(int));  // this is a fake allocation to hold the pool
    if (owner == NULL) {
        FAILWITH(IERR_NO_MEMORY);
    }
    preader->_local_symtab_pool = owner;
    *p_owner = owner;

    iRETURN;
}

iERR _ion_reader_reset_local_symbol_table(ION_READER *preader )
{
    iENTER;

    // recycle the old symtab if there is one
    if (preader->_local_symtab_pool != NULL) {
        ion_free_owner( preader->_local_symtab_pool );
        preader->_local_symtab_pool = NULL;
    }
    SUCCEED();

    iRETURN;
}

iERR ion_reader_get_position(hREADER hreader, int64_t *p_bytes, int32_t *p_line, int32_t *p_offset)
{
    iENTER;
    ION_READER *preader;
    int64_t bytes = -1;
    int32_t lines = -1, offset = -1;

    if (hreader) {
        preader = HANDLE_TO_PTR(hreader, ION_READER);
        IONCHECK(_ion_reader_get_position_helper(preader, &bytes, &lines, &offset));
    }

    *p_bytes  = bytes;
    *p_line   = lines;
    *p_offset = offset;

    iRETURN;
}

iERR _ion_reader_get_position_helper(ION_READER *preader, int64_t *p_bytes, int32_t *p_line, int32_t *p_offset)
{
    iENTER;

    ASSERT( preader );
    ASSERT( p_bytes );
    ASSERT( p_line );
    ASSERT( p_offset );

    switch(preader->type) {
    case ion_type_text_reader:
        *p_line   = preader->typed_reader.text._scanner._line;
        *p_offset = preader->typed_reader.text._scanner._offset;
        // fall through to binary to get the "bytes read" from the input stream
    case ion_type_binary_reader:
        *p_bytes  = ion_stream_get_position(preader->istream);
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}


//-----------------------------------------------------------
//   SEEK RELATED FUNCTIONS
//-----------------------------------------------------------


/** moves the stream position to the specified offset. Resets the 
 *  the state of the reader to be at the top level. As long as the
 *  specified position is at the first byte of a value (just before 
 *  the type description byte) this will work neatly. If the seek
 *  is into the middle of a value (including a collection) the
 *  view of the data is likely to be invalid.
 *
 *  if a length is specified (default is -1 or no limit) eof will
 *  be returned when length bytes are consumed.
 *
 *  A common pattern when using this interface would be to open
 *  the reader from an in memory buffer stream or a seek-able
 *  file handle.  Then call ion_reader_next which will read the
 *  ion version marker and the initial local symbol table (if one
 *  is present).  At that point the symbol table will be current
 *  and later seek's will have an appropriate symbol table to use.
 */
iERR ion_reader_seek(hREADER hreader, POSITION offset, SIZE length)
{
    iENTER;
    ION_READER *preader;
    ION_STREAM *pstream;
    POSITION    local_end;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    if (offset < 0) FAILWITH(IERR_INVALID_ARG);

    preader = HANDLE_TO_PTR(hreader, ION_READER);

    /*
        does reader have seekable stream? (seek or offset > current position)
        clear exising value
        reset memory on reader (except local symbol table)
        set stack to 0
        flag as running from seek (???)
        reset underlying reader
            binary - state
            text - parser state and tokenizer
        seek on stream
        set local eof (if length >= 0)

    */

    // we'll let the steam_seek API decide if it can seek or not
    pstream = preader->istream;
    IONCHECK(ion_stream_seek(pstream, offset));
    if (length >= 0) {
        local_end = offset + length;
    }
    else {
        local_end = -1;
    }

    // here we reset what little state the reader need to address directly
    preader->_eof = FALSE;
    IONCHECK(_ion_reader_reset_temp_pool( preader ));
    memset(&preader->_int_helper, 0, sizeof(preader->_int_helper));

    // and then we reset the underlying parser (text or binary)
    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_reset(preader, tid_DATAGRAM, local_end));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_text_reset(preader, tid_DATAGRAM, local_end));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    // keep the readers copy of depth up to date
    preader->_depth = 0;


    // the parser reset will set the local terminator (at least it will
    // for binary). Here we set the readers EOF if there are any bytes to read
    //preader->_eof = (length == 0);

    iRETURN;
}

/** set the current symbol table to the table passed in.  This 
 *  can be used to reset the readers symbol
 *  table is you wish to seek in a stream which contains multiple
 *  symbol tables.  This symbol table handle should be a handle
 *  returned by ion_reader_get_symbol_table.
 */
iERR ion_reader_set_symbol_table(hREADER hreader, hSYMTAB hsymtab)
{
    iENTER;
    ION_READER       *preader;
    ION_SYMBOL_TABLE *symtab;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!hsymtab) FAILWITH(IERR_INVALID_ARG);
    symtab = HANDLE_TO_PTR(hsymtab, ION_SYMBOL_TABLE);
    
    
    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_set_symbol_table(preader, symtab));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_set_symbol_table(preader, symtab));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

/** returns the offset of the value the reader is currently
 *  positioned on.  This offset is appropriate to use later
 *  to seek to.
 */
iERR ion_reader_get_value_offset(hREADER hreader, POSITION *p_offset)
{
    iENTER;
    ION_READER *preader;
    POSITION    offset;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_offset) FAILWITH(IERR_INVALID_ARG);
    
    
    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_value_offset(preader, &offset));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_value_offset(preader, &offset));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_offset = offset;
    SUCCEED();

    iRETURN;
}

/** returns the length of the value the reader is currently
 *  positioned on.  This length is appropriate to use later
 *  when calling ion_reader_seek to limit "over-reading" in
 *  the underlying stream which could result in errors that
 *  are not really of intereest.
 */
iERR ion_reader_get_value_length(hREADER hreader, SIZE *p_length)
{
    iENTER;
    ION_READER       *preader;
    SIZE              len;

    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_length) FAILWITH(IERR_INVALID_ARG);
    
    
    switch(preader->type) {
    case ion_type_text_reader:
        IONCHECK(_ion_reader_text_get_value_length(preader, &len));
        break;
    case ion_type_binary_reader:
        IONCHECK(_ion_reader_binary_get_value_length(preader, &len));
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_length = len;
    SUCCEED();

    iRETURN;
}

/** returns the current symbol table the value the reader is currently
 *  positioned on.  This can be used to reset the readers symbol
 *  table is you wish to seek in a stream which contains multiple
 *  symbol tables.  This symbol table handle can be used to call
 *  ion_reader_set_symbol_table.
 */
iERR ion_reader_get_symbol_table(hREADER hreader, hSYMTAB *p_hsymtab)
{
    iENTER;
    ION_SYMBOL_TABLE *psymtab;
    ION_READER       *preader;
    
    if (!hreader) FAILWITH(IERR_INVALID_ARG);
    preader = HANDLE_TO_PTR(hreader, ION_READER);
    if (!p_hsymtab) FAILWITH(IERR_INVALID_ARG);
    
    
    IONCHECK(_ion_reader_get_symbol_table_helper(preader, &psymtab));
    *p_hsymtab = PTR_TO_HANDLE(psymtab);
    
    iRETURN;
}

iERR _ion_reader_get_symbol_table_helper(ION_READER *preader, ION_SYMBOL_TABLE **p_psymtab)
{
    iENTER;
    ION_SYMBOL_TABLE *psymtab;
    
    ASSERT(preader);
    ASSERT(p_psymtab);
    
    switch(preader->type) {
    case ion_type_text_reader:
    case ion_type_binary_reader:
            psymtab = preader->_current_symtab;
        break;
    case ion_type_unknown_reader:
    default:
        FAILWITH(IERR_INVALID_STATE);
    }
    
    *p_psymtab = psymtab;
    
    iRETURN;
}
