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

#ifndef ION_WRITER_H_
#define ION_WRITER_H_

#include "ion_types.h"
#include "ion_platform_config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _ion_writer_options
{
    BOOL output_as_binary;

    /** On text output, this escapes non-ascii characters. So the output file is pure ascii, only valid for text output
     *
     */
    BOOL escape_all_non_ascii;

    /** Turns on pretty printing, only valid for text output
     *
     */
    BOOL pretty_print;

    /** Uses tabs for indenting instead of spaces
     *
     */
    BOOL indent_with_tabs;

    /** Sets indent amount for pretty printing. Defaults to 2. Ignored if indent_with_tabs is true.
     *
     */
    SIZE indent_size;

    /** Puts "small" containers on a single line instead of putting all values on separate lines
     *
     */
    BOOL small_containers_in_line;

    /** Turns off the otherwise automatic insertion of system values (like symbol tables)
     *
     */
    BOOL supress_system_values;

    /** Turns on very active flushing of the internal streams (text only)
     *
     */
    BOOL flush_every_value;

    /** The max container depth defaults to 10
     *
     */
    SIZE max_container_depth;

    /** The max number of annotations on 1 value, defaults to 10
     *
     */
    SIZE max_annotation_count;

    /** The temp buffer is used to hold temp strings (etc) default is 1024
     *
     */
    SIZE temp_buffer_size;

    /** memory is allocated in pages owned by the primary entities it's default size is 4096
     *
     */
    SIZE allocation_page_size;

    /** Handle to catalog of shared symbol tables for the writer to use
     *
     */
    ION_CATALOG *pcatalog;

    /** An ordered list of ION_SYMBOL_TABLE_IMPORT that the writer will import into each new local
     *  symbol table context. Should be initialized by calling `ion_writer_options_initialize_shared_imports`,
     *  populated by calling `ion_writer_options_add_shared_imports` and/or
     *  `ion_writer_options_add_shared_imports_symbol_tables`, and freed by calling
     *  `ion_writer_options_close_shared_imports`.
     *
     *  NOTE: the system symbol table is always used as the first import; it need not be provided here.
     */
    ION_COLLECTION encoding_psymbol_table;

    /** Handle to the decNumber context for the writer to use. This allows configuration of the maximum number of
     * decimal digits, decimal exponent range, etc. See decContextDefault in decContext.h for simple initialization.
     *
     * If NULL, the writer will initialize its decimal context by calling decContextDefault with the DEC_INIT_DECQUAD
     * option, which results in a maximum of 34 decimal digits and an exponent range of [-6143, 6144].
     *
     * Note that up to 34 digits of precision will always be supported, even if configured to be less than 34.
     */
    decContext *decimal_context;

    /** Normally floating point numbers (`float` or `double`) are written as 32-bit or 64-bit Ion floats depending
     *  on which Ion writer API is used. When enabled, this API allows the writer to represent a 64-bit float
     *  with only 32 bits whenever possible without losing precision.
     *
     */
    BOOL compact_floats;

    /** Enable JSON downconversion for the writer.
     * This will cause the writer to produce valid JSON, losing any Ion specific features.
     */
    BOOL json_downconvert;

} ION_WRITER_OPTIONS;


/**
 * Initializes the options' imports list. This must be done before calling `ion_writer_options_add_*`.
 * NOTE: This does NOT need to be called if the writer does not need to use shared imports.
 * @param options - The writer options containing the imports list to initialize.
 */
ION_API_EXPORT iERR ion_writer_options_initialize_shared_imports(ION_WRITER_OPTIONS *options);

/**
 * Adds the imports from the given collection of ION_SYMBOL_TABLE_IMPORT to the options' imports list.
 * `ion_writer_options_initialize_shared_imports` must have been called first. The given collection must not contain
 * a system symbol table.
 */
ION_API_EXPORT iERR ion_writer_options_add_shared_imports(ION_WRITER_OPTIONS *options, ION_COLLECTION *imports);

/**
 * Adds the given array of ION_SYMBOL_TABLE (which must be shared symbol tables) to the options' imports list.
 * `ion_writer_options_initialize_shared_imports` must have been called first. The given array must not contain
 * a system symbol table.
 */
ION_API_EXPORT iERR ion_writer_options_add_shared_imports_symbol_tables(ION_WRITER_OPTIONS *options, ION_SYMBOL_TABLE **imports, SIZE imports_count);

/**
 * Frees the options' imports list. This must be done once the options are no longer needed, and only if
 * `ion_writer_options_initialize_shared_imports` was called.
 */
ION_API_EXPORT iERR ion_writer_options_close_shared_imports(ION_WRITER_OPTIONS *options);

/** Ion Writer interfaces. Takes a byte buffer and length which
 *  will contain the text or binary content, returns handle to a writer.
 *  @param  p_hwriter
 *  @param  buffer  Byte buffer, allocated and provided by caller.
 *  @param  buf_length  size of the buffer (0 or greater). If the buffer is not big enough, ion_write
 *                      operation will return IERR_EOF rather than IERR_OK.
 *  @param  p_option    writer configuration object.
 */
ION_API_EXPORT iERR ion_writer_open_buffer          (hWRITER *p_hwriter
                                                    ,BYTE *buffer
                                                    ,SIZE buf_length
                                                    ,ION_WRITER_OPTIONS *p_options);


/** Open stream to write ion data.
 * @param   p_hwriter
 * @param   fn_block_handler    User provided function to write from handler_state buffer to file,
 * @param   handler_state       Related to write buffer. ion_writer will write to the buffer provided by the handler_state,
 *                              fn_block_handler will write the buffer to file.
 * @param   p_options           writer configuration object.
 * @see ion_reader_open_stream
 * @see ion_writer_open_buffer
 */
ION_API_EXPORT iERR ion_writer_open_stream          (hWRITER *p_hwriter
                                                    ,ION_STREAM_HANDLER fn_output_handler
                                                    ,void *handler_state
                                                    ,ION_WRITER_OPTIONS *p_options);

ION_API_EXPORT iERR ion_writer_open                 (hWRITER *p_hwriter
                                                    ,ION_STREAM *p_stream
                                                    ,ION_WRITER_OPTIONS *p_options);

ION_API_EXPORT iERR ion_writer_get_depth            (hWRITER hwriter, SIZE *p_depth);

ION_API_EXPORT iERR ion_writer_set_catalog          (hWRITER hwriter, hCATALOG    hcatalog);
ION_API_EXPORT iERR ion_writer_get_catalog          (hWRITER hwriter, hCATALOG *p_hcatalog);

/**
 * Sets the writer's symbol table.
 *
 * If the writer's current symbol table context must be serialized, forces the writer to finish and flush its current
 * symbol table context (with the same side-effects as `ion_writer_finish`) first. If the given symbol table is a shared
 * symbol table, a new local symbol table that imports that shared symbol table is created. Raises an error if a
 * manually-written symbol table is in progress or if the writer is not at the top level.
 */
ION_API_EXPORT iERR ion_writer_set_symbol_table     (hWRITER hwriter, hSYMTAB     hsymtab);
ION_API_EXPORT iERR ion_writer_get_symbol_table     (hWRITER hwriter, hSYMTAB  *p_hsymtab);

/**
 * Adds the given list of imports to the writer's list of imports. These imports will only be used in the writer's
 * current symbol table context. To configure the writer to use the same list of imports for each new symbol table
 * context, convey that list of imports through ION_WRITER_OPTIONS.
 *
 * If the writer's current symbol table context must be serialized, forces the writer to finish and flush its current
 * symbol table context (with the same side-effects as `ion_writer_finish`) first. A new symbol table context is then
 * created, starting with any imports specified in ION_WRITER_OPTIONS, and followed by the list of imports given to this
 * function.
 *
 * This function may be called multiple times in succession without changing the current symbol table context as long as
 * no values have been written in between calls; in this case, this function appends to the writer's list of imports.
 *
 * Raises an error if a manually-written symbol table is in progress, if the writer is not at the top level, or if the
 * writer has pending annotations.
 */
ION_API_EXPORT iERR ion_writer_add_imported_tables  (hWRITER hwriter, ION_COLLECTION *imports);

/**
 * Sets the writer's current field name. Only valid if the writer is currently in a struct. It is the caller's
 * responsibility to keep `name` in scope until the writer's next value is written.
 */
ION_API_EXPORT iERR ion_writer_write_field_name     (hWRITER hwriter, iSTRING name);

/**
 * Sets the writer's current field name from the given Ion symbol. Only valid if the writer is currently in a struct.
 * It is the caller's responsibility to keep `field_name` in scope until the writer's next value is written.
 */
ION_API_EXPORT iERR ion_writer_write_field_name_symbol(hWRITER hwriter, ION_SYMBOL *field_name);

ION_API_EXPORT iERR ion_writer_clear_field_name     (hWRITER hwriter);

/**
 * It is the caller's responsibility to keep `annotation` string in scope until the writer's annotations are cleared.
 */
ION_API_EXPORT iERR ion_writer_add_annotation       (hWRITER hwriter, iSTRING annotation);

ION_API_EXPORT iERR ion_writer_add_annotation_symbol(hWRITER hwriter, ION_SYMBOL *annotation);
ION_API_EXPORT iERR ion_writer_write_annotations    (hWRITER hwriter, iSTRING p_annotations, SIZE count);
ION_API_EXPORT iERR ion_writer_write_annotation_symbols(hWRITER hwriter, ION_SYMBOL *annotations, SIZE count);
ION_API_EXPORT iERR ion_writer_clear_annotations    (hWRITER hwriter);

ION_API_EXPORT iERR ion_writer_write_null           (hWRITER hwriter);
ION_API_EXPORT iERR ion_writer_write_typed_null     (hWRITER hwriter, ION_TYPE type);
ION_API_EXPORT iERR ion_writer_write_bool           (hWRITER hwriter, BOOL value);
ION_API_EXPORT iERR ion_writer_write_int            (hWRITER hwriter, int value);
ION_API_EXPORT iERR ion_writer_write_int32          (hWRITER hwriter, int32_t value);
ION_API_EXPORT iERR ion_writer_write_int64          (hWRITER hwriter, int64_t value);
ION_API_EXPORT iERR ion_writer_write_long           (hWRITER hwriter, long value);
ION_API_EXPORT iERR ion_writer_write_ion_int        (hWRITER hwriter, ION_INT *value);
ION_API_EXPORT iERR ion_writer_write_double         (hWRITER hwriter, double value);
ION_API_EXPORT iERR ion_writer_write_float          (hWRITER hwriter, float value);

/**
 * @deprecated use of decQuads directly is deprecated. ION_DECIMAL should be used. See `ion_writer_write_ion_decimal`.
 */
ION_API_EXPORT iERR ion_writer_write_decimal        (hWRITER hwriter, decQuad *value);
ION_API_EXPORT iERR ion_writer_write_ion_decimal    (hWRITER hwriter, ION_DECIMAL *value);
ION_API_EXPORT iERR ion_writer_write_timestamp      (hWRITER hwriter, iTIMESTAMP value);
ION_API_EXPORT iERR ion_writer_write_symbol         (hWRITER hwriter, iSTRING p_value);
ION_API_EXPORT iERR ion_writer_write_ion_symbol     (hWRITER hwriter, ION_SYMBOL *symbol);
ION_API_EXPORT iERR ion_writer_write_string         (hWRITER hwriter, iSTRING p_value);
ION_API_EXPORT iERR ion_writer_write_clob           (hWRITER hwriter, BYTE *p_buf, SIZE length);
ION_API_EXPORT iERR ion_writer_write_blob           (hWRITER hwriter, BYTE *p_buf, SIZE length);

ION_API_EXPORT iERR ion_writer_start_lob            (hWRITER hwriter, ION_TYPE lob_type);
ION_API_EXPORT iERR ion_writer_append_lob           (hWRITER hwriter, BYTE *p_buf, SIZE length);
ION_API_EXPORT iERR ion_writer_finish_lob           (hWRITER hwriter);
ION_API_EXPORT iERR ion_writer_start_container      (hWRITER hwriter, ION_TYPE container_type);
ION_API_EXPORT iERR ion_writer_finish_container     (hWRITER hwriter);

ION_API_EXPORT iERR ion_writer_write_one_value      (hWRITER hwriter, hREADER hreader);
ION_API_EXPORT iERR ion_writer_write_all_values     (hWRITER hwriter, hREADER hreader);

/**
 * Flushes pending bytes without forcing an Ion Version Marker or ending the current symbol table context.
 * If writer was created using open_stream, also flushes write buffer to stream. If any value is in-progress, flushing
 * any writer is an error.
 * @param   p_bytes_flushed - the number of bytes written into the buffer/stream.
 */
ION_API_EXPORT iERR ion_writer_flush                (hWRITER hwriter, SIZE *p_bytes_flushed);

/**
 * Flushes pending bytes, ending the current symbol table context and forcing an Ion Version Marker if the writer
 * continues writing to the stream. If writer was created using open_stream, also flushes write buffer to stream.
 * If any value is in-progress, finishing any writer is an error.
 * @param   p_bytes_flushed - the number of bytes written into the buffer/stream.
 */
ION_API_EXPORT iERR ion_writer_finish               (hWRITER hwriter, SIZE *p_bytes_flushed);

/**
 * Finishes the writer, frees the writer's associated resources, and finally frees the writer itself. The writer may
 * not continue writing to the stream after this function is called. If any value is in-progress, closing any writer
 * raises an error, but still frees the writer and any associated memory.
 */
ION_API_EXPORT iERR ion_writer_close                (hWRITER hwriter);

#ifdef __cplusplus
}
#endif


#endif
