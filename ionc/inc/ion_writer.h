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

    /** Sets the default indent amount (default is 2)
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

    /** Pointer to the first element of an array of shared symbol tables that the writer will import into each new local
     *  symbol table context. The size of the array is specified by `encoding_psymbol_table_count`. The user owns the
     *  associated memory, and must ensure it stays in scope for the lifetime of the writer.
     *  NOTE: the system symbol table is always used as the first import; it need not be provided here.
     */
    ION_SYMBOL_TABLE *encoding_psymbol_table;

    /** Number of symbol tables in the `encoding_psymbol_table` array.
     *
     */
    SIZE encoding_psymbol_table_count;

    /** Handle to the decNumber context for the writer to use. This allows configuration of the maximum number of
     * decimal digits, decimal exponent range, etc. See decContextDefault in decContext.h for simple initialization.
     *
     * If NULL, the writer will initialize its decimal context by calling decContextDefault with the DEC_INIT_DECQUAD
     * option, which results in a maximum of 34 decimal digits and an exponent range of [-6143, 6144].
     *
     * Note that up to 34 digits of precision will always be supported, even if configured to be less than 34.
     */
    decContext *decimal_context;

} ION_WRITER_OPTIONS;


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
ION_API_EXPORT iERR ion_writer_set_symbol_table     (hWRITER hwriter, hSYMTAB     hsymtab);
ION_API_EXPORT iERR ion_writer_get_symbol_table     (hWRITER hwriter, hSYMTAB  *p_hsymtab);

ION_API_EXPORT iERR ion_writer_write_field_name     (hWRITER hwriter, iSTRING name);
ION_API_EXPORT iERR ion_writer_write_field_sid      (hWRITER hwriter, SID sid);
ION_API_EXPORT iERR ion_writer_clear_field_name     (hWRITER hwriter);
ION_API_EXPORT iERR ion_writer_add_annotation       (hWRITER hwriter, iSTRING annotation);
ION_API_EXPORT iERR ion_writer_add_annotation_sid   (hWRITER hwriter, SID sid);
ION_API_EXPORT iERR ion_writer_write_annotations    (hWRITER hwriter, iSTRING *p_annotations, SIZE count);
ION_API_EXPORT iERR ion_writer_write_annotation_sids(hWRITER hwriter, SID *p_sids, SIZE count);
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

/**
 * @deprecated use of decQuads directly is deprecated. ION_DECIMAL should be used. See `ion_writer_write_ion_decimal`.
 */
ION_API_EXPORT iERR ion_writer_write_decimal        (hWRITER hwriter, decQuad *value);
ION_API_EXPORT iERR ion_writer_write_ion_decimal    (hWRITER hwriter, ION_DECIMAL *value);
ION_API_EXPORT iERR ion_writer_write_timestamp      (hWRITER hwriter, iTIMESTAMP value);
ION_API_EXPORT iERR ion_writer_write_symbol_sid     (hWRITER hwriter, SID value);
ION_API_EXPORT iERR ion_writer_write_symbol         (hWRITER hwriter, iSTRING p_value);
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
 * If writer was created using open_stream, also flushes write buffer to stream. If not at the top level, flushing a
 * binary writer will do nothing.
 * @param   p_bytes_flushed - the number of bytes written into the buffer/stream.
 */
ION_API_EXPORT iERR ion_writer_flush                (hWRITER hwriter, SIZE *p_bytes_flushed);

/**
 * Flushes pending bytes, ending the current symbol table context and forcing an Ion Version Marker if the writer
 * continues writing to the stream. If writer was created using open_stream, also flushes write buffer to stream.
 * If not at the top level, finishing any writer is an error.
 * @param   p_bytes_flushed - the number of bytes written into the buffer/stream.
 */
ION_API_EXPORT iERR ion_writer_finish               (hWRITER hwriter, SIZE *p_bytes_flushed);

/**
 * Finishes the writer, frees the writer's associated resources, and finally frees the writer itself. The writer may
 * not continue writing to the stream after this function is called. If not at the top level, closing any writer is an
 * error.
 */
ION_API_EXPORT iERR ion_writer_close                (hWRITER hwriter);

#ifdef __cplusplus
}
#endif


#endif
