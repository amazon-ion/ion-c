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
// defines for the readers, common, text and binary
//
#ifndef ION_READER_H_
#define ION_READER_H_
#include "ion_types.h"
#include "ion_stream.h"
#ifdef __cplusplus
extern "C" {
#endif

/**
 * A function that may be called by the reader upon a change to the stream's symbol table context.
 *
 * For example, an ION_READER_CONTEXT_CALLBACK function may be used to wrap `ion_writer_add_imported_tables`, with the
 * user context pointing to a writer instance, in order to update the writer's symbol table context in lockstep with
 * the reader.
 *
 * @param context - User-provided context, e.g., a hWRITER.
 * @param imports - A collection of ION_SYMBOL_TABLE_IMPORT, representing the shared symbol tables in the new symbol
 *  table context.
 */
typedef iERR (*ION_READER_CONTEXT_CALLBACK)(void *context, ION_COLLECTION *imports);

typedef struct _ion_reader_context_change_notifier {
    /**
     * The function to call upon a change to the reader's symbol table context.
     */
    ION_READER_CONTEXT_CALLBACK notify;

    /**
     * The user context to provide as the first argument to `notify`. May be NULL.
     */
    void *context;

} ION_READER_CONTEXT_CHANGE_NOTIFIER;


/** Reader configuration data, could be supplied by user during reader creation time.
 * All fields in the structure are defaulted to 0, except for the following:
 *
 * #define DEFAULT_ANNOTATION_LIMIT         10
 * #define DEFAULT_WRITER_STACK_DEPTH       10
 * #define DEFAULT_CHUNK_THRESHOLD     DEFAULT_BLOCK_SIZE
 * #define DEFAULT_SYMBOL_THRESHOLD        512
 *
 * Some field also has a range limit:
 * #define MIN_ANNOTATION_LIMIT              1
 * #define MIN_WRITER_STACK_DEPTH            2
 * #define MIN_SYMBOL_THRESHOLD             32
 * #define MIN_CHUNK_THRESHOLD              32
 * #define MIN_ION_ALLOCATION_BLOCK_SIZE    32
 *
 */
typedef struct _ion_reader_options
{
    /** If true the reader will return otherwise hidden system values
     *
     */
    BOOL return_system_values;

    /** Character to be treated as new line for line counting, defaults to '\n'
     *
     */
    int  new_line_char;

    /** The max container depth defaults to 10
     *
     */
    SIZE max_container_depth;

    /** The max number of annotations on 1 value, defaults to 10
     *
     */
    SIZE max_annotation_count;

    /** The max number number of bytes the annotations on a single value. This 
     *  is an total. How the bytes are divided among the annotations is irrelevant
     *  (i.e. 1 large, or 100 small may have the same total space requirements). 
     *  defaults to user_value_threshold (or 4096).
     *
     */
    SIZE max_annotation_buffered;

    /** The size maximum size allowed for symbols, 512 bytes is the default
     *
     */
    SIZE symbol_threshold;

    /** user value allocation threshold, max size of allocation made to process
     *  any value returned to the user, default is 4096. Includes symbol, int, 
     *  decimal, timestamp, blob values in all cases. This includes string, clob, 
     *  and blob values if they are to be returned to the caller in a contiguous
     *  buffer.
     *
     */
    SIZE user_value_threshold;

    /** The size over which long values are returned as chunks. This is only
     *  valid for string, clob and blob values as all others must be buffered
     *  up to the limit of user_value_threshold. The default is 4096.
     *
     */
    SIZE chunk_threshold;

    /** Memory is allocated in pages owned by the primary entities it's default size is 4096
     *
     */
    SIZE allocation_page_size;

    /** If true this will disable validation of string content which verifies the
     *  string returned is in fact a valid UTF-8 sequence.  This defaults to false.
     */
    BOOL skip_character_validation;

    /** Handle to catalog of shared symbol tables for the reader to use. If NULL, will be treated as empty.
     *
     */
    ION_CATALOG *pcatalog;

    /** Handle to the decNumber context for the reader to use. This allows configuration of the maximum number of
     * decimal digits, decimal exponent range, etc. See decContextDefault in decContext.h for simple initialization.
     *
     * If NULL, the reader will initialize its decimal context by calling decContextDefault with the DEC_INIT_DECQUAD
     * option, which results in a maximum of 34 decimal digits and an exponent range of [-6143, 6144].
     *
     * Note that up to 34 digits of precision will always be supported, even if configured to be less than 34.
     */
    decContext *decimal_context;

    /** Notification callback data to be used upon symbol table context change. Ignored if
     * `context_change_notifier.notify` is NULL.
     */
    ION_READER_CONTEXT_CHANGE_NOTIFIER context_change_notifier;

} ION_READER_OPTIONS;

//
// Ion Reader interface.  Takes a byte buffer and length which
// may have text or binary content, returns handle to a reader.
//

/**
 * Allocates a new reader consuming a given buffer of data.
 *
 * @param p_hreader will receive a pointer to the new reader.
 *   It must be freed via ion_reader_close().
 * @param buffer the Ion data to read, either UTF-8 text or binary.
 *   Must be a valid handle. The caller retains ownership of the buffer and
 *   must ensure that its data is unmodified until the reader is closed.
 * @param buf_length the length of the buffer
 * @param p_options may be null, in that case, default value will be used.
 */
ION_API_EXPORT iERR ion_reader_open_buffer(hREADER *p_hreader
                                          ,BYTE *buffer
                                          ,SIZE buf_length
                                          ,ION_READER_OPTIONS *p_options);

/** Create hREADER object, and associate it with the stream for reading.
 *
 * The hREADER object itself does not have a read data buffer, it's using the buffer from handler_state, which
 * usually contains a BYTE[].
 *
 * hREADER object will invoke fn_input_handler to read from input data source (File, for example) and fill
 * the BYTE[] in handler_state.
 *
    <pre>
    typedef struct _test_file {
        FILE *in;
        int   block_size;
        BYTE *buffer;
    } TEST_FILE;
    BYTE      g_buffer[1024];
    TEST_FILE g_test_file =
    {
        NULL,
        TEST_FILE_BUF_MAX,
        g_buffer
    };
    iERR test_stream_handler(struct _ion_stream *pstream)
    {
        iENTER;
        TEST_FILE *tfile;
        SIZE len;
        tfile = (TEST_FILE *)pstream->handler_state;
        pstream->curr = tfile->buffer;
        len = fread( tfile->buffer, sizeof(*tfile->buffer), tfile->block_size, g_test_file.in );
        if (len < 1) {
            pstream->limit = NULL;
            DONTFAILWITH(IERR_EOF);
        }
        pstream->limit = pstream->curr + len;
        iRETURN;
    }
    fstream = fopen(pathname, "r");
    g_test_file.in = fstream;
    ion_reader_open_stream(&reader, &g_test_file, test_stream_handler, NULL);
    </pre>
 *
 * @param   p_hreader           Newly created reader object will be stored here.
 * @param   p_stream            stream opened with ion_stream functions
 * @param   p_options const,    Reader configuration data object, used while creating reader object.
 * @return  IERR_OK if succeeded
 */
ION_API_EXPORT iERR ion_reader_open_stream(hREADER *p_hreader
                                          ,void *handler_state
                                          ,ION_STREAM_HANDLER fn_input_handler
                                          ,ION_READER_OPTIONS *p_options);

/** Resets input stream for given reader.
 *
 * readers' current stream would be closed and would be initialized with given stream.
 * catalog, symbol table, dec_context & other reader defaults would be reused [ @see _ion_reader_initialize impl.]
 *
 * @param   p_hreader   Reader object for which input stream is to be reset
 * @param   handler_state   Object that contains a buffer to store the read-in data from input stream.
 * @param   fn_input_handler    Function used to read from input stream and refill the buffer.
 *                              the handler is responsible for settting start, curr and limit of reader object
 *                              it may opt to change the handler_state if it wants to.
 *                              on this first call it is actually handling whatever initialization
 *                              it might need as well
 * @return  IERR_OK if succeeded
 */
ION_API_EXPORT iERR ion_reader_reset_stream(hREADER *p_hreader
                                           ,void *handler_state
                                           ,ION_STREAM_HANDLER fn_input_handler);

/** Resets input user-managed stream for given reader.
 *
 * Readers' current stream would be closed and would be initialized with given stream.
 * Resets the state of the reader to be at the top level. Symbol table and other reader state such as
 * whether its a binary or a text reader is retained.
 * A common pattern when using this interface would be to open the reader with a user-managed-stream,
 * Then call ion_reader_next which will read the
 * ion version marker and the initial local symbol table (if one
 * is present).  At that point the symbol table will be current.
 * A later user-managed-stream seek is immediately followed by calling this API.
 * This ensures symbol table is retained and reader/parser state is valid following the random jump;
 * and parsing continues unhindered from thereon.
 * @param   p_hreader            Reader object for which input stream is to be reset
 * @param   handler_state        Object that contains a buffer to store the read-in data from input stream.
 * @param   fn_input_handler     Function used to read from input stream and refill the buffer.
 *                               the handler is responsible for setting start, curr and limit of reader
 *                               object.
 * @param   length               Length of the user-managed-stream. An EOF by the reader when this length
 *                               is reached.
 * @return  IERR_OK if succeeded
 */
ION_API_EXPORT iERR ion_reader_reset_stream_with_length(hREADER *p_hreader
                                                       ,void *handler_state
                                                       ,ION_STREAM_HANDLER fn_input_handler
                                                       ,POSITION length);

ION_API_EXPORT iERR ion_reader_open                    (hREADER *p_hreader
                                                       ,ION_STREAM *p_stream
                                                       ,ION_READER_OPTIONS *p_options);
ION_API_EXPORT iERR ion_reader_get_catalog             (hREADER hreader, hCATALOG *p_hcatalog);

/** moves the stream position to the specified offset. Resets the 
 *  the state of the reader to be at the top level. As long as the
 *  specified position is at the first byte of a top-level value
 *  (just before the type description byte) this will work neatly.
 *  Do not attempt to seek to a value below the top level, as the
 *  view of the data is likely to be invalid.
 *
 *  If a length is specified (default is -1 or no limit) eof will
 *  be returned when length bytes are consumed.
 *
 *  A common pattern when using this interface would be to open
 *  the reader from an in memory buffer stream or a seek-able
 *  file handle.  Then call ion_reader_next which will read the
 *  ion version marker and the initial local symbol table (if one
 *  is present).  At that point the symbol table will be current
 *  and later seek's will have an appropriate symbol table to use.
 */
ION_API_EXPORT iERR ion_reader_seek                (hREADER  hreader
                                                   ,POSITION offset
                                                   ,SIZE     length);
/** set the current symbol table to the table passed in.  This 
 *  can be used to reset the readers symbol
 *  table is you wish to seek in a stream which contains multiple
 *  symbol tables.  This symbol table handle should be a handle
 *  returned by ion_reader_get_symbol_table.
 */
ION_API_EXPORT iERR ion_reader_set_symbol_table    (hREADER   hreader
                                                   ,hSYMTAB   hsymtab);
/** returns the offset of the value the reader is currently
 *  positioned on.  This offset is appropriate to use later
 *  to seek to.
 */
ION_API_EXPORT iERR ion_reader_get_value_offset    (hREADER   hreader
                                                   ,POSITION *p_offset);

/**
 * Gets the position of the current value in the reader.
 *
 * The position of the current value is the first character of its Ion-text
 * representation or (if present) the first character of the first
 * annotation.  This information is useful for reporting semantic errors
 * to end-users when Ion-text files are used to express domain-specific
 * language (DSL) scripts.
 *
 * The numbers reported by this function (p_line) start at 1, while the
 * column offset (p_col_offset) start at zero since it is the
 * offset from the start of the line.
 *
 * If the last call to `ion_reader_next` encountered a container
 * terminator (`]`, `)` or `}`), instead of another value, the position
 * reported is that of the terminator.  Thus, it is possible to obtain
 * the starting and ending positions of all containers.
 *
 * If the last call to `ion_reader_next` encountered the end of file
 * instead of another value, the position reported is that of the last
 * character in the file.
 *
 * If the reader is a binary reader, fails immediately and returns
 * IERR_INVALID_ARG.  If all that's desired is the offset of the value
 * relative to the beginning of the buffer, please use
 * ion_reader_get_value_offset, which works with both text and binary
 * readers.
 */
ION_API_EXPORT iERR ion_reader_get_value_position  (hREADER   hreader
                                                   ,int64_t   *p_offset
                                                   ,int32_t   *p_line
                                                   ,int32_t   *p_col_offset);

/** returns the length of the value the reader is currently
 *  positioned on.  This length is appropriate to use later
 *  when calling ion_reader_seek to limit "over-reading" in
 *  the underlying stream which could result in errors that
 *  are not really of interest. NOTE: readers of text data
 *  will always set *p_length to -1 because text Ion data is
 *  not length-prefixed. When the reader may be reading text
 *  Ion data, the correct way to calculate a value's length
 *  is by subtracting the current value's offset (see
 *  `ion_reader_get_value_offset`) from the next value's
 *  offset. This technique will work for both binary and text
 *  Ion data.
 */
ION_API_EXPORT iERR ion_reader_get_value_length    (hREADER  hreader
                                                   ,SIZE   *p_length);
/** returns the current symbol table the value the reader is currently
 *  positioned on.  This can be used to reset the readers symbol
 *  table is you wish to seek in a stream which contains multiple
 *  symbol tables.  This symbol table handle can be used to call
 *  ion_reader_set_symbol_table.
 */
ION_API_EXPORT iERR ion_reader_get_symbol_table    (hREADER   hreader
                                                   ,hSYMTAB  *p_hsymtab);
/** Returns the next ION_TYPE in the stream. In case of EOF, IERR_OK will be returned. p_value_type = tid_EOF.
 * @param   hreader
 * @param   p_value_type    ION_TYPE (tid_EOF, tid_BOOL, etc, defined in ion_const.h). tid_EOF if EOF.
 */
ION_API_EXPORT iERR ion_reader_next                (hREADER hreader, ION_TYPE *p_value_type);
ION_API_EXPORT iERR ion_reader_step_in             (hREADER hreader);
ION_API_EXPORT iERR ion_reader_step_out            (hREADER hreader);
ION_API_EXPORT iERR ion_reader_get_depth           (hREADER hreader, SIZE *p_depth);

/**
 * Returns the type of the current value, or tid_none if no value has been assigned. (before next() is called)
 */
ION_API_EXPORT iERR ion_reader_get_type            (hREADER hreader, ION_TYPE *p_value_type);
ION_API_EXPORT iERR ion_reader_has_any_annotations (hREADER hreader, BOOL *p_has_any_annotations);
ION_API_EXPORT iERR ion_reader_has_annotation      (hREADER hreader, iSTRING annotation, BOOL *p_annotation_found);
ION_API_EXPORT iERR ion_reader_is_null             (hREADER hreader, BOOL *p_is_null);
ION_API_EXPORT iERR ion_reader_is_in_struct        (hREADER preader, BOOL *p_is_in_struct);
ION_API_EXPORT iERR ion_reader_get_field_name      (hREADER hreader, iSTRING p_str);
ION_API_EXPORT iERR ion_reader_get_field_name_symbol(hREADER hreader, ION_SYMBOL **p_psymbol);
ION_API_EXPORT iERR ion_reader_get_annotations     (hREADER hreader, iSTRING p_strs, SIZE max_count, SIZE *p_count);
ION_API_EXPORT iERR ion_reader_get_annotation_symbols(hREADER hreader, ION_SYMBOL *p_symbols, SIZE max_count, SIZE *p_count);
ION_API_EXPORT iERR ion_reader_get_annotation_count(hREADER hreader, SIZE *p_count);
ION_API_EXPORT iERR ion_reader_get_an_annotation   (hREADER hreader, int idx, iSTRING p_strs);
ION_API_EXPORT iERR ion_reader_get_an_annotation_symbol(hREADER hreader, int idx, ION_SYMBOL *p_symbol);
ION_API_EXPORT iERR ion_reader_read_null           (hREADER hreader, ION_TYPE *p_value);
ION_API_EXPORT iERR ion_reader_read_bool           (hREADER hreader, BOOL *p_value);

/** Read integer value from Ion stream.
 * The size of the integer is sizeof(int)
 * If the value in the Ion stream does not fit into the variable, it will return with IERR_NUMERIC_OVERFLOW.
 */
ION_API_EXPORT iERR ion_reader_read_int            (hREADER hreader, int *p_value);

/** Read integer value from Ion stream.
 * The size of the integer is sizeof(int32_t)
 * If the value in the Ion stream does not fit into the variable, it will return with IERR_NUMERIC_OVERFLOW.
 */
ION_API_EXPORT iERR ion_reader_read_int32            (hREADER hreader, int32_t *p_value);

/** Read integer value from Ion stream.
 * The size of the integer is sizeof(int64_t)
 * If the value in the Ion stream does not fit into the variable, it will return with IERR_NUMERIC_OVERFLOW.
 */
ION_API_EXPORT iERR ion_reader_read_int64          (hREADER hreader, int64_t *p_value);

/** Read integer value from Ion stream.
 * This supports arbitary length integers defined by ion_int. 
 */
ION_API_EXPORT iERR ion_reader_read_ion_int        (hREADER hreader, ION_INT *p_value);

/** Read integer value from Ion stream.
 * The size of the integer is sizeof(long), which is 4 or 8 bytes depends on the OS
 * If the value in the Ion stream does not fit into the variable, it will return with IERR_NUMERIC_OVERFLOW.
 */
ION_API_EXPORT iERR ion_reader_read_long           (hREADER hreader, long *p_value);
ION_API_EXPORT iERR ion_reader_read_double         (hREADER hreader, double *p_value);

/**
 * @deprecated use of decQuads directly is deprecated. ION_DECIMAL should be used. See `ion_reader_read_ion_decimal`.
 */
ION_API_EXPORT iERR ion_reader_read_decimal        (hREADER hreader, decQuad *p_value);
ION_API_EXPORT iERR ion_reader_read_ion_decimal    (hREADER hreader, ION_DECIMAL *p_value);

/**
 * @return IERR_NULL_VALUE if the current value is null.timestamp.
 */
ION_API_EXPORT iERR ion_reader_read_timestamp      (hREADER hreader, iTIMESTAMP p_value);

/** Read the current symbol value as an ION_SYMBOL.
 */
ION_API_EXPORT iERR ion_reader_read_ion_symbol(hREADER hreader, ION_SYMBOL *p_symbol);

/**
 * Determines the content of the current text value, which must be an
 * Ion string or symbol.  The reader retains ownership of the returned byte
 * array, and the caller must copy the data out (if necessary) before moving
 * the cursor.
 *
 * @param hreader must be a valid handle.
 *
 * @param p_value receives the string information.
 */
ION_API_EXPORT iERR ion_reader_get_string_length   (hREADER hreader, SIZE *p_length); // note this may require the string value to be loaded in memory
ION_API_EXPORT iERR ion_reader_read_string         (hREADER hreader, iSTRING p_value);
ION_API_EXPORT iERR ion_reader_read_partial_string (hREADER hreader, BYTE *p_buf, SIZE buf_max, SIZE *p_length);

// TODO: move this to get_value_size, get_value_bytes, get_value_chuck that can read
//       string, long int, decimal and timestamp values in addition to blob and clob
ION_API_EXPORT iERR ion_reader_get_lob_size          (hREADER hreader, SIZE *p_length); // this may require the lob value to be loaded in memroy
ION_API_EXPORT iERR ion_reader_read_lob_bytes        (hREADER hreader, BYTE *p_buf, SIZE buf_max, SIZE *p_length);
ION_API_EXPORT iERR ion_reader_read_lob_partial_bytes(hREADER hreader, BYTE *p_buf, SIZE buf_max, SIZE *p_length);

/**
 * Gets the current position and if hreader is a text reader, also gets
 * the line and column numbers.
 *
 * Note that this is only useful for error reporting or debugging purposes
 * about malformed Ion data, since the position reported by this function
 * is unlikely to be pointed at the start of a value.  To obtain the
 * position of the current value from the reader to report the origin
 * of semantic errors within well formed Ion data, see
 * ion_reader_get_value_position.
 */
ION_API_EXPORT iERR ion_reader_get_position          (hREADER hreader, int64_t *p_bytes, int32_t *p_line, int32_t *p_offset);

/**
 * Closes a reader and releases associated memory.  The caller is responsible
 * for releasing the underlying buffer (if any).  After calling this method
 * the given handle will no longer be value.
 *
 * @param hreader must be a valid handle.
 */
ION_API_EXPORT iERR ion_reader_close(hREADER hreader);
#ifdef __cplusplus
}
#endif

#endif
