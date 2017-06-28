/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#ifndef ION_READER_IMPL_H_
#define ION_READER_IMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

/** This is the struct that hold state for the ion text reader when parsing
 *  Ion from UTF8.
 *
 */

typedef struct _ion_reader_text
{
    /** tokenizer used to interprete the input stream for tokens. (TOKEN_INT, TOKEN_DOT, TOKEN_COMMA, etc)
     *  this supports recognize() or skip() or read()
     *      recognize() looks ahead and returns a beginning of a whole value token (null to struct)
     *      skip(token type) scans over a token of a known type ignoring the characters
     *      read(token type, destination) reads the value characters from the input stream and writing to the destination (converting as necessary)
     *  all of these track line count and offset for error reporting
     *  note that _scanner is in line (not allocated)
     */
    ION_SCANNER          _scanner;
    
    
    /** Current state of the parser. (datagram, plain_value, list_value, list_comma, etc)
     *
     */
    ION_PARSER_STATE     _state;

    /** value start and value end hold the byte offset in the input stream of the first character of the value and
     *  the first character after the value. These can be used to seek the stream to the beginning of the value
     *  or just past the value. These are set to -1 if the values are unknown. start is unknown until the beginning
     *  of the value is recognized and only while the value is not finished being read. start moves forward if
     *  the value is being read in pieces (string, clob, blob, and containers). end is not set unless the value
     *  as been scanned to get the length, it is also -1 if the reader is not "on" a value.
     *
     */
    POSITION              _value_start;
    POSITION              _value_end;
    POSITION              _annotation_start;

    /** space for the field name. The string value always points to the field name buffer and the length of the string is the
     *  number of bytes in the current field name. The actual characters are in the field name buffer and we limit field names
     *  to field name buffer length.
     *
     */
    ION_SYMBOL            _field_name;
    BYTE                 *_field_name_buffer;
    SIZE                  _field_name_buffer_length;
    
    /** This collection holds the buffered annotation symbols. It is a list of ION_STRING
     *  each string holding one annotation. It is preallocated for max_annotation_count
     *  string. For safety any annotation is null terminated. The null byte is not included
     *  in the length stored in the ion string. Space for the null's is added to the
     *  reader option requested annotation max buffered.
     *
     */
    SIZE                  _annotation_count;               // number of annotations on this value
    ION_SYMBOL           *_annotation_string_pool;         // preallocated set of ION_SYMBOL to hold annotations
    SIZE                  _annotation_string_pool_length;  // max number of annotations, size of string pool as count
    BYTE                 *_annotation_value_next;          // position in annotation value buffer for the next annotation value
    BYTE                 *_annotation_value_buffer;        // preallocate buffer to hold all annotation character for all annotations of the current value
    SIZE                  _annotation_value_buffer_length; // allocated length of the annotation value buffer


    /** ION_TYPE for the value at the current position. tid_INT, tid_FLOAT, tid_BLOB, etc)
     * The value is only valid if _value_ready is set.
     * tid_EOF is a valid value type, so the usual value check would be checking both value_ready
     * and value_type != EOF
     *
     */
    ION_TYPE              _value_type;

    /** this is the type of the value that has been recognized by the parser. It is much more detailed
     *  as it must define the value termination sequence and correct interpretation of the bytes.
     *
     */
    ION_SUB_TYPE          _value_sub_type;

    /** this is the current container. Initially this is DATAGRAM, it changes as the user
     *  step into and out of containers as they are encountered.
     *
     */
    ION_TYPE             _current_container;

    /**  local stack of parent container types for stepInto() and stepOut()
     *   this contains the stack of ION_TYPEs of the containers the caller
     *   has stepped into. These types are always a container type, never null.
     *   This stack is empty when we are at the top level (aka at the datagram
     *   level).
     *
     */
    ION_COLLECTION        _container_state_stack; 

} ION_TEXT_READER;





typedef enum _ion_reader_binary_state
{
    S_INVALID          =  0,
    S_BEFORE_TID       =  1,
    S_AFTER_TID        =  2,
    S_BEFORE_CONTENTS  =  3
} BINARY_STATE;

typedef struct _ion_reader_binary_parent_state
{
    int64_t _next_position;
    int     _tid;
    int64_t _local_end;
} BINARY_PARENT_STATE;

typedef struct _ion_reader_binary
{
    BINARY_STATE    _state; // 0=before tid, 1=after tid, 2=before contents

    BOOL            _in_struct;   // the binary reader can keep this correct, but the text parser can't (due to text having to parse struct's children)
    int             _parent_tid;  // using -1 for eof (or bof aka undefined) and 16 for datagram
    int64_t         _local_end;
    int64_t         _annotation_start;
    int64_t         _value_start;
    ION_TYPE        _value_type;
    SID             _value_field_id;
    SID             _value_symbol_id; // The SID of the pending symbol value.
    int             _value_tid;
    int32_t         _value_len;

    ION_COLLECTION _annotation_sids; // ~ 6 ints array of ION_STRING+

    // local stack for stepInto() and stepOut()
    ION_COLLECTION _parent_stack;

} ION_BINARY_READER;

#define BINARY(preader) (&((preader)->typed_reader.binary))

/** Read both text ion and binary ion data.
 *
 */
struct _ion_reader
{
    ION_OBJ_TYPE        type;
    ION_READER_OPTIONS  options;

    BOOL                has_static_buffer;

    ION_STREAM         *istream;
    BOOL                _reader_owns_stream;
    BOOL                _eof;
    int                 _depth;

    ION_CATALOG        *_catalog;
    decContext          _deccontext;                // ~ 10 ints working context
    SIZE                _expected_remaining_utf8_bytes; // used for reading and validating utf8 sequences a page at a time this is the number expected to finish a partially read character
    BOOL                _return_system_values;

    ION_SYMBOL_TABLE   *_current_symtab;
    ION_SYMBOL_TABLE   *_local_symtab_pool;         // memory pool for local symbol table we recycle
    ION_READER        **_temp_entity_pool;          // memory pool for top level objects that we'll throw away
    
    struct {
        BOOL            _is_ion_int;
        int64_t         _as_int64;
        ION_INT         _as_ion_int;
    } _int_helper;

    union {
        ION_TEXT_READER   text;
        ION_BINARY_READER binary;
    } typed_reader;
};

//
// shared internal reader routines
//

iERR _ion_reader_open_buffer_helper(ION_READER **p_preader, BYTE *buffer, SIZE buf_length, ION_READER_OPTIONS *p_options);
iERR _ion_reader_open_stream_helper(ION_READER **p_preader, ION_STREAM *p_stream, ION_READER_OPTIONS *p_options);
iERR _ion_reader_make_new_reader(ION_READER_OPTIONS *p_options, ION_READER **p_reader);
iERR _ion_reader_set_options(ION_READER *preader, ION_READER_OPTIONS* p_options);
void _ion_reader_initialize_option_defaults(ION_READER_OPTIONS *p_options);
iERR _ion_reader_validate_options(ION_READER_OPTIONS* p_options);
iERR _ion_reader_initialize(ION_READER *preader, BYTE *version_buffer, SIZE version_length);

iERR _ion_reader_get_catalog_helper(ION_READER *preader, ION_CATALOG **p_pcatalog);
iERR _ion_reader_get_symbol_table_helper(ION_READER *preader, ION_SYMBOL_TABLE **p_psymtab);
iERR _ion_reader_set_symbol_table_helper(ION_READER *preader, ION_SYMBOL_TABLE *symtab);
iERR _ion_reader_next_helper(ION_READER *preader, ION_TYPE *p_value_type);
iERR _ion_reader_step_in_helper(ION_READER *preader);
iERR _ion_reader_step_out_helper(ION_READER *preader);
iERR _ion_reader_get_depth_helper(ION_READER *preader, SIZE *p_depth);
iERR _ion_reader_get_type_helper(ION_READER *preader, ION_TYPE *p_value_type);
iERR _ion_reader_has_any_annotations_helper(ION_READER *preader, BOOL *p_has_annotations);
iERR _ion_reader_has_annotation_helper(ION_READER *preader, ION_STRING *annotation, BOOL *p_annotation_found);
iERR _ion_reader_get_annotation_count_helper(ION_READER *preader, int32_t *p_count);
iERR _ion_reader_get_an_annotation_helper(ION_READER *preader, int32_t idx, ION_STRING *p_str);
iERR _ion_reader_get_an_annotation_sid_helper(ION_READER *preader, int32_t idx, SID *p_sid);
iERR _ion_reader_is_null_helper(ION_READER *preader, BOOL *p_is_null);
iERR _ion_reader_get_field_name_helper(ION_READER *preader, ION_STRING **p_pstr);
iERR _ion_reader_get_field_sid_helper(ION_READER *preader, SID *p_sid);
iERR _ion_reader_get_annotations_helper(ION_READER *preader, ION_STRING *p_strs, SIZE max_count, SIZE *p_count);
iERR _ion_reader_read_null_helper(ION_READER *preader, ION_TYPE *p_value);
iERR _ion_reader_read_bool_helper(ION_READER *preader, BOOL *p_value);
iERR _ion_reader_read_int_helper(ION_READER *preader, int *p_value);
iERR _ion_reader_read_int32_helper(ION_READER *preader, int32_t *p_value);
iERR _ion_reader_read_int64_helper(ION_READER *preader, int64_t *p_value);
iERR _ion_reader_read_ion_int_helper(ION_READER *preader, ION_INT *p_value);
iERR _ion_reader_read_mixed_int_helper(ION_READER *preader);
iERR _ion_reader_read_double_helper(ION_READER *preader, double *p_value);
iERR _ion_reader_read_decimal_helper(ION_READER *preader, decQuad *p_value);
iERR _ion_reader_read_ion_decimal_helper(ION_READER *preader, ION_DECIMAL *p_value);
iERR _ion_reader_read_timestamp_helper(ION_READER *preader, ION_TIMESTAMP *p_value);
iERR _ion_reader_read_symbol_sid_helper(ION_READER *preader, SID *p_value);
iERR _ion_reader_read_symbol_helper(ION_READER *preader, ION_SYMBOL *p_symbol);

iERR _ion_reader_get_string_length_helper(ION_READER *preader, SIZE *p_length);
iERR _ion_reader_read_string_helper(ION_READER *preader, ION_STRING *p_value);
iERR _ion_reader_get_lob_size_helper(ION_READER *preader, SIZE *p_length);
iERR _ion_reader_get_string_size_helper(ION_READER *preader, SIZE *p_length);

iERR _ion_reader_read_lob_bytes_helper     (ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length);
iERR _ion_reader_read_partial_string_helper(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length);

iERR _ion_reader_close_helper(ION_READER *preader);

iERR _ion_reader_allocate_temp_pool                 ( ION_READER *preader );
iERR _ion_reader_reset_temp_pool                    ( ION_READER *preader);
iERR _ion_reader_get_new_local_symbol_table_owner   (ION_READER *preader, void **p_owner);
iERR _ion_reader_free_local_symbol_table            (ION_READER *preader);
iERR _ion_reader_reset_local_symbol_table           (ION_READER *preader);

iERR _ion_reader_get_position_helper(ION_READER *preader, int64_t *p_bytes, int32_t *p_line, int32_t *p_offset);

//
// text reader routines
//

        iERR ion_reader_text_close(hREADER *p_hreader);
        iERR ion_reader_text_initialize(hREADER *p_hreader);





//
// binary reader routines
//
iERR _ion_reader_binary_open                (ION_READER *preader);
iERR _ion_reader_binary_reset               (ION_READER *preader, int parent_tid, POSITION local_end);
iERR _ion_reader_binary_close               (ION_READER *preader);

iERR _ion_reader_binary_next                (ION_READER *preader, ION_TYPE *p_value_type);
iERR _ion_reader_binary_get_local_symbol_table_helper(ION_READER *preader, ION_SYMBOL_TABLE **pplocal );
iERR _ion_reader_binary_step_in             (ION_READER *preader);
iERR _ion_reader_binary_step_out            (ION_READER *preader);
iERR _ion_reader_binary_get_depth           (ION_READER *preader, SIZE *p_depth);
iERR _ion_reader_binary_get_value_length    (ION_READER *preader, SIZE *p_length);
iERR _ion_reader_binary_get_value_offset    (ION_READER *preader, POSITION *p_offset);

iERR _ion_reader_binary_get_type            (ION_READER *preader, ION_TYPE *p_value_type);
iERR _ion_reader_binary_has_any_annotations (ION_READER *preader, BOOL *p_has_any_annotations);
iERR _ion_reader_binary_has_annotation      (ION_READER *preader, iSTRING annotation, BOOL *p_annotation_found);
iERR _ion_reader_binary_get_annotation_count(ION_READER *preader, int32_t *p_count);
iERR _ion_reader_binary_get_an_annotation   (ION_READER *preader, int32_t idx, ION_STRING *p_str);
iERR _ion_reader_binary_get_an_annotation_sid(ION_READER *preader, int32_t idx, SID *p_sid);

iERR _ion_reader_binary_get_field_name     (ION_READER *preader, ION_STRING **pstr);
iERR _ion_reader_binary_get_field_sid      (ION_READER *preader, SID *p_sid);
iERR _ion_reader_binary_get_annotations    (ION_READER *preader, iSTRING p_strs, SIZE max_count, SIZE *p_count);
iERR _ion_reader_binary_get_annotation_sids(ION_READER *preader, SID *p_sids, SIZE max_count, SIZE *p_count);

iERR _ion_reader_binary_is_null             (ION_READER *preader, BOOL *p_is_null);
iERR _ion_reader_binary_read_null           (ION_READER *preader, ION_TYPE *p_value);
iERR _ion_reader_binary_read_bool           (ION_READER *preader, BOOL *p_value);
iERR _ion_reader_binary_read_int32          (ION_READER *preader, int32_t *p_value);
iERR _ion_reader_binary_read_int64          (ION_READER *preader, int64_t *p_value);
iERR _ion_reader_binary_read_ion_int        (ION_READER *preader, ION_INT *p_value);
iERR _ion_reader_binary_read_double         (ION_READER *preader, double *p_value);
iERR _ion_reader_binary_read_decimal        (ION_READER *preader, decQuad *p_value, decNumber **p_num);
iERR _ion_reader_binary_read_timestamp      (ION_READER *preader, iTIMESTAMP p_value);
iERR _ion_reader_binary_read_symbol_sid     (ION_READER *preader, SID *p_value);
iERR _ion_reader_binary_read_symbol_sid_helper(ION_READER *preader, ION_BINARY_READER *binary, SID *p_value);
iERR _ion_reader_binary_read_symbol         (ION_READER *preader, ION_SYMBOL *p_symbol);

iERR _ion_reader_binary_get_string_length   (ION_READER *preader, SIZE *p_length);
iERR _ion_reader_binary_read_string_bytes   (ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length);
iERR _ion_reader_binary_read_string         (ION_READER *preader, ION_STRING *pstr);

iERR _ion_reader_binary_get_lob_size        (ION_READER *preader, SIZE *p_length);
iERR _ion_reader_binary_read_lob_bytes      (ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length);

iERR _ion_reader_binary_validate_utf8       (BYTE *buf, SIZE len, SIZE expected_remaining, SIZE *p_expected_remaining);

/** Cast uint64_t and the sign bit (isNegative) to int64_t value.
 * NUMERIC_OVERFLOW if value unsigned value doesn't fit.
 * NULL_VALUE if pointer for return value (int64Value) is NULL.
 *
 */
iERR _ion_cast_to_int64(uint64_t unsignedInt64Value, BOOL isNegative, int64_t* int64Ptr);

// these could be macros ... hmmm
// BOOL ion_reader_binary_expect_symbol_table  (ION_READER *preader);
// BOOL ion_reader_binary_is_in_struct         (ION_READER *preader);

// these are short versions of the public interfaces that
// do less checking and are used by the write_all_values
// routines internally.  Generally all the public functions
// should have a pairing like this.
iERR _ion_reader_is_null_helper(ION_READER *preader, BOOL *p_is_null);
iERR _ion_reader_get_type_helper(ION_READER *preader, ION_TYPE *p_value_type);

iERR _ion_reader_get_field_name_helper(ION_READER *preader, ION_STRING **pstr);
iERR _ion_reader_has_any_annotations_helper(ION_READER *preader, BOOL *p_has_annotations);

iERR _ion_reader_read_bool_helper(ION_READER *preader, BOOL *p_bool_value);
iERR _ion_reader_read_int64_helper(ION_READER *preader, int64_t *p_int64_value);
iERR _ion_reader_read_double_helper(ION_READER *preader, double *p_double_value);
iERR _ion_reader_read_decimal_helper(ION_READER *preader, decQuad *p_decimal_value);
iERR _ion_reader_read_timestamp_helper(ION_READER *preader, ION_TIMESTAMP *p_timestamp_value);
iERR _ion_reader_read_string_helper(ION_READER *preader, ION_STRING *p_string_value);
iERR _ion_reader_read_symbol_as_string_helper(ION_READER *preader, ION_STRING *p_string_value);
iERR _ion_reader_read_symbol_as_sid_helper(ION_READER *preader, SID *p_sid);
iERR _ion_reader_get_lob_size_helper(ION_READER *preader, int32_t *p_len);
iERR _ion_reader_step_in_helper(ION_READER *preader);
iERR _ion_reader_step_out_helper(ION_READER *preader);

#ifdef __cplusplus
}
#endif

#endif /* ION_READER_IMPL_H_ */
