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

#ifndef ION_WRITER_IMPL_H_
#define ION_WRITER_IMPL_H_

//
// ion_writer.h
//
// internal declarations for the text and binary writers
//
// common ion writer properties
//
// all ion writer are primary resources
//
// TODO: revisit the logic behind this buffer
//       should we just be using the normal "owner
//       allocation" system.  Or do we want this
//       (or a near relative) for stuff that exists
//       at the datagram level (like local symbol
//       tables and their symbols) which we can
//       throw away between top level values?
//
//       it seems like the alternative would be a better choice
//

#include <ionc/ion_collection.h>
#include "ion_internal.h"
#include <ionc/ion_stream.h>
#include <ionc/ion_types.h>

#ifdef __cplusplus
extern "C" {
#endif


#define ION_WRITER_TEMP_BUFFER_DEFAULT 1024
typedef struct _ion_temp_buffer {
    BYTE *base;
    BYTE *position;
    BYTE *limit;
} ION_TEMP_BUFFER;

iERR ion_temp_buffer_init(hOWNER owner, ION_TEMP_BUFFER *temp_buffer, SIZE buf_size);
iERR ion_temp_buffer_alloc(ION_TEMP_BUFFER *temp_buffer, SIZE needed, void **p_ptr);
iERR ion_temp_buffer_make_utf8_string(ION_TEMP_BUFFER *temp_buffer, char *cstr, SIZE length, void **p_ptr, SIZE *p_utf8_length);
iERR ion_temp_buffer_reset(ION_TEMP_BUFFER *temp_buffer);

typedef enum _ION_WRITER_SYMTAB_INTERCEPT_STATE {
    iWSIS_NONE               = 0x000,
    iWSIS_IN_LST_STRUCT      = 0x001,
    iWSIS_SYMBOLS            = 0x002,
    iWSIS_IN_SYMBOLS_LIST    = 0x004,
    iWSIS_IMPORTS            = 0x008,
    iWSIS_IN_IMPORTS_LIST    = 0x010,
    iWSIS_IN_IMPORTS_STRUCT  = 0x020,
    iWSIS_IMPORT_VERSION     = 0x040,
    iWSIS_IMPORT_MAX_ID      = 0x080,
    iWSIS_IMPORT_NAME        = 0x100,
} ION_WRITER_SYMTAB_INTERCEPT_STATE;

#define ION_WRITER_SI_LST_APPEND (uint16_t)0x8000

#define ION_WRITER_SI_HAS_SYMBOLS(writer) (writer->_completed_symtab_intercept_states & iWSIS_SYMBOLS)
#define ION_WRITER_SI_HAS_IMPORTS(writer) (writer->_completed_symtab_intercept_states & iWSIS_IMPORTS)
#define ION_WRITER_SI_HAS_IMPORT_NAME(writer) (writer->_completed_symtab_intercept_states & iWSIS_IMPORT_NAME)
#define ION_WRITER_SI_HAS_IMPORT_VERSION(writer) (writer->_completed_symtab_intercept_states & iWSIS_IMPORT_VERSION)
#define ION_WRITER_SI_HAS_IMPORT_MAX_ID(writer) (writer->_completed_symtab_intercept_states & iWSIS_IMPORT_MAX_ID)
#define ION_WRITER_SI_IS_LST_APPEND(writer) (writer->_completed_symtab_intercept_states & ION_WRITER_SI_LST_APPEND)

#define _ION_WRITER_SI_COMPLETE(writer, completed, next) \
    writer->_completed_symtab_intercept_states |= (completed); \
    writer->_current_symtab_intercept_state = next;

#define ION_WRITER_SI_COMPLETE_SYMBOLS(writer) _ION_WRITER_SI_COMPLETE(writer, iWSIS_SYMBOLS, iWSIS_IN_LST_STRUCT);
#define ION_WRITER_SI_COMPLETE_IMPORTS(writer) _ION_WRITER_SI_COMPLETE(writer, iWSIS_IMPORTS, iWSIS_IN_LST_STRUCT);
#define ION_WRITER_SI_COMPLETE_IMPORT_NAME(writer) _ION_WRITER_SI_COMPLETE(writer, iWSIS_IMPORT_NAME, iWSIS_IN_IMPORTS_STRUCT);
#define ION_WRITER_SI_COMPLETE_IMPORT_VERSION(writer) _ION_WRITER_SI_COMPLETE(writer, iWSIS_IMPORT_VERSION, iWSIS_IN_IMPORTS_STRUCT);
#define ION_WRITER_SI_COMPLETE_IMPORT_MAX_ID(writer) _ION_WRITER_SI_COMPLETE(writer, iWSIS_IMPORT_MAX_ID, iWSIS_IN_IMPORTS_STRUCT);
#define ION_WRITER_SI_COMPLETE_IMPORT(writer) _ION_WRITER_SI_COMPLETE(writer, writer->_completed_symtab_intercept_states, iWSIS_IN_IMPORTS_LIST);
#define ION_WRITER_SI_MARK_LST_APPEND(writer) _ION_WRITER_SI_COMPLETE(writer, ION_WRITER_SI_LST_APPEND, iWSIS_IN_LST_STRUCT);

typedef struct _ion_text_writer
{
    BOOL       _no_output;           // is true until at least 1 char is written to the stream
    BOOL       _pending_separator;   // tells the writer that a value that will need a separator has been written but the separator has not
    int        _separator_character; // correct separator which depends on the container (' ' or ',')

    int        _pending_blob_bytes;
    int        _pending_triple;

    int        _top;
    int        _stack_size;
    ION_TYPE  *_stack_parent_type;
    BYTE      *_stack_flags; // _stack_in_struct _stack_pending_comma;

} ION_TEXT_WRITER;

typedef struct _ion_binary_patch {
    int     _offset;
    int     _type;
    int     _length;
    BOOL    _in_struct;
} ION_BINARY_PATCH;

typedef struct _ion_binary_writer
{
    ION_TYPE            _lob_in_progress;

    ION_COLLECTION      _patch_stack;  // stack of patch pointers
    ION_COLLECTION      _patch_list;   // list of patches
    ION_COLLECTION      _value_list;   // list of pointers to value buffers of some size (like 8k)

    ION_STREAM         *_value_stream; // temporary in memory buffer for holding values to merge with the patch list

} ION_BINARY_WRITER;

typedef struct _ion_writer
{
    ION_OBJ_TYPE       type;
    ION_WRITER_OPTIONS options;
    decContext         deccontext;                  // working context

    ION_CATALOG       *pcatalog;
    ION_COLLECTION     _imported_symbol_tables; // Collection of ION_SYMBOL_TABLE_IMPORT
    ION_SYMBOL_TABLE  *symbol_table;        // if there are local symbols defined this will be a seperately allocated table, and should be freed as we close the top level value
    ION_SYMBOL_TABLE  *_pending_symbol_table;// The in-progress manually-written LST, if applicable. Becomes `symbol_table` when the LST struct is finished.
    BOOL               _has_local_symbols;

    ION_WRITER_SYMTAB_INTERCEPT_STATE   _current_symtab_intercept_state;
    uint16_t                            _completed_symtab_intercept_states;

    ION_TEMP_BUFFER    temp_buffer;         // holds field names and annotations until the writer needs them
    void              *_temp_entity_pool;   // memory pool for top level objects that we'll throw away during flush
    void              *_pending_temp_entity_pool; // Owns the in-progress manually-written LST, if applicable. Becomes `_temp_entity_pool` on flush.

    BOOL               _in_struct;
    SIZE               depth;

    ION_SYMBOL         field_name;

    SIZE               annotation_count;
    SIZE               annotation_curr;
    ION_SYMBOL        *annotations;

    BOOL               writer_owns_stream;   // true when open writer created the stream object
    ION_STREAM        *output;

    BOOL                _needs_version_marker;

    union {
        struct _ion_text_writer   text;
        struct _ion_binary_writer binary;
    } _typed_writer;

} _ion_writer;

#define TEXTWRITER(x) (&((x)->_typed_writer.text))

#define ION_TEXT_WRITER_FLAG_IN_STRUCT      0x01
#define ION_TEXT_WRITER_FLAG_PENDING_COMMA  0x02

// these macros assume the convention that the current text writer
// is pointed to by pwriter
#define ION_TEXT_WRITER_SET_IN_STRUCT(v)     SET_FLAG(TEXTWRITER(pwriter)->_stack_flags[TEXTWRITER(pwriter)->_top], ION_TEXT_WRITER_FLAG_IN_STRUCT, v)
#define ION_TEXT_WRITER_SET_PENDING_COMMA(v) SET_FLAG(TEXTWRITER(pwriter)->_stack_flags[TEXTWRITER(pwriter)->_top], ION_TEXT_WRITER_FLAG_PENDING_COMMA, v)

#define ION_TEXT_WRITER_IN_STRUCT()            (TEXTWRITER(pwriter)->_stack_flags[TEXTWRITER(pwriter)->_top] & ION_TEXT_WRITER_FLAG_IN_STRUCT)
#define ION_TEXT_WRITER_PENDING_COMMA()        (TEXTWRITER(pwriter)->_stack_flags[TEXTWRITER(pwriter)->_top] & ION_TEXT_WRITER_FLAG_PENDING_COMMA)

#define ION_TEXT_WRITER_TOP_TYPE()             (TEXTWRITER(pwriter)->_stack_parent_type[TEXTWRITER(pwriter)->_top - 1])
#define ION_TEXT_WRITER_TOP_IN_STRUCT()        (TEXTWRITER(pwriter)->_stack_flags[TEXTWRITER(pwriter)->_top - 1] & ION_TEXT_WRITER_FLAG_IN_STRUCT)
#define ION_TEXT_WRITER_TOP_PENDING_COMMA()    (TEXTWRITER(pwriter)->_stack_flags[TEXTWRITER(pwriter)->_top - 1] & ION_TEXT_WRITER_FLAG_PENDING_COMMA)

#define ION_TEXT_WRITER_IS_PRETTY()            (pwriter->options.pretty_print)
#define ION_TEXT_WRITER_IS_JSON()              (pwriter->options.json_downconvert)

#define ION_TEXT_WRITER_APPEND_CHAR(c)         ION_PUT((pwriter->output), c)
#define ION_TEXT_WRITER_APPEND_EOL()           ION_TEXT_WRITER_APPEND_CHAR('\n')

typedef struct _ion_obj_writer {
    ION_HEADER header;
    ION_WRITER writer;
} ION_OBJ_WRITER;

// #define WRITER_SIZE (sizeof(ION_WRITER) > sizeof(ION_BINARY_WRITER) ? sizeof(ION_TEXT_WRITER) : sizeof(ION_BINARY_WRITER))
/* if high bit it's unicode, if it's only low 5 bits its a control char */
#define ION_WRITER_NEEDS_ESCAPE_ASCII(c) ((c >= 127) || (c < 32) || (c == '\\'))
#define ION_WRITER_NEEDS_ESCAPE_UTF8(c) ((c < 32) || (c == '\\'))

//
// internal common api's - operates over the base (shared) write
// writer definition these are in ion_writer.c
//
iERR _ion_writer_open_buffer_helper(ION_WRITER **p_pwriter, BYTE *buffer, SIZE buf_length, ION_WRITER_OPTIONS *p_options);
iERR _ion_writer_open_stream_helper(ION_WRITER **p_pwriter, ION_STREAM p_stream, void *handler_state, ION_WRITER_OPTIONS *p_options);
iERR _ion_writer_open_helper(ION_WRITER **p_pwriter, ION_STREAM *stream, ION_WRITER_OPTIONS *p_options);
void _ion_writer_initialize_option_defaults(ION_WRITER_OPTIONS *p_options);
iERR _ion_writer_initialize(ION_WRITER *pwriter, ION_OBJ_TYPE writer_type);

iERR _ion_writer_get_depth_helper(ION_WRITER *pwriter, SIZE *p_depth);
iERR _ion_writer_set_temp_size_helper(ION_WRITER *pwriter, SIZE size_of_temp_space);
iERR _ion_writer_set_max_annotation_count_helper(ION_WRITER *pwriter, SIZE annotation_limit);
iERR _ion_writer_set_catalog_helper(ION_WRITER *pwriter, ION_CATALOG *pcatalog);
iERR _ion_writer_get_catalog_helper(ION_WRITER *pwriter, ION_CATALOG **p_pcatalog);
iERR _ion_writer_set_symbol_table_helper(ION_WRITER *pwriter, ION_SYMBOL_TABLE *psymtab);
iERR _ion_writer_get_symbol_table_helper(ION_WRITER *pwriter, ION_SYMBOL_TABLE **p_psymtab);
iERR _ion_writer_add_imported_table_helper(ION_WRITER *pwriter, ION_SYMBOL_TABLE_IMPORT *import, BOOL *finish_on_unique);
iERR _ion_writer_add_imported_tables_helper(ION_WRITER *pwriter, ION_COLLECTION *imports);
iERR _ion_writer_write_field_name_helper(ION_WRITER *pwriter, ION_STRING *name);
iERR _ion_writer_write_field_sid_helper(ION_WRITER *pwriter, SID sid);
iERR _ion_writer_write_field_name_symbol_helper(ION_WRITER *pwriter, ION_SYMBOL *field_name);
iERR _ion_writer_clear_field_name_helper(ION_WRITER *pwriter);
iERR _ion_writer_add_annotation_helper(ION_WRITER *pwriter, ION_STRING *annotation);
iERR _ion_writer_add_annotation_sid_helper(ION_WRITER *pwriter, SID sid);
iERR _ion_writer_add_annotation_symbol_helper(ION_WRITER *pwriter, ION_SYMBOL *annotation);
iERR _ion_writer_write_annotations_helper(ION_WRITER *pwriter, ION_STRING *p_annotations, int32_t count);
iERR _ion_writer_write_annotation_symbols_helper(ION_WRITER *pwriter, ION_SYMBOL *annotations, SIZE count);
iERR _ion_writer_clear_annotations_helper(ION_WRITER *pwriter);
iERR _ion_writer_write_typed_null_helper(ION_WRITER *pwriter, ION_TYPE type);
iERR _ion_writer_write_bool_helper(ION_WRITER *pwriter, BOOL value);
iERR _ion_writer_write_int64_helper(ION_WRITER *pwriter, int64_t value);
iERR _ion_writer_write_ion_int_helper(ION_WRITER *pwriter, ION_INT *value);
iERR _ion_writer_write_mixed_int_helper(ION_WRITER *pwriter, ION_READER *preader);
iERR _ion_writer_write_float_helper(ION_WRITER *pwriter, float value);
iERR _ion_writer_write_double_helper(ION_WRITER *pwriter, double value);
iERR _ion_writer_write_decimal_helper(ION_WRITER *pwriter, decQuad *value);
iERR _ion_writer_write_ion_decimal_helper(ION_WRITER *pwriter, ION_DECIMAL *value);
iERR _ion_writer_write_timestamp_helper(ION_WRITER *pwriter, ION_TIMESTAMP *value);
iERR _ion_writer_write_symbol_id_helper(ION_WRITER *pwriter, SID value);
iERR _ion_writer_validate_symbol_id(ION_WRITER *pwriter, SID sid);
iERR _ion_writer_write_symbol_helper(ION_WRITER *pwriter, ION_STRING *symbol);
iERR _ion_writer_write_ion_symbol_helper(ION_WRITER *pwriter, ION_SYMBOL *symbol);
iERR _ion_writer_write_string_helper(ION_WRITER *pwriter, ION_STRING *pstr);
iERR _ion_writer_write_clob_helper(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_write_blob_helper(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_start_lob_helper(ION_WRITER *pwriter, ION_TYPE lob_type);
iERR _ion_writer_append_lob_helper(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_finish_lob_helper(ION_WRITER *pwriter);
iERR _ion_writer_start_container_helper(ION_WRITER *pwriter, ION_TYPE container_type);
iERR _ion_writer_finish_container_helper(ION_WRITER *pwriter);
iERR _ion_writer_write_one_value_helper(ION_WRITER *pwriter, ION_READER *preader);
iERR _ion_writer_write_all_values_helper(ION_WRITER *pwriter, ION_READER *preader);
iERR _ion_writer_flush_helper(ION_WRITER *pwriter, SIZE *p_bytes_flushed);
iERR _ion_writer_close_helper(ION_WRITER *pwriter);
iERR _ion_writer_free_local_symbol_table( ION_WRITER *pwriter );
iERR _ion_writer_make_symbol_helper(ION_WRITER *pwriter, ION_STRING *pstr, SID *p_sid);
iERR _ion_writer_clear_field_name_helper(ION_WRITER *pwriter);
iERR _ion_writer_get_field_name_as_string_helper(ION_WRITER *pwriter, ION_STRING *p_str, BOOL *p_is_symbol_identifier);
iERR _ion_writer_get_field_name_as_sid_helper(ION_WRITER *pwriter, SID *p_sid);
iERR _ion_writer_clear_annotations_helper(ION_WRITER *pwriter);
iERR _ion_writer_get_annotation_count_helper(ION_WRITER *pwriter, int32_t *p_count);
iERR _ion_writer_get_annotation_as_string_helper(ION_WRITER *pwriter, int32_t idx, ION_STRING *p_str, BOOL *p_is_symbol_identifier);
iERR _ion_writer_get_annotation_as_sid_helper(ION_WRITER *pwriter, int32_t idx, SID *p_sid);

/**
 * Returns TRUE only if the writer has a local symbol table that must be serialized. The conditions under which this
 * is TRUE are different for text and binary writers.
 */
BOOL _ion_writer_has_symbol_table(ION_WRITER *pwriter);

iERR ion_temp_buffer_init(hOWNER owner, ION_TEMP_BUFFER *temp_buffer, SIZE size_of_temp_space);
iERR ion_temp_buffer_alloc(ION_TEMP_BUFFER *temp_buffer, SIZE needed, void **p_ptr);
iERR ion_temp_buffer_make_utf8_string(ION_TEMP_BUFFER *temp_buffer, char *cstr, SIZE length, void **p_ptr, SIZE *p_utf8_length);
iERR ion_temp_buffer_make_string_copy(ION_TEMP_BUFFER *temp_buffer, ION_STRING *pdst, ION_STRING *psrc);

iERR ion_temp_buffer_reset(ION_TEMP_BUFFER *temp_buffer);

iERR _ion_writer_allocate_temp_pool( ION_WRITER *pwriter );
iERR _ion_writer_reset_temp_pool( ION_WRITER *pwriter );
iERR _ion_writer_free_temp_pool( ION_WRITER *pwriter );
iERR _ion_writer_free_pending_pool( ION_WRITER *pwriter );



//
// text writer interfaces in ion_writer_text.c
//
iERR _ion_writer_text_append_symbol_string(ION_WRITER *pwriter, ION_STRING *p_str, BOOL system_identifiers_need_quotes);
iERR _ion_writer_text_append_ascii_cstr(ION_STREAM *poutput, char *cp);
iERR _ion_writer_text_append_escape_sequence_string(ION_STREAM *poutput, BOOL down_convert, BYTE *cp, BYTE *limit, BYTE **p_next);
iERR _ion_writer_text_append_escape_sequence_cstr_limit(ION_STREAM *poutput, char *cp, char *limit, char **p_next);
iERR _ion_writer_text_append_escape_sequence_cstr(ION_STREAM *poutput, char *cp, char **p_next);
iERR _ion_writer_text_append_escaped_string (ION_STREAM *poutput, ION_STRING *p_str, char quote_char, BOOL down_convert);
iERR _ion_writer_text_append_escaped_string_utf8(ION_STREAM *poutput, ION_STRING *p_str, char quote_char);
iERR _ion_writer_text_append_unicode_scalar(ION_STREAM *poutput, int unicode_scalar, BOOL down_convert);
iERR _ion_writer_text_read_unicode_scalar(char *cp, int *p_chars_read, int *p_unicode_scalar);

iERR _ion_writer_text_initialize(ION_WRITER *pwriter);
iERR _ion_writer_text_initialize_stack(ION_WRITER *pwriter);
iERR _ion_writer_text_write_typed_null(ION_WRITER *pwriter, ION_TYPE type);
iERR _ion_writer_text_write_bool(ION_WRITER *pwriter, BOOL value);
iERR _ion_writer_text_write_int64(ION_WRITER *pwriter, int64_t value);
iERR _ion_writer_text_write_ion_int(ION_WRITER *pwriter, ION_INT *iint);
iERR _ion_writer_text_write_double(ION_WRITER *pwriter, double value);
iERR _ion_writer_text_write_double_json(ION_WRITER *pwriter, double value);
iERR _ion_writer_text_write_decimal_quad(ION_WRITER *pwriter, decQuad *value);
iERR _ion_writer_text_write_decimal_number(ION_WRITER *pwriter, decNumber *value);
iERR _ion_writer_text_write_timestamp(ION_WRITER *pwriter, iTIMESTAMP value);
iERR _ion_writer_text_write_symbol_id(ION_WRITER *pwriter, SID value);
iERR _ion_writer_text_write_symbol(ION_WRITER *pwriter, iSTRING symbol);
iERR _ion_writer_text_write_string(ION_WRITER *pwriter, iSTRING str);

BOOL _ion_writer_text_has_symbol_table(ION_WRITER *pwriter);

iERR _ion_writer_text_write_clob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_text_append_clob_contents(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_text_write_blob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_text_append_blob_contents(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_text_close_blob_contents(ION_WRITER *pwriter);
iERR _ion_writer_text_start_lob(ION_WRITER *pwriter, ION_TYPE lob_type);
iERR _ion_writer_text_append_lob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_text_finish_lob(ION_WRITER *pwriter);
iERR _ion_writer_text_start_container(ION_WRITER *pwriter, ION_TYPE container_type);
iERR _ion_writer_text_finish_container(ION_WRITER *pwriter);
iERR _ion_writer_text_close(ION_WRITER *pwriter, BOOL flush);

//
// internal binary impl's of public api's
//
iERR _ion_writer_binary_initialize(ION_WRITER *pwriter);
iERR _ion_writer_binary_write_typed_null(ION_WRITER *pwriter, ION_TYPE type);
iERR _ion_writer_binary_write_bool(ION_WRITER *pwriter, BOOL value);
iERR _ion_writer_binary_write_int64(ION_WRITER *pwriter, int64_t value);
iERR _ion_writer_binary_write_ion_int(ION_WRITER *pwriter, ION_INT *iint);
iERR _ion_writer_binary_write_float(ION_WRITER *pwriter, float value);
iERR _ion_writer_binary_write_double(ION_WRITER *pwriter, double value);
iERR _ion_writer_binary_write_decimal_quad(ION_WRITER *pwriter, decQuad *value);
iERR _ion_writer_binary_write_decimal_number(ION_WRITER *pwriter, decNumber *value);
iERR _ion_writer_binary_write_timestamp(ION_WRITER *pwriter, iTIMESTAMP value);
iERR _ion_writer_binary_write_symbol_id(ION_WRITER *pwriter, SID value);
iERR _ion_writer_binary_write_symbol(ION_WRITER *pwriter, iSTRING symbol);
iERR _ion_writer_binary_write_string(ION_WRITER *pwriter, iSTRING str);
iERR _ion_writer_binary_write_clob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_binary_write_blob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_binary_start_lob(ION_WRITER *pwriter, ION_TYPE lob_type);
iERR _ion_writer_binary_append_lob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length);
iERR _ion_writer_binary_finish_lob(ION_WRITER *pwriter);
iERR _ion_writer_binary_start_container(ION_WRITER *pwriter, ION_TYPE container_type);
iERR _ion_writer_binary_finish_container(ION_WRITER *pwriter);
iERR _ion_writer_binary_close(ION_WRITER *pwriter, BOOL flush);

iERR _ion_writer_binary_output_stream_handler(ION_STREAM *pstream);
iERR _ion_writer_binary_input_stream_handler(ION_STREAM *pstream);
iERR _ion_writer_binary_start_value(ION_WRITER *pwriter, int value_length);
iERR _ion_writer_binary_close_value(ION_WRITER *writer);
iERR _ion_writer_binary_push_position(ION_WRITER *bwriter, int type_id);
iERR _ion_writer_binary_pop(ION_WRITER *bwriter);
iERR _ion_writer_binary_patch_lengths(ION_WRITER *bwriter, int added_length);
iERR _ion_writer_binary_top_length(ION_WRITER *bwriter, int *plength);
iERR _ion_writer_binary_top_position(ION_WRITER *bwriter, int *poffset);
iERR _ion_writer_binary_top_in_struct(ION_WRITER *bwriter, BOOL *p_is_in_struct);

iERR _ion_writer_binary_flush_to_output(ION_WRITER *pwriter);
iERR _ion_writer_binary_serialize_symbol_table(ION_SYMBOL_TABLE *psymtab, ION_STREAM *out, int *p_length);
int   ion_writer_binary_serialize_import_struct_length(ION_SYMBOL_TABLE_IMPORT_DESCRIPTOR *import);
int   ion_writer_binary_serialize_symbol_length(ION_SYMBOL *symbol);

#ifdef __cplusplus
}
#endif




#endif /* ION_WRITER_IMPL_H_ */
