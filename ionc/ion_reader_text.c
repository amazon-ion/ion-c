/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/**
 * Code for the Ion text parser.  This is the 2nd version of the C parser.
 */
#include "ion.h"
#include "ion_internal.h"
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#define ION_FLOAT64_POS_INF (_ION_FLOAT64_POS_INF())
double _ION_FLOAT64_POS_INF() { 
  static const uint64_t value = 0x7FF0000000000000;
  return *(double*)&value; 
}

#define ION_FLOAT64_NEG_INF (_ION_FLOAT64_NEG_INF())
double _ION_FLOAT64_NEG_INF() { 
  static const uint64_t value = 0xFFF0000000000000;
  return *(double*)&value; 
}

#define ION_FLOAT64_NAN (_ION_FLOAT64_NAN())
double _ION_FLOAT64_NAN() { 
  static const uint64_t value = 0x7FFEFEFEFEFEFEFE;
  return *(double*)&value; 
}

#define STR_TO_UINT64 strtoull

// provide declarations for the ion sub type "enums"
#define IST_RECORD( NAME, TTT, STATE, FLAGS ) ION_SUB_TYPE_STRUCT g_##NAME = { #NAME, TTT, STATE, FLAGS  };
#include "ion_sub_type_records.h"

#define IST_RECORD( NAME, TTT, STATE, FLAGS ) ION_SUB_TYPE NAME = & g_##NAME ;
#include "ion_sub_type_records.h"
#include "ion_decimal_impl.h"

/**
 *        open the reader, allocate supporting buffers for fieldname, annotations,
 *        and the parent container stack.
 *
 */

iERR _ion_reader_text_open(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;

    ASSERT(preader);
    
    text->_state             = IPS_BEFORE_UTA;
    text->_current_container = tid_DATAGRAM;
   
    text->_value_start = -1;
    text->_value_end   = -1;

    IONCHECK(_ion_reader_text_open_alloc_buffered_string(preader
        , preader->options.symbol_threshold
        , &(text->_field_name.value)
        , &(text->_field_name_buffer)
        , &(text->_field_name_buffer_length)
    ));
    text->_field_name.sid = UNKNOWN_SID;

    text->_annotation_string_pool_length = preader->options.max_annotation_count;  // max number of annotations, size of string pool as count
    text->_annotation_value_buffer_length = preader->options.max_annotation_buffered + (preader->options.max_annotation_count * sizeof(BYTE));

    text->_annotation_string_pool = (ION_SYMBOL *)ion_alloc_with_owner(preader, text->_annotation_string_pool_length * sizeof(ION_SYMBOL));
    if (!text->_annotation_string_pool) {
        FAILWITH(IERR_NO_MEMORY);
    }

    text->_annotation_value_buffer = (BYTE *)ion_alloc_with_owner(preader, text->_annotation_value_buffer_length);
    if (!text->_annotation_value_buffer) {
        FAILWITH(IERR_NO_MEMORY);
    }

    text->_annotation_count = 0;
    text->_annotation_value_next = text->_annotation_value_buffer;

    text->_value_type         = tid_none;
    text->_value_sub_type     = IST_NONE;

    _ion_collection_initialize(preader, &(text->_container_state_stack), sizeof(ION_TYPE));
    
    IONCHECK(_ion_scanner_initialize(&(text->_scanner), preader));

    iRETURN;
}

iERR _ion_reader_text_open_alloc_buffered_string(ION_READER *preader, SIZE len, ION_STRING *p_string, BYTE **p_buf, SIZE *p_buf_len)
{
    iENTER;
    BYTE *ptr;

    ptr = (BYTE *)ion_alloc_with_owner(preader, len);
    if (!ptr) {
        FAILWITH(IERR_NO_MEMORY);
    }

    *p_buf            = ptr;
    *p_buf_len        = len;
     p_string->value  = ptr;
     p_string->length = 0;
    
    iRETURN;
}

iERR _ion_reader_text_reset(ION_READER *preader, ION_TYPE parent_tid, POSITION local_end)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;

    ASSERT(preader);
    ASSERT(parent_tid == tid_DATAGRAM); // TODO: support values other than DATAGRAM

    IONCHECK(_ion_reader_text_reset_value(preader));

    if (parent_tid == tid_DATAGRAM || parent_tid == tid_SEXP || parent_tid == tid_LIST) {
        text->_state = IPS_BEFORE_UTA;
    }
    else if (parent_tid == tid_STRUCT) {
        text->_state = IPS_BEFORE_FIELDNAME;
    }
    else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }
    text->_current_container = parent_tid;

    _ion_collection_reset(&(text->_container_state_stack));

    IONCHECK(_ion_scanner_reset(&(text->_scanner)));

    SUCCEED();

    iRETURN;
}

iERR _ion_reader_text_reset_value(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;

    ASSERT(preader);

    text->_value_start              = -1;
    text->_value_end                = -1;
    text->_annotation_start         = -1;
    text->_annotation_count         =  0;
    text->_annotation_value_next    =  text->_annotation_value_buffer;

    ION_STRING_INIT(&text->_field_name.value);
    text->_field_name.add_count = 0;
    text->_field_name.sid = UNKNOWN_SID;
    text->_field_name.psymtab = NULL;

    text->_value_type               =  tid_none;
    text->_value_sub_type           =  IST_NONE;

    IONCHECK(_ion_scanner_reset_value(&text->_scanner));

    SUCCEED();

    iRETURN;
}

iERR _ion_reader_text_close(ION_READER *preader)
{
    iENTER;

    // we need to free the local symbol table, if we allocated one
    if (preader->_local_symtab_pool != NULL) {
        ion_free_owner( preader->_local_symtab_pool );
        preader->_local_symtab_pool = NULL;
    }

    SUCCEED();

    iRETURN;
}

/**
 *   public APIs for actual operations - starting with NEXT()
 *
 */

iERR _ion_reader_text_next(ION_READER *preader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    ION_TYPE         t   = tid_none;
    ION_SUB_TYPE     ist = IST_NONE;

#ifdef DEBUG
    static long _value_counter = 0;

    _value_counter++;
    if (_value_counter == 99999912) { // 7238 || scanner->_has_marked_value) {
        ion_helper_breakpoint();
        _value_counter = _value_counter + 0;;
    }
#endif

    ASSERT(preader);

    // first we check to see if we have some unfished business with
    // the previously recgonized value (we may not)
    switch (text->_state) {
    case IPS_BEFORE_SCALAR:
    case IPS_BEFORE_CONTAINER:
    case IPS_IN_VALUE:
        /* we have work to finish */
        IONCHECK(_ion_scanner_skip_value_contents(&text->_scanner, text->_value_sub_type));
        text->_state = IPS_AFTER_VALUE;
        break;
    default:
        /* nothing to do */
        break;
    }

    
    // next we close out the parsing of any previous value
    // we check for a valid follow token based on the container type
    // this will be a punctuation token.
    // for 
    //      datagram: a value or a uta or eof
    //      sexp:     a value or a uta or a ')'
    //      list:     a ',' or a ']'
    //      struct:   a "," or a ']'
    if (text->_state == IPS_AFTER_VALUE) {
        IONCHECK(_ion_reader_text_check_follow_token(preader));
    }

    // this reset the various value states, like the annotation list
    // it will also skip over the "rest" of the value if the caller
    // did not read the contents of the previously read value

    IONCHECK(_ion_reader_text_reset_value(preader));

    switch(text->_state)
    {
    case IPS_EOF:
        ist = IST_EOF;
        break;

    case IPS_BEFORE_FIELDNAME:
        ASSERT(text->_current_container == tid_STRUCT);
        IONCHECK(_ion_reader_text_load_fieldname(preader, &ist));
        if (ist == IST_CLOSE_SINGLE_BRACE) {
            break;
        }
        // fall through to UTA
    case IPS_BEFORE_UTA:
        // we check for system value in this routine since we'll be at the same state
        // after the system value in the event it is a system value
        IONCHECK(_ion_reader_text_load_utas(preader, &ist));
        break;

    case IPS_ERROR:
    case IPS_NONE:
    case IPS_BEFORE_SCALAR:
    case IPS_BEFORE_CONTAINER:
    case IPS_IN_VALUE:
    case IPS_AFTER_VALUE:
    case IPS_AFTER_COMMA:
    default:
        text->_state = IPS_ERROR;
        FAILWITH(IERR_INVALID_STATE);
    }
    
    // we're after the uta's and have recognized the value
    // it's ready for the caller - save off the state we'll
    // need for later processing
    text->_value_start    = text->_scanner._value_start;
    text->_value_sub_type = ist;
    text->_value_type     = IST_BASE_TYPE( ist );
    text->_state          = IST_FOLLOW_STATE( ist );
    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_SYNTAX);
    }
    
    *p_value_type = text->_value_type;
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_text_intern_symbol(ION_READER *preader, ION_STRING *symbol_name, ION_SYMBOL **psym, BOOL parse_symbol_identifiers) {
    iENTER;
    ION_SYMBOL       *sym = NULL;
    ION_SYMBOL_TABLE *symbols;
    BOOL is_symbol_identifier;

    ASSERT(psym);

    if (parse_symbol_identifiers) {
        IONCHECK(_ion_reader_text_get_symbol_table(preader, &symbols));
        IONCHECK(_ion_symbol_table_parse_possible_symbol_identifier(symbols, symbol_name, NULL, &sym, &is_symbol_identifier));
        ASSERT(!(is_symbol_identifier ^ (sym != NULL)));
    }

    *psym = sym;

    iRETURN;
}

iERR _ion_reader_text_load_fieldname(ION_READER *preader, ION_SUB_TYPE *p_ist)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    ION_SUB_TYPE     ist = IST_NONE;
    BOOL             eos_encountered = FALSE;
    ION_SYMBOL      *sym;

    ASSERT(preader);
    ASSERT(text);
    ASSERT(text->_current_container == tid_STRUCT);

    IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
    if (ist == IST_CLOSE_SINGLE_BRACE) {
        // TODO: (or "was") :   IONCHECK(_ion_scanner_un_next(&text->_scanner, ist));
    }
    else {
        // TODO: perhaps this test could be left to read as symbol, which ends up 
        //       checking the ist to determine the correct terminator to watch for
        if (ist != IST_SYMBOL_PLAIN 
         && ist != IST_SYMBOL_QUOTED 
         && ist != IST_STRING_PLAIN 
         && ist != IST_STRING_LONG
        ) {
            FAILWITH(IERR_INVALID_FIELDNAME);
        }
        IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                           , text->_field_name_buffer
                                           , text->_field_name_buffer_length
                                           , ist
                                           , &text->_field_name.value.length
                                           , &eos_encountered
        ));
        if (!eos_encountered) {
            FAILWITH(IERR_TOKEN_TOO_LONG);
        }

        text->_field_name.value.value = text->_field_name_buffer;
        IONCHECK(_ion_reader_text_intern_symbol(preader, &text->_field_name.value, &sym, ist != IST_SYMBOL_QUOTED));
        if (sym) {
            ION_STRING_ASSIGN(&text->_field_name.value, &sym->value);
            text->_field_name.sid = sym->sid;
            text->_field_name.psymtab = sym->psymtab;
            text->_field_name.add_count = sym->add_count;
        }

        IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
        if (ist != IST_SINGLE_COLON) {
            FAILWITH(IERR_INVALID_FIELDNAME);
        }
    }

    // we need this to see if we hit the end of the struct
    *p_ist = ist;
    
    iRETURN;
}

iERR _ion_reader_text_check_for_no_op_IVM(ION_READER *preader, ION_SUB_TYPE ist, BOOL *p_is_no_op_IVM)
{
    iENTER;
    BOOL is_no_op_IVM = FALSE;
    ION_SYMBOL *sym = NULL;
    ION_STRING str;
    ION_TEXT_READER *text = &preader->typed_reader.text;

    ASSERT(p_is_no_op_IVM);
    ASSERT(preader->_depth == 0 && text->_annotation_count == 0);

    str.value = text->_scanner._value_buffer;
    str.length = text->_scanner._value_image.length;
    if (ist != IST_SYMBOL_QUOTED) {
        IONCHECK(_ion_reader_text_intern_symbol(preader, &str, &sym, TRUE));
        if (sym) {
            if (ION_STRING_EQUALS(&ION_SYMBOL_VTM_STRING, &sym->value)) {
                // This is a symbol identifier, e.g. $2, pointing to the text $ion_1_0, which is a no-op
                // system symbol.
                is_no_op_IVM = TRUE;
            }
        }
    }
    else if (ION_STRING_EQUALS(&ION_SYMBOL_VTM_STRING, &str)) {
        // This is an unannotated quoted symbol value with the text $ion_1_0, which is a no-op system
        // symbol.
        is_no_op_IVM = TRUE;
    }
    *p_is_no_op_IVM = is_no_op_IVM;
    iRETURN;
}

iERR _ion_reader_text_load_utas(ION_READER *preader, ION_SUB_TYPE *p_ist)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    ION_SUB_TYPE     ist = IST_NONE;
    ION_SYMBOL      *str;
    ION_SYMBOL      *sym;
    SIZE             remaining;
    BYTE            *next_dst;
    BOOL             is_double_colon;
    BOOL             eos_encountered = FALSE, is_system_value;

    ASSERT(preader);
    ASSERT(text);
    ASSERT(text->_annotation_value_next == text->_annotation_value_buffer); // we should have an empty annotation list at this point
    ASSERT(text->_annotation_count == 0);

    remaining = text->_annotation_value_buffer_length;
    next_dst  = text->_annotation_value_next;
    for (;;) {
        for (;;) {
            IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
            // make sure we only let extended symbols in the context of sexp
            if (ist == IST_SYMBOL_EXTENDED && text->_current_container != tid_SEXP) {
                FAILWITH(IERR_INVALID_SYNTAX);
            }
            // if we read an explicit EOF, we do not want to propagate it if we're not top-level
            if (ist == IST_EOF && text->_current_container != tid_DATAGRAM) {
                FAILWITH(IERR_EOF);
            }
            // if we read one of the closing characters, we need to honor the FCF to avoid returning EOF if the wrong closing punctuation was used
            if (ist != IST_EOF && IST_BASE_TYPE(ist) == tid_EOF) {
                if (text->_current_container == tid_DATAGRAM) {
                    // any closing type in a datagram is wrong
                    FAILWITH(IERR_INVALID_TOKEN);
                }
                if (text->_current_container == tid_LIST && ist != IST_CLOSE_BRACKET) {
                    FAILWITH(IERR_INVALID_TOKEN);
                }
                if (text->_current_container == tid_SEXP && ist != IST_CLOSE_PAREN) {
                    FAILWITH(IERR_INVALID_TOKEN);
                }
                if (text->_current_container == tid_STRUCT && ist != IST_CLOSE_SINGLE_BRACE) {
                    // XXX struct has field name handling so technically should never get here
                    FAILWITH(IERR_INVALID_TOKEN);
                }
            }

            if (ist != IST_SYMBOL_PLAIN && ist != IST_SYMBOL_QUOTED) {
                break;
            }

            IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                                , text->_scanner._value_buffer
                                                , text->_scanner._value_buffer_length
                                                , ist
                                                , &text->_scanner._value_image.length
                                                , &eos_encountered
            ));
            if (!eos_encountered) {
                FAILWITH(IERR_TOKEN_TOO_LONG);
            }
            
            IONCHECK(_ion_scanner_peek_double_colon(&text->_scanner, &is_double_colon));
            if (is_double_colon == FALSE) {
                if (ist == IST_SYMBOL_QUOTED && text->_scanner._value_image.length < 0) {
                    FAILWITH(IERR_INVALID_SYMBOL);
                }
                text->_scanner._value_image.value = text->_scanner._value_buffer;
                text->_scanner._value_location = SVL_VALUE_IMAGE;
                if (preader->_depth == 0 && text->_annotation_count == 0) {
                    IONCHECK(_ion_reader_text_check_for_no_op_IVM(preader, ist, &is_system_value));
                    if (is_system_value) {
                        // This is a no-op IVM.
                        continue;
                    }
                }
                break;
            }

            // if this is our first annotation, remember where the first annotation started.
            if (text->_annotation_start == -1) {
                text->_annotation_start = text->_scanner._value_start;
            }

            // now we append the annotation ...
            // first we need a string to hold it
            if (text->_annotation_count >= text->_annotation_string_pool_length) {
                FAILWITH(IERR_TOO_MANY_ANNOTATIONS);
            }
            str = &text->_annotation_string_pool[text->_annotation_count++];

            str->value.value = text->_scanner._value_buffer;
            str->value.length = text->_scanner._value_image.length;
            str->sid = UNKNOWN_SID; // Known symbols don't necessarily have local symbol table mappings in text.
            IONCHECK(_ion_reader_text_intern_symbol(preader, &str->value, &sym, ist != IST_SYMBOL_QUOTED));
            if (sym) {
                // The original text was a symbol identifier.
                ASSERT(sym->sid > UNKNOWN_SID);
                ION_STRING_ASSIGN(&str->value, &sym->value);
                str->sid = sym->sid;
                str->psymtab = sym->psymtab;
                str->add_count = sym->add_count;
            }

            // now we check to make sure we have buffer space left for the
            // characters (as bytes of utf8) and a "bonus" null terminator (for safety)
            if (remaining < str->value.length + 1) {
               FAILWITH(IERR_BUFFER_TOO_SMALL);
            }
            memcpy(next_dst, str->value.value, str->value.length);
            next_dst[str->value.length] = '\0'; // remember we should be writing into the annotation value buffer
            str->value.value = next_dst; // Point the annotation pool at the copied text, not the scanner's buffer.
            next_dst  += str->value.length + 1;   // +1 for the null terminator
            remaining -=  str->value.length + 1;
        }
        // we have reached the first bytes of the actual value (after any fieldname or any annotations)

        // if we're at the top level we can look for a system value
        if (text->_current_container != tid_DATAGRAM) {
            break;
        }
        IONCHECK(_ion_reader_text_check_for_system_values_to_skip_or_process(preader, ist, &is_system_value));
        if (is_system_value == FALSE) {
            // not a consumed system value - break out and finish up
            break;
        }
        // it was a system value we consumed, make another loop around
    }

    // if we ended on a doube brace we know we have a LOB but we don't know which kind
    // here we go and find out
    if (ist == IST_DOUBLE_BRACE) {
        IONCHECK(_ion_scanner_next_distinguish_lob(&text->_scanner, &ist));
    }

    text->_annotation_value_next = next_dst; // although I don't think we actually ever use this until we read our set set of annotations
    *p_ist = ist;

    iRETURN;
}

iERR _ion_reader_text_check_for_system_values_to_skip_or_process(ION_READER *preader, ION_SUB_TYPE ist, BOOL *p_is_system_value )
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    BOOL              is_system_value = FALSE;
    BOOL              is_shared_symbol_table, is_local_symbol_table;
    ION_SYMBOL_TABLE *system, *local = NULL;
    void             *owner = NULL;

    // we only look for system values at the top level,
    ASSERT(text->_current_container == tid_DATAGRAM);
    ASSERT(ION_COLLECTION_IS_EMPTY(&text->_container_state_stack));

    // the only system value we skip at the top is a local symbol table, 
    // but we process the version symbol, and shared symbol tables too
    if (ist == IST_SYMBOL_PLAIN) {
        // if we're on a symbol the read uta will have moved the symbol into the value image
        ASSERT(text->_scanner._value_location == SVL_VALUE_IMAGE);

        BOOL has_annotations = text->_annotation_count != 0;
        if (!has_annotations) {
            ION_STRING* maybe_version_marker = &text->_scanner._value_image;
            int major_version;
            int minor_version;

            if (SUCCESS == _ion_reader_text_parse_version_marker(maybe_version_marker, &major_version, &minor_version)) {
                BOOL is_valid_version = (major_version == 1) && (minor_version == 0);
                if (!is_valid_version) {
                    char error_message[ION_ERROR_MESSAGE_MAX_LENGTH];
                    snprintf(error_message, ION_ERROR_MESSAGE_MAX_LENGTH, "Unsupported Ion version %i.%i", major_version, minor_version);
                    FAILWITHMSG(IERR_INVALID_ION_VERSION, error_message);
                } else {
                    // when we see an IVM (ion version marker, aka version T? marker)
                    // we clear out any previous local symbol table and set the symbol
                    // table to be the appropriate system symbol table
                    IONCHECK(_ion_reader_reset_local_symbol_table(preader));
                    is_system_value = TRUE;
                }
            }
        }
    }
    else if (ist == IST_STRUCT && text->_annotation_count > 0) { // an annotated struct is a candidate for a symbol table
        /*
         * When we see a local symbol table (which would be a struct appropriately annotated
         * we will process this if the caller hasn't asked to have system values returned.
         * If they have, then we just return it and it's their problem to read and set the
         * local symbol table. This is not the same as the Java impl.
         */
        
        // TODO - this should be done with flags set while we're
        // recognizing the annotations below (in the fullness of time)
        // TODO in accordance with the spec, only check the FIRST annotation.
        IONCHECK(_ion_reader_text_has_annotation(preader, &ION_SYMBOL_SYMBOL_TABLE_STRING, &is_local_symbol_table));
            
        // if we return system values we don't process them
        if (is_local_symbol_table && preader->options.return_system_values != TRUE) { 
            // this is a local symbol table and the user has not *insisted* we return system values, so we process it
            IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
            IONCHECK(_ion_reader_get_new_local_symbol_table_owner(preader, &owner )); // this owner will be _local_symtab_pool
            // fake the state values so the symbol table load helper will "next" properly
            preader->typed_reader.text._state = IPS_BEFORE_CONTAINER;
            preader->typed_reader.text._value_type = tid_STRUCT;
            IONCHECK(_ion_symbol_table_load_helper(preader, owner, system, &local));
            if (local == NULL) {
                FAILWITH(IERR_NOT_A_SYMBOL_TABLE);
            }
            preader->_current_symtab = local;
            is_system_value = TRUE;
        }
        else if (preader->options.return_shared_symbol_tables != TRUE) { 
            // it wasn't a local symbol table, it might still be a shared symbol table, 
            // but we only process this if the user did not tell us to return shared symbol tables
            IONCHECK(_ion_reader_text_has_annotation(preader, &ION_SYMBOL_SHARED_SYMBOL_TABLE_STRING, &is_shared_symbol_table));
            if (is_shared_symbol_table) {
                IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
                // fake the state values so the symbol table load helper will "next" properly
                preader->typed_reader.text._state = IPS_BEFORE_CONTAINER;
                preader->typed_reader.text._value_type = tid_STRUCT;
                IONCHECK(_ion_symbol_table_load_helper(preader, preader->_catalog->owner, system, &local));
                if (local == NULL) {
                    FAILWITH(IERR_NOT_A_SYMBOL_TABLE);
                }
                IONCHECK(_ion_catalog_add_symbol_table_helper(preader->_catalog, local));
                is_system_value = TRUE;
            }
        }
    }

    *p_is_system_value = is_system_value;
    iRETURN;
}

enum version_marker_state { START, MAJOR_VERSION, UNDERSCORE, MINOR_VERSION };

static inline int add_digit(int i, char digit) {
    return 10 * i + (digit - '0');
}

// Attempts to parse an IVM, returns TRUE if there was an error parsing or FALSE on success
enum version_marker_result _ion_reader_text_parse_version_marker(ION_STRING* version_marker, int* major_version, int* minor_version)
{
    char* prefix = "$ion_";
    size_t prefix_length = strlen(prefix);
    if (version_marker->length <= prefix_length) {
        return ERROR;
    }
    if (0 != strncmp(prefix, version_marker->value, prefix_length)) {
        return ERROR;
    }

    enum version_marker_state state = START;
    char c = '\0';
    int major_version_so_far = 0;
    int minor_version_so_far = 0;

    for (int i = prefix_length; i < version_marker->length; i++) {
        c = version_marker->value[i];
        switch (state) {
            case START:
                if (isdigit(c)) {
                    major_version_so_far = add_digit(major_version_so_far, c);
                    state = MAJOR_VERSION;
                } else {
                    return ERROR;
                }
                break;
            case MAJOR_VERSION:
                if (c == '_') {
                    state = UNDERSCORE;
                } else if (isdigit(c)) {
                    major_version_so_far = add_digit(major_version_so_far, c);
                } else {
                    return ERROR;
                }
                break;
            case UNDERSCORE:
                if (isdigit(c)) {
                    minor_version_so_far = add_digit(minor_version_so_far, c);
                    state = MINOR_VERSION;
                } else {
                    return ERROR;
                }
                break;
            case MINOR_VERSION:
                if (isdigit(c)) {
                    minor_version_so_far = add_digit(minor_version_so_far, c);
                } else {
                    return ERROR;
                }
                break;
        }
    }

    if (state != MINOR_VERSION) {
        return ERROR;
    }

    *major_version = major_version_so_far;
    *minor_version = minor_version_so_far;
    return SUCCESS;
}

// checks to see what follows our just finished value. this is done as 
// we start the next() function. one key bit is this function handles comma's

iERR _ion_reader_text_check_follow_token(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    ION_SUB_TYPE     ist = IST_NONE;
    int              ch = '\0';
    
    ASSERT(preader);

    // we need to check for the numeric stop characters
    switch (ION_TYPE_INT(text->_value_type)) {
    case tid_INT_INT:
    case tid_FLOAT_INT:
    case tid_DECIMAL_INT:
    case tid_TIMESTAMP_INT:
        IONCHECK(_ion_scanner_read_char(&text->_scanner, &ch));
        // TODO make this work with any whitespace and comments
        // (Ion spec wiki indicates only these stop characters--need clarification on spec)
        // negative scanner codes indicate some kind of whitespace (e.g. carriage returns and EOF)
        if (strchr(NUMERIC_STOP_CHARACTERS, ch) == NULL && ch >= 0) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        _ion_scanner_unread_char(&text->_scanner, ch);
        break;
    default:
        // nothing to do
        break;
    }

    // what we should see is a value termination token.
    // for 
    //      datagram: a value or a uta (symbol) or eof
    //      sexp:     a value or a uta (symbol) or an extended symbol or a ')'
    //      list:     a ',' or a ']'
    //      struct:   a "," or a ']'
    IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
// hack: TODO: remove    if (IST_FLAG_IS_ON(ist, FCF_CLOSE_PREV)) {
// hack: TODO: remove        IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
// hack: TODO: remove    }
    if (text->_current_container == tid_DATAGRAM) {
        if ((ist->flags & FCF_DATAGRAM) == 0) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        if (ist == IST_EOF) {
            text->_state = IPS_EOF;
        }
        else {
            IONCHECK(_ion_scanner_un_next(&text->_scanner, ist));
            text->_state = IPS_BEFORE_UTA;
        }
    }
    else if (text->_current_container == tid_SEXP) {
        if ((ist->flags & FCF_SEXP) == 0) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        if (ist == IST_CLOSE_PAREN) {
            text->_state = IPS_EOF;
        }
        else {
            IONCHECK(_ion_scanner_un_next(&text->_scanner, ist));
            text->_state = IPS_BEFORE_UTA;
        }
    }
    else if (text->_current_container == tid_LIST) {
        if ((ist->flags & FCF_LIST) == 0) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        if (ist == IST_COMMA) {
            // we skip the comma following a value this is optional on the last value of a list
            IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
        }
        if (ist == IST_CLOSE_BRACKET) {
            text->_state = IPS_EOF;
        }
        else {
            IONCHECK(_ion_scanner_un_next(&text->_scanner, ist));
            text->_state = IPS_BEFORE_UTA;
        }
    }
    else if (text->_current_container == tid_STRUCT) {
        if ((ist->flags & FCF_STRUCT) == 0) {
            FAILWITH(IERR_INVALID_SYNTAX);
        }
        if (ist == IST_COMMA) {
            // we skip the comma following a value this is optional on the last value of a struct
            IONCHECK(_ion_scanner_next(&text->_scanner, &ist));
        }
        if (ist == IST_CLOSE_SINGLE_BRACE) {
            text->_state = IPS_EOF;
        }
        else {
            IONCHECK(_ion_scanner_un_next(&text->_scanner, ist));
            text->_state = IPS_BEFORE_FIELDNAME;
        }
    }
    else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }

    // make sure we only let extended symbols in the context of sexp
    if (ist == IST_SYMBOL_EXTENDED && text->_current_container != tid_SEXP) {
        FAILWITH(IERR_INVALID_SYNTAX);
    }

    iRETURN;
}   

iERR _ion_reader_text_step_in(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_TYPE         *pparent, new_container_type, old_container_type;

    ASSERT(preader && preader->type == ion_type_text_reader);

    if (text->_state != IPS_BEFORE_CONTAINER) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // since we use it here
    new_container_type = text->_value_type;
    old_container_type = text->_current_container;

    // first, push the current container onto the parent container stack
    pparent = (ION_TYPE *)_ion_collection_push(&text->_container_state_stack);
    *pparent = old_container_type;

    // now we set the current container type
    text->_current_container = new_container_type;
    
    // finally we set the state so the next call to next()
    // will know what to expect in the input stream
    if (new_container_type == tid_STRUCT) {
        text->_state = IPS_BEFORE_FIELDNAME;
    }
    else {
        ASSERT(new_container_type == tid_SEXP || new_container_type == tid_LIST);
        text->_state = IPS_BEFORE_UTA;
    }

    iRETURN;
}

iERR _ion_reader_text_step_out(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_TYPE          new_container_type;
    int               container_depth;

    ASSERT(preader && preader->type == ion_type_text_reader);

    container_depth = ION_COLLECTION_SIZE(&text->_container_state_stack);
    if (container_depth < 1) {
        // if we didn't step in, we can't step out
        FAILWITH(IERR_STACK_UNDERFLOW);
    }

    if (text->_state != IPS_AFTER_VALUE) {
        uint16_t flags = text->_value_sub_type->flags;
        uint16_t looking_at_container = flags & FCF_IS_CONTAINER;
        if (looking_at_container) {
            IONCHECK(_ion_scanner_skip_value_contents(&text->_scanner, text->_value_sub_type));
        }
    }

    ION_SUB_TYPE sub_type = NULL;
    switch (ION_TYPE_INT(text->_current_container)) {
        case tid_SEXP_INT:
            sub_type = IST_SEXP;
            break;
        case tid_STRUCT_INT:
            sub_type = IST_STRUCT;
            break;
        case tid_LIST_INT:
            sub_type = IST_LIST;
            break;
        default: {
            char error_message[ION_ERROR_MESSAGE_MAX_LENGTH];
            snprintf(error_message, ION_ERROR_MESSAGE_MAX_LENGTH, "Unable to step out of unrecognized container type %s", text->_current_container);
            FAILWITHMSG(IERR_INVALID_STATE, error_message);
        }
    }
    if (text->_value_type != tid_EOF) {
        IONCHECK(_ion_scanner_skip_value_contents(&text->_scanner, sub_type));
    }

    text->_value_type = text->_current_container;
    text->_value_sub_type = sub_type;
    text->_state = IPS_AFTER_VALUE;

    // remember we have to use the value BEFORE popping it off the stack (and, possibly,
    // having it deallocated underneath us). so get the parent container type off the
    // top of the stack first.
    new_container_type = *((ION_TYPE *)_ion_collection_head(&text->_container_state_stack));
    
    // now we can safely pop the parent container off of the parent container stack
    _ion_collection_pop_head(&text->_container_state_stack);

    // now we set the current container type
    text->_current_container = new_container_type;
    
    iRETURN;
}



iERR _ion_reader_text_get_depth(ION_READER *preader, SIZE *p_depth)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    int container_depth;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_depth);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    container_depth = ION_COLLECTION_SIZE(&text->_container_state_stack);
    *p_depth = container_depth;

    iRETURN;
}

iERR _ion_reader_text_get_type(ION_READER *preader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_value_type);

    if (text->_state == IPS_ERROR) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if (text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_value_type = text->_value_type;

    iRETURN;
}

iERR _ion_reader_text_is_null(ION_READER *preader, BOOL *p_is_null)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_is_null);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // we just look at the is null flag in our value sub type to know this
    *p_is_null = ((text->_value_sub_type->flags & FCF_IS_NULL) != 0);

    iRETURN;
}

iERR _ion_reader_text_has_any_annotations(ION_READER *preader, BOOL *p_has_annotations)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_has_annotations);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_has_annotations = (text->_annotation_count > 0);

    iRETURN;
}

iERR _ion_reader_text_has_annotation(ION_READER *preader, ION_STRING *annotation, BOOL *p_annotation_found)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_SYMBOL       *str;
    SIZE              count;
    BOOL              found = FALSE;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(annotation);
    ASSERT(p_annotation_found);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // we just do this the hard way. If there are more than a few annotations
    // on a single value - there's something wrong and slow is a fine response
    count = text->_annotation_count;
    for (str = text->_annotation_string_pool; count--; str++) { // postfix decrement count since we only stop after we saw the last one
        if (ION_STRING_EQUALS(&str->value, annotation)) {
            found = TRUE;
            break;
        }
    }

    // we either found a match, or we didn't. let's tell the call what happened
    *p_annotation_found = found;

    iRETURN;
}

iERR _ion_reader_text_get_annotation_count(ION_READER *preader, int32_t *p_count)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_count);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_count = text->_annotation_count;

    iRETURN;
}

iERR _ion_reader_text_get_an_annotation(ION_READER *preader, int32_t idx, ION_STRING *p_str)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_SYMBOL       *str;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_str != NULL);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (idx < 0 || idx >= text->_annotation_count) {
        FAILWITH(IERR_INVALID_ARG);
    }

    // get a pointer to our string header in the annotation string pool
    str = text->_annotation_string_pool + idx;
    IONCHECK(ion_string_copy_to_owner(preader->_temp_entity_pool, p_str, &str->value));
    if (str->sid == 0) {
        ION_STRING_INIT(p_str); // This nulls the string, because symbol 0 has no text representation.
    }

    iRETURN;
}

iERR _ion_reader_text_get_an_annotation_sid(ION_READER *preader, int32_t idx, SID *p_sid)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_SYMBOL       *str;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_sid != NULL);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (idx < 0 || idx >= text->_annotation_count) {
        FAILWITH(IERR_INVALID_ARG);
    }

    // get a pointer to our string header in the annotation string pool
    str = text->_annotation_string_pool + idx;
    *p_sid = str->sid;

    iRETURN;
}

iERR _ion_reader_text_get_annotations(ION_READER *preader, ION_STRING *p_strs, int32_t max_count, int32_t *p_count)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_SYMBOL       *src;
    ION_STRING       *dst;
    int32_t           idx;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_strs != NULL);
    ASSERT(p_count != NULL);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (max_count < text->_annotation_count) {
        FAILWITH(IERR_BUFFER_TOO_SMALL);
    }

    src = text->_annotation_string_pool;
    dst = p_strs;
    for (idx = 0; idx < text->_annotation_count; idx++) {
        IONCHECK(ion_string_copy_to_owner(preader->_temp_entity_pool, dst, &src->value));
        if (src->sid == 0) {
            ION_STRING_INIT(dst); // This nulls the string, because symbol 0 has no text representation.
        }
        dst++;
        src++;
    }

    *p_count = text->_annotation_count;

    iRETURN;
}

iERR _ion_reader_text_get_field_name(ION_READER *preader, ION_STRING **p_pstr)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_pstr);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // TODO: I'm not sure I like this. The caller is getting a pointer 
    //       into the middle of the parser struct
    if (text->_field_name.sid == 0) {
        *p_pstr = NULL;
    }
    else {
        *p_pstr = &text->_field_name.value;
    }

    iRETURN;
}

iERR _ion_reader_text_get_symbol_table(ION_READER *preader, ION_SYMBOL_TABLE **p_return)
{
    iENTER;
    ION_SYMBOL_TABLE *system;

    if (preader->_current_symtab == NULL) {
        IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
        preader->_current_symtab = system;
    }
    *p_return = preader->_current_symtab;

    iRETURN;
}


iERR _ion_reader_text_get_field_sid(ION_READER *preader, SID *p_sid)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_sid);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_sid = text->_field_name.sid;
    iRETURN;
}

iERR _ion_reader_text_get_annotation_sids(ION_READER *preader, SID *p_sids, SIZE max_count, SIZE *p_count)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_SYMBOL       *str;
    SIZE              ii, count;

    ASSERT(preader);
    ASSERT(p_sids);
    ASSERT(p_count);

    count = text->_annotation_count;
    if (max_count < count) {
        FAILWITH(IERR_INVALID_ARG);
    }

    for (ii = 0; ii<count; ii++) {
        str = &text->_annotation_string_pool[ii];
        p_sids[ii] = str->sid;
    }
    *p_count = count;

    iRETURN;
}

iERR _ion_reader_text_get_value_offset(ION_READER *preader, POSITION *p_offset)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    POSITION          offset;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_offset);

    // return -1 on eof (alternatively we could "throw" an eof error
    if (preader->_eof) {
        offset = -1;
    }
    else {
        if (text->_annotation_start >= 0) {
            // if the value was annotated we need to back up and include
            // the annotation, since the annotation is part of the value.
            offset = text->_annotation_start;
        }
        else {
            offset = text->_value_start;
        }
    }

    *p_offset = offset;
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_text_get_value_length(ION_READER *preader, SIZE *p_length)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    SIZE              length;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_length);


    // return -1 on eof (alternatively we could "throw" an eof error
    if (preader->_eof) {
        length = -1;
    }
    else {

        // TODO: what do we want to do here? If this is the length
        //       of the value in the input stream we could scan to
        //       the end of the value and return that length. Or we
        //       could convert the value to it's binary form (probably
        //       correct for some use cases) and return the binary
        //       length - but that's expensive.
        //
        // for now we "fail"

        length = -1;
    }
    *p_length = length;

    SUCCEED();
    iRETURN;
}

iERR _ion_reader_text_read_null(ION_READER *preader, ION_TYPE *p_value)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader && preader->type == ion_type_text_reader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) == 0) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // we just look at the is null flag in our value sub type to know this
    *p_value= text->_value_sub_type->base_type;

    iRETURN;
}

iERR _ion_reader_text_read_bool(ION_READER *preader, BOOL *p_value)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;

    ASSERT(preader);
    ASSERT(preader->type == ion_type_text_reader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR || text->_state == IPS_NONE) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // for boolean value the value sub type tells us everything we need to know
    if (text->_value_sub_type == IST_BOOL_TRUE) {
        *p_value = TRUE;
    }
    else if (text->_value_sub_type == IST_BOOL_FALSE) {
        *p_value = FALSE;
    }
    else {
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR _ion_reader_text_read_mixed_int_helper(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    SIZE             len;

    ASSERT(preader);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE
     || text->_value_sub_type->base_type != tid_INT
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    // we'll look at the length of the image and use this to guess
    // which size of int we need to read - it's ok to guess large

    len = text->_scanner._value_image.length; // decimal or hex characters
    preader->_int_helper._is_ion_int = TRUE;  // we will default to var len int
    
    if (text->_value_sub_type == IST_INT_NEG_DECIMAL) {
        len --; // discount the "-"
        if (len < 19) { // 9,223,372,036,854,775,807 is max signed int64, 19 decimal chars
            preader->_int_helper._is_ion_int = FALSE;
        }
    }
    else if (text->_value_sub_type == IST_INT_NEG_HEX) {
        len --; // discount the "-"
        len -= 2; // discount the "0x" prefix
        if ((len / 2) < 16) { // 64 bit int is 16 hex chars
            preader->_int_helper._is_ion_int = FALSE;
        }
    }
    else if (text->_value_sub_type == IST_INT_NEG_BINARY) {
        len --; // discount the "-"
        len -= 2; // discount the "0b" prefix
        if ((len / 2) < 64) { // 64 bit int is 64 binary chars
            preader->_int_helper._is_ion_int = FALSE;
        }
    }
    else if (text->_value_sub_type == IST_INT_POS_DECIMAL) {
        /* no adjustment */
        if (len < 19) { // 9,223,372,036,854,775,807 is max signed int64, 19 decimal chars
            preader->_int_helper._is_ion_int = FALSE;
        }
    }
    else if (text->_value_sub_type == IST_INT_POS_HEX) {
        len -= 2; // discount the "0x" prefix
        if ((len / 2) < 16) { // 64 bit int is 16 hex chars
            preader->_int_helper._is_ion_int = FALSE;
        }
    }
    else if (text->_value_sub_type == IST_INT_POS_BINARY) {
        len -= 2; // discount the "0b" prefix
        if ((len / 2) < 64) { // 64 bit int is 64 binary chars
            preader->_int_helper._is_ion_int = FALSE;
        }
    }

    // read it in using the appropriate helper
    if (preader->_int_helper._is_ion_int) {
        IONCHECK(_ion_reader_read_ion_int_helper(preader, &preader->_int_helper._as_ion_int));
    }
    else {
        IONCHECK(_ion_reader_text_read_int64(preader, &preader->_int_helper._as_int64));
    }

    iRETURN;
}

iERR _ion_reader_text_read_int32(ION_READER *preader, int32_t *p_value)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    int64_t          value64;

    IONCHECK(_ion_reader_text_read_int64(preader, &value64));
    if (value64 > MAX_INT32 || value64 < MIN_INT32) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    *p_value = (int32_t)value64;

    iRETURN;
}

iERR _ion_reader_text_read_int64(ION_READER *preader, int64_t *p_value)
{
    iENTER;
    ION_TEXT_READER    *text = &preader->typed_reader.text;
    char               *value_start, *value_end;
    unsigned long long  magnitude;
    BOOL                sign = FALSE;
 
    ASSERT(preader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || text->_value_sub_type->base_type != tid_INT
     ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    if (text->_value_sub_type == IST_INT_NEG_DECIMAL || text->_value_sub_type == IST_INT_NEG_HEX) {
        sign = TRUE;
    }

    value_end = text->_scanner._value_image.value + text->_scanner._value_image.length;
    value_start = text->_scanner._value_image.value; // if this is hexadecimal, we may need to skip past the "0x"

    // convert only the magnitude
    if (text->_value_sub_type == IST_INT_POS_DECIMAL) {
        magnitude = STR_TO_UINT64(value_start, &value_end, 10);
    }
    else if (text->_value_sub_type == IST_INT_NEG_DECIMAL) {
        // we add 1 to go past the "-"
        magnitude = STR_TO_UINT64(value_start + 1, &value_end, 10);
    }
    else if (text->_value_sub_type == IST_INT_POS_HEX) {
        // base is now 16, and we add 2 to the start for the "0x"
        magnitude = STR_TO_UINT64(value_start + 2, &value_end, II_HEX_BASE);
    }
    else if (text->_value_sub_type == IST_INT_NEG_HEX) {
        // base is now 16, and we add 3 to the start for the "-0x"
        magnitude = STR_TO_UINT64(value_start + 3, &value_end, II_HEX_BASE);
    }
    else if (text->_value_sub_type == IST_INT_POS_BINARY) {
        // base is now 2, and we add 2 to the start for the "0b"
        magnitude = STR_TO_UINT64(value_start + 2, &value_end, II_BINARY_BASE);
    }
    else if (text->_value_sub_type == IST_INT_NEG_BINARY) {
        // base is now 2, and we add 3 to the start for the "-0b"
        magnitude = STR_TO_UINT64(value_start + 3, &value_end, II_BINARY_BASE);
    }
    else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }

    if ((!sign && magnitude > ((uint64_t) LLONG_MAX)) || (sign && magnitude > (((uint64_t) LLONG_MAX) + 1))) {
        FAILWITHMSG(IERR_NUMERIC_OVERFLOW, "value too large for type int64_t");
    }

    // deal with sign conversion since we only parsed the value without the sign
    if (sign && magnitude <= LLONG_MAX) {
        // convert the magnitude iff <= LLONG_MAX
        *p_value = -((int64_t) magnitude);  
    } else {
        // either we have a positive or exactly LLONG_MIN
        // in which case we can just cast to signed int
        *p_value = (int64_t) magnitude;
    }
    iRETURN;
}

iERR _ion_reader_text_read_ion_int_helper(ION_READER *preader, ION_INT *p_value)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;

    ASSERT(preader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || text->_value_sub_type->base_type != tid_INT
     ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }
 
    // choose the right ion_int conversion routine
    if (text->_value_sub_type == IST_INT_POS_DECIMAL || text->_value_sub_type == IST_INT_NEG_DECIMAL) {
        IONCHECK(ion_int_from_string(p_value, &text->_scanner._value_image));
    }
    else if (text->_value_sub_type == IST_INT_POS_HEX || text->_value_sub_type == IST_INT_NEG_HEX) {
        IONCHECK(ion_int_from_hex_string(p_value, &text->_scanner._value_image));
    }
    else if (text->_value_sub_type == IST_INT_POS_BINARY || text->_value_sub_type == IST_INT_NEG_BINARY) {
        IONCHECK(ion_int_from_binary_string(p_value, &text->_scanner._value_image));
    }
    else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }

    iRETURN;
}

iERR _ion_reader_text_read_double(ION_READER *preader, double *p_value)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    double           double_value;

    ASSERT(preader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || text->_value_sub_type->base_type != tid_FLOAT
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }
    
    if (text->_value_sub_type == IST_FLOAT_64) {
        ASSERT(text->_scanner._value_location == SVL_VALUE_IMAGE);
        ASSERT(text->_scanner._value_image.length > 0);
        ASSERT(text->_scanner._value_image.value[text->_scanner._value_image.length] == 0);
        double_value = atof(text->_scanner._value_image.value);
    }
    else if (text->_value_sub_type == IST_PLUS_INF) {
        double_value = ION_FLOAT64_POS_INF;
    }
    else if (text->_value_sub_type == IST_MINUS_INF) {
        double_value = ION_FLOAT64_NEG_INF;
    }
    else if (text->_value_sub_type == IST_NAN) {
        double_value = ION_FLOAT64_NAN;
    }
    else {
        FAILWITH(IERR_PARSER_INTERNAL);
    }
    *p_value = double_value;

    iRETURN;
}

iERR _ion_reader_text_read_decimal(ION_READER *preader, decQuad *p_quad, decNumber **p_num)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;

    ASSERT(preader);
    ASSERT(p_quad);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || text->_value_sub_type->base_type != tid_DECIMAL
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }
    
    ASSERT(text->_scanner._value_location == SVL_VALUE_IMAGE);
    ASSERT(text->_scanner._value_image.length > 0);
    ASSERT(text->_scanner._value_image.value[text->_scanner._value_image.length] == 0);

    IONCHECK(_ion_decimal_from_string_helper((char *)text->_scanner._value_image.value, &preader->_deccontext,
                                             preader, p_quad, p_num));

    iRETURN;
}

iERR _ion_reader_text_read_timestamp(ION_READER *preader, ION_TIMESTAMP *p_value)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    SIZE             used;

    ASSERT(preader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || text->_value_sub_type->base_type != tid_TIMESTAMP
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }
    
    ASSERT(text->_scanner._value_location == SVL_VALUE_IMAGE);
    ASSERT(text->_scanner._value_image.length > 0);
    ASSERT(text->_scanner._value_image.value[text->_scanner._value_image.length] == 0);

    IONCHECK(ion_timestamp_parse(p_value
                               , text->_scanner._value_image.value
                               , text->_scanner._value_image.length
                               , &used
                               , &preader->_deccontext
    ));
    if (text->_scanner._value_image.length != used) {
        // TODO - DO WE CARE?
    }

    iRETURN;
}

iERR _ion_reader_text_read_symbol_sid(ION_READER *preader, SID *p_value)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    BOOL             eos_encountered = FALSE;

    ASSERT(preader);
    ASSERT(p_value);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || text->_value_sub_type->base_type != tid_SYMBOL
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    if (text->_scanner._value_location == SVL_IN_STREAM) {
        IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                            , text->_scanner._value_buffer
                                            , text->_scanner._value_buffer_length
                                            , text->_value_sub_type
                                            , &(text->_scanner._value_image.length)
                                            , &eos_encountered
        ));
        if (eos_encountered == FALSE) {
            FAILWITH(IERR_BUFFER_TOO_SMALL)
        }
        text->_scanner._value_location = SVL_VALUE_IMAGE;
        text->_scanner._value_image.value = text->_scanner._value_buffer;
    }
    
    ASSERT(text->_scanner._value_location == SVL_VALUE_IMAGE);
    ASSERT(text->_scanner._value_image.length > 0);
    ASSERT(text->_scanner._value_image.value[text->_scanner._value_image.length] == 0);

    IONCHECK(_ion_symbol_table_find_by_name_helper(preader->_current_symtab, &text->_scanner._value_image, p_value, NULL,
                                                  text->_value_sub_type != IST_SYMBOL_QUOTED));

    iRETURN;
}

iERR _ion_reader_text_read_symbol(ION_READER *preader, ION_SYMBOL *p_symbol)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_STRING       *user_str = &text->_scanner._value_image;
    ION_SYMBOL       *sym;

    ASSERT(preader);
    ASSERT(p_symbol);

    if (text->_state == IPS_ERROR
        || text->_state == IPS_NONE
        || (text->_value_sub_type->base_type != tid_SYMBOL)
        ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    IONCHECK(_ion_reader_text_load_string_in_value_buffer(preader));
    IONCHECK(_ion_reader_text_intern_symbol(preader, user_str, &sym, text->_value_sub_type != IST_SYMBOL_QUOTED));
    if (sym) {
        // This was a symbol identifier, e.g. $10. The user should be presented with the symbol text, not the SID.
        ASSERT(sym->sid > UNKNOWN_SID); // Success to this point means the symbol is defined.
        ION_STRING_ASSIGN(&p_symbol->value, &sym->value);
        p_symbol->sid = sym->sid;
    }
    else {
        // This is a regular text symbol. No local symbol table mapping is required.
        ION_STRING_ASSIGN(&p_symbol->value, user_str);
        p_symbol->sid = UNKNOWN_SID;
    }
    iRETURN;
}

iERR _ion_reader_text_get_string_length(ION_READER *preader, SIZE *p_length)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    BYTE             terminator = '"';
    BOOL             eos_encountered;

    ASSERT(preader);
    ASSERT(p_length);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || (text->_value_sub_type->base_type != tid_SYMBOL && text->_value_sub_type->base_type != tid_STRING)
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }
    
    if (text->_scanner._value_location == SVL_IN_STREAM) {
        IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                            , text->_scanner._value_buffer
                                            , text->_scanner._value_buffer_length
                                            , text->_value_sub_type
                                            , &(text->_scanner._value_image.length)
                                            , &eos_encountered
        ));
        if (eos_encountered == FALSE) {
            FAILWITH(IERR_BUFFER_TOO_SMALL)
        }
        text->_scanner._value_location = SVL_VALUE_IMAGE;
        text->_scanner._value_image.value = text->_scanner._value_buffer;
    }

    *p_length = text->_scanner._value_image.length;

    iRETURN;
}

iERR _ion_reader_text_read_string(ION_READER *preader, ION_STRING *p_user_str)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    ION_STRING       *user_str = &text->_scanner._value_image;
    ION_SYMBOL       *sym;

    ASSERT(preader);
    ASSERT(p_user_str);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || (
          text->_value_sub_type->base_type != tid_SYMBOL
        &&
          text->_value_sub_type->base_type != tid_STRING
        )
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    // force the contents to be loaded into our string buffer
    // note that this is an extra copy but we have to know
    // how long it is to allocate the users string buffer
    IONCHECK(_ion_reader_text_load_string_in_value_buffer(preader));

    if (text->_value_sub_type->base_type == tid_SYMBOL) {
        IONCHECK(_ion_reader_text_intern_symbol(preader, user_str, &sym, text->_value_sub_type != IST_SYMBOL_QUOTED));
        if (sym) {
            // This was a symbol identifier, e.g. $10. The user should be presented with the symbol text, not the SID.
            ASSERT(sym->sid > UNKNOWN_SID); // Success to this point means the symbol is defined.
            ION_STRING_ASSIGN(p_user_str, &sym->value);
        }
        else {
            // This is a regular text symbol. No local symbol table mapping is required.
            ION_STRING_ASSIGN(p_user_str, user_str);
        }
    }
    else {
        if (ION_STRING_IS_NULL(user_str)) {
            // Attempting to read null.string with this API is an error. Note that when the underlying value is a
            // symbol, a null ION_STRING may be returned to represent a symbol with unknown text.
            FAILWITH(IERR_NULL_VALUE);
        }
        ION_STRING_ASSIGN(p_user_str, user_str);
    }

    iRETURN;
}       

iERR _ion_reader_text_load_string_in_value_buffer(ION_READER *preader)
{
    iENTER;
    ION_TEXT_READER *text = &preader->typed_reader.text;
    BYTE             terminator = '"';
    BOOL             eos_encountered;

    ASSERT(preader);
    
    if (text->_scanner._value_location == SVL_IN_STREAM) {
        
        IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                            , text->_scanner._value_buffer
                                            , text->_scanner._value_buffer_length
                                            , text->_value_sub_type
                                            , &(text->_scanner._value_image.length)
                                            , &eos_encountered
        ));
        if (eos_encountered == FALSE) {
            FAILWITH(IERR_BUFFER_TOO_SMALL)
        }
        text->_scanner._value_location = SVL_VALUE_IMAGE;
        text->_scanner._value_image.value = text->_scanner._value_buffer;
    }

    iRETURN;
}       

iERR _ion_reader_text_read_string_bytes(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length) 
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    SIZE              written, remaining;
    BOOL              eos_encountered;

    ASSERT(preader);
    ASSERT(p_buf);
    ASSERT(buf_max > 0);
    ASSERT(p_length);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || (
          text->_value_sub_type->base_type != tid_SYMBOL
        &&
          text->_value_sub_type->base_type != tid_STRING
        )
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    if (text->_scanner._value_location == SVL_VALUE_IMAGE) {
        // copy bytes out of text->_scanner.value - note we remove those
        // altogether and reduce the value length
        written = text->_scanner._value_image.length;
        if (written > buf_max) {
            if (accept_partial == FALSE) {
                FAILWITH(IERR_BUFFER_TOO_SMALL);
            }
            written = buf_max;
        }
        memcpy(p_buf, text->_scanner._value_image.value, written);
        if (written < text->_scanner._value_image.length) {
            remaining = text->_scanner._value_image.length - written;
            text->_scanner._value_image.value += written;
            text->_scanner._value_image.length -= remaining;
        }
        else {
            text->_scanner._value_image.length = 0;
        }
    }
    else if (text->_scanner._value_location == SVL_IN_STREAM) {
        // bytes are in the stream, so we copy them out while utf8 converting
        // and handling escape sequences as needed
        IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                           , p_buf
                                           , buf_max
                                           , text->_value_sub_type
                                           , &written
                                           , &eos_encountered
        ));
        if (eos_encountered && accept_partial == FALSE) {
            FAILWITH(IERR_BUFFER_TOO_SMALL);
        }
    }
    else {
        // this case occurs when the value has been consumed and the user asks for more
        written = 0;
    }

    *p_length = written;

    iRETURN;
}       

iERR _ion_reader_text_get_lob_size(ION_READER *preader, SIZE *p_length)
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    BOOL              eos_encountered = FALSE;


    ASSERT(preader);
    ASSERT(p_length);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
    ) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    if (text->_scanner._value_location == SVL_IN_STREAM) {
        // if the caller wants the length we have to copy the value over to our
        // value buffer to get it
        if (text->_value_sub_type->base_type == tid_CLOB) {

            IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                               ,  text->_scanner._value_buffer
                                               ,  text->_scanner._value_buffer_length
                                               ,  text->_value_sub_type
                                               , &text->_scanner._value_image.length
                                               , &eos_encountered
            ));
        }
        else if (text->_value_sub_type == IST_BLOB) {

            IONCHECK(_ion_scanner_read_as_base64(&text->_scanner
                                               ,  text->_scanner._value_buffer
                                               ,  text->_scanner._value_buffer_length
                                               , &text->_scanner._value_image.length
                                               , &eos_encountered
            ));
        }
        else {
            FAILWITH(IERR_INVALID_STATE);
        }
        if (eos_encountered) {
            text->_scanner._value_location    = SVL_VALUE_IMAGE;
            text->_scanner._value_image.value = text->_scanner._value_buffer;
        }
        else {
            FAILWITH(IERR_LOOKAHEAD_OVERFLOW);
        }
    }

    // it better be in the value buffer by now or we don't know what to do
    if (text->_scanner._value_location != SVL_VALUE_IMAGE) {
        FAILWITH(IERR_INVALID_STATE);
    }
    *p_length = text->_scanner._value_image.length;

    iRETURN;
}

iERR _ion_reader_text_read_lob_bytes(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length) 
{
    iENTER;
    ION_TEXT_READER  *text = &preader->typed_reader.text;
    SIZE              written, remaining;
    BOOL              eos_encountered = FALSE;

    ASSERT(preader);
    ASSERT(p_buf);
    ASSERT(buf_max);
    ASSERT(p_length);

    if (text->_state == IPS_ERROR 
     || text->_state == IPS_NONE 
     || ((text->_value_sub_type != IST_CLOB_PLAIN) && (text->_value_sub_type != IST_CLOB_LONG) && (text->_value_sub_type != IST_BLOB))
    ) {
        FAILWITH(IERR_INVALID_STATE);

    }
    if ((text->_value_sub_type->flags & FCF_IS_NULL) != 0) {
        FAILWITH(IERR_NULL_VALUE);
    }

    if (text->_scanner._value_location == SVL_IN_STREAM) {
        // if the caller wants the length we have to copy the value over to our
        // value buffer to get it
        if (text->_value_sub_type->base_type == tid_CLOB) {

            IONCHECK(_ion_scanner_read_as_string(&text->_scanner
                                               ,  p_buf
                                               ,  buf_max
                                               ,  text->_value_sub_type
                                               , &written
                                               , &eos_encountered
            ));

        }
        else if (text->_value_sub_type == IST_BLOB) {

            IONCHECK(_ion_scanner_read_as_base64(&text->_scanner, p_buf, buf_max, &written, &eos_encountered));

        }
        else {
            FAILWITH(IERR_INVALID_STATE);
        }
        if (!eos_encountered && accept_partial == FALSE) {
            FAILWITH(IERR_BUFFER_TOO_SMALL);
        }
    }
    else if (text->_scanner._value_location == SVL_VALUE_IMAGE) {
        written = text->_scanner._value_image.length;
        if (written > buf_max) {
            if (accept_partial == FALSE) {
                FAILWITH(IERR_BUFFER_TOO_SMALL);
            }
            written = buf_max;
        }

        memcpy(p_buf, text->_scanner._value_image.value, written);

        if (written < text->_scanner._value_image.length) {
            remaining = text->_scanner._value_image.length - written;
            text->_scanner._value_image.value += written;
            text->_scanner._value_image.length -= remaining;
        }
        else {
            // this case occurs when the value has been consumed and the user asks for more
            text->_scanner._value_image.length = 0;
            eos_encountered = TRUE;
        }
    }
    else {
        // this happens when we have read all the value and have marked the value location as none
        written = 0;

    }
    
    *p_length = written;

    iRETURN;
}
