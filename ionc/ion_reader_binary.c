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
// implements the Ion binary reader
// from class IonBinaryIterator
//

#include "ion_internal.h"
#include "ion_reader_impl.h"

iERR _ion_reader_binary_local_read_length(ION_READER *preader, int tid, int *p_length);
iERR _ion_binary_reader_fits_container(ION_READER *preader, SIZE len);
iERR _ion_reader_binary_local_process_possible_magic_cookie(ION_READER *preader, int td, BOOL *p_is_system_value);
iERR _ion_reader_binary_local_process_possible_symbol_table(ION_READER *preader, BOOL *p_is_system_value);

//
//  actual "public" functions
//

iERR _ion_reader_binary_open(ION_READER *preader)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader);

    binary = &preader->typed_reader.binary;

    _ion_collection_initialize(preader, &binary->_parent_stack, sizeof(BINARY_PARENT_STATE)); // array of BINARY_PARENT_STATE
    _ion_collection_initialize(preader, &binary->_annotation_sids, sizeof(SID)); // array of SID's

    binary->_local_end = ION_STREAM_MAX_LENGTH;
    binary->_state = S_BEFORE_TID;

    // the original Java reader was intended to allow opening an iterator
    // over an internal buffer so the parent might be different, this
    // reader could be adapted fairly easily to do this, but not until
    // there's an actual use case
    binary->_parent_tid = TID_DATAGRAM;
    binary->_value_symbol_id = UNKNOWN_SID;
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_reset(ION_READER *preader, int parent_tid, POSITION local_end)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader);
    ASSERT(parent_tid == TID_DATAGRAM); // TODO: support values other than DATAGRAM

    binary = &preader->typed_reader.binary;

    _ion_collection_reset(&binary->_parent_stack); // array of BINARY_PARENT_STATE
    _ion_collection_reset(&binary->_annotation_sids); // array of SID's

    binary->_state = S_BEFORE_TID;

    // the original Java reader was intended to allow opening an iterator
    // over an internal buffer so the parent might be different, this
    // reader could be adapted fairly easily to do this, but not until
    // there's an actual use case
    binary->_parent_tid = parent_tid;

    binary->_local_end = local_end;
    binary->_value_symbol_id = UNKNOWN_SID;

    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_close(ION_READER *preader)
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

iERR _ion_reader_binary_next(ION_READER *preader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_BINARY_READER *binary;
    POSITION           value_start, annotation_end, pos;
    int                type_desc_byte, ion_type_id;
    int                length;
    uint32_t           field_sid, annotation_len;
    SIZE               skipped;
    SID               *psid;
    BOOL               is_system_value = FALSE;
    POSITION           annotation_content_start, value_content_start;
begin:

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    // get actual type id, this also handle the hasNext & eof logic as necessary
    if (preader->_eof) {
        goto at_eof;
    }

    // if we stepped into the value with next() and then
    // decided never read the value itself we have to skip
    // over the value contents here
    if (binary->_state == S_BEFORE_CONTENTS && binary->_value_len) {
        IONCHECK(ion_stream_skip(preader->istream, binary->_value_len, &skipped));
        if (binary->_value_len != skipped) FAILWITH(IERR_UNEXPECTED_EOF);
    }

    value_start = ion_stream_get_position(preader->istream);
    if (value_start >= binary->_local_end) {
        goto at_eof;
    }

    // reset the value fields
    type_desc_byte = -1;
    _ion_collection_reset(&binary->_annotation_sids);

    // read the field sid if we are in a structure
    if (binary->_in_struct) {
        IONCHECK(ion_binary_read_var_uint_32(preader->istream, &field_sid));
        binary->_value_field_id = field_sid;
    }
    else {
        binary->_value_field_id = -1;
    }

    // the "Possible" routines return -1 if they
    // found, and consumed, interesting data. And
    // if they did we need to read the next tid byte
    for (;;) {
        value_start = ion_stream_get_position(preader->istream); // the field name isn't part of the value
        ION_GET(preader->istream, type_desc_byte);               // read the TID byte
        if (type_desc_byte == EOF) {
            goto at_eof;
        }
        
        // first check for the magic cookie - especially since the first byte
        // says this is an annotation (with an, otherwise, invalid length of zero)
        if (preader->_depth == 0 && type_desc_byte == ION_VERSION_MARKER[0])
        {
            IONCHECK(_ion_reader_binary_local_process_possible_magic_cookie(preader, type_desc_byte, &is_system_value));
            if (!is_system_value) FAILWITH(IERR_INVALID_BINARY); // E0 is not a TID.
            continue;
        }

        // mark where we are
        binary->_value_tid = type_desc_byte;
        binary->_state = S_AFTER_TID;

        // get actual type id
        ion_type_id = getTypeCode(type_desc_byte);

        if (getTypeCode(type_desc_byte) == TID_UTA)
        {
            // but if there is a user type annotation
            // we read the annotation list in here

            //      set annotation start to the position of
            //      the first type desc byte
            binary->_annotation_start = value_start;

            //      first we skip the value length and then
            //      read the local annotation length
            IONCHECK(_ion_reader_binary_local_read_length(preader, binary->_value_tid, &length));
            annotation_content_start = ion_stream_get_position(preader->istream);

            // read the length of the annotation list here
            IONCHECK(ion_binary_read_var_uint_32(preader->istream, &annotation_len));
            if (annotation_len < 1) FAILWITH(IERR_INVALID_BINARY);

            // no - we'll just read them now while we're in the neighborhood
            annotation_end = ion_stream_get_position(preader->istream) + annotation_len;
            for (;;) {
                pos = ion_stream_get_position(preader->istream);
                if (pos >= annotation_end) break;
                psid = (SID *)_ion_collection_append(&binary->_annotation_sids);
                if (!psid) FAILWITH(IERR_NO_MEMORY);
                IONCHECK(ion_binary_read_var_uint_32(preader->istream, (uint32_t*)psid));
            }

            //      read tid again
            value_start = ion_stream_get_position(preader->istream); // we have a new value start
            ION_GET(preader->istream, binary->_value_tid);           // read the TID byte, the beginning of a value
            ion_type_id = getTypeCode(binary->_value_tid);
            if (ion_type_id == TID_UTA) {
                // Nested annotations are forbidden
                FAILWITH(IERR_INVALID_BINARY);
            }

            IONCHECK(_ion_reader_binary_local_read_length(preader, binary->_value_tid, &binary->_value_len));
            value_content_start = ion_stream_get_position(preader->istream);

            if (ion_type_id == TID_SYMBOL) {
                IONCHECK(_ion_reader_binary_read_symbol_sid_helper(preader, binary, &binary->_value_symbol_id));
            }

            POSITION expected_end = annotation_content_start + length;
            POSITION actual_end = value_content_start + binary->_value_len;
            if (expected_end != actual_end) {
                FAILWITH(IERR_INVALID_BINARY);
            }

            if (preader->_depth == 0 && ion_type_id == TID_STRUCT) {
                // this looks at the current value, checks to see if it has the
                // $ion_1_0 annoation and if it does load the symbol table and
                // move forward, otherwise just read the actual values td and
                // return that instead.  If it's not a symbol table, then the 14
                // (user type annotation) will be handled during "next()"
                IONCHECK(_ion_reader_binary_local_process_possible_symbol_table(preader, &is_system_value));
                if (is_system_value) continue;
            }
        }
        else {
            // Not an annotation, so just clear the annotation marker that may be left over from our previous value.
            binary->_annotation_start = -1;
            IONCHECK(_ion_reader_binary_local_read_length(preader, binary->_value_tid, &binary->_value_len));
            if (getTypeCode(type_desc_byte) == TID_SYMBOL) {
                // A non-null symbol at the top-level, not in an annotation wrapper, with SID 2 is a faux IVM (a no-op).
                if (getLowNibble(type_desc_byte) != ION_lnIsNull) {
                    IONCHECK(_ion_reader_binary_read_symbol_sid_helper(preader, binary, &binary->_value_symbol_id));
                    if (preader->_depth == 0 && binary->_value_symbol_id == ION_SYS_SID_IVM) {
                        binary->_value_symbol_id = UNKNOWN_SID; // Go around again, skipping this value.
                        continue;
                    }
                }
            }
            else if(getTypeCode(binary->_value_tid) == TID_NULL && getLowNibble(binary->_value_tid) != ION_lnIsNull) {
                // This is NOP padding.
                if (binary->_value_len) {
                    if (binary->_value_len > (preader->istream->_limit - preader->istream->_curr)) {
                        FAILWITH(IERR_UNEXPECTED_EOF);
                    }
                    binary->_state = S_BEFORE_CONTENTS; // This forces a skip.
                }
                goto begin; // So that the user does not have to 'next' again to skip the padding.
            }
        }
        break;
    }

    // set the state forward
    binary->_state       = S_BEFORE_CONTENTS;
    binary->_value_start = value_start;
    binary->_value_type  = ion_helper_get_iontype_from_tid(ion_type_id);
    *p_value_type        = binary->_value_type;
    SUCCEED();
    
    
at_eof:
    preader->_eof = TRUE;
    binary->_value_type = tid_EOF;
    *p_value_type = tid_EOF;
    SUCCEED();

    iRETURN;
}


iERR _ion_reader_binary_step_in(ION_READER *preader)
{
    iENTER;
    ION_BINARY_READER   *binary;
    int                  tid;
    int64_t              next_start;
    BINARY_PARENT_STATE *pparent_state;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;
    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    switch(tid) {
    case TID_SEXP:
    case TID_LIST:
    case TID_STRUCT:
        break;
    case TID_CHUNKED_STRING:
    case TID_CHUNKED_SYMBOL:
    case TID_CHUNKED_CLOB:
    case TID_CHUNKED_BLOB:
        FAILWITH(IERR_NOT_IMPL);    // but I don't think these are any different than the other cases
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    // let's check to make sure this container fits in it's parent
    IONCHECK(_ion_binary_reader_fits_container(preader, binary->_value_len));  
  
    // here we can push the stack and start at the
    // beginning of the collections values

    // when we step back out we'll be just before our
    // siblings type desc byte
    next_start =  ion_stream_get_position(preader->istream);
    next_start += binary->_value_len;

    pparent_state = (BINARY_PARENT_STATE *)_ion_collection_push(&binary->_parent_stack );
    pparent_state->_next_position = next_start;
    pparent_state->_tid           = binary->_parent_tid;
    pparent_state->_local_end     = binary->_local_end;

    // now we set up for this collections contents
    binary->_local_end = next_start;
    binary->_state = S_BEFORE_TID;
    binary->_parent_tid = getTypeCode(binary->_value_tid);
    binary->_in_struct = (binary->_parent_tid == TID_STRUCT);

    iRETURN;
}

iERR _ion_reader_binary_step_out(ION_READER *preader)
{
    iENTER;
    ION_BINARY_READER   *binary;
    BINARY_PARENT_STATE *pparent_state;
    int64_t              to_skip, next_start, curr_pos;
    SIZE                 this_skip, one_skip;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    if (ION_COLLECTION_SIZE(&binary->_parent_stack) < 1) {
        // if we didn't step in, we can't step out
        FAILWITH(IERR_STACK_UNDERFLOW);
    }

    pparent_state = (BINARY_PARENT_STATE *)_ion_collection_head(&binary->_parent_stack);

    next_start          = pparent_state->_next_position;
    binary->_parent_tid = pparent_state->_tid;
    binary->_local_end  = pparent_state->_local_end;
    binary->_in_struct  = (binary->_parent_tid == TID_STRUCT);

    _ion_collection_pop_head(&binary->_parent_stack);

    curr_pos = ion_stream_get_position(preader->istream);

    if (curr_pos <= next_start) {
        // if we're at EOF then we should be spot on (curr_pos == next_start)
        ASSERT(preader->_eof ? (curr_pos == next_start) : (curr_pos <= next_start));  
        to_skip = next_start - curr_pos;
        while (to_skip > 0) {
            if (to_skip > MAX_SIZE) {
                one_skip = MAX_SIZE;
            }
            else {
                one_skip = (SIZE)to_skip;
            }
            IONCHECK(ion_stream_skip(preader->istream, one_skip, &this_skip));
            if (one_skip != this_skip) FAILWITH(IERR_UNEXPECTED_EOF);
            to_skip -= this_skip;
        }
    }
    else {
        ASSERT(ION_COLLECTION_IS_EMPTY(&binary->_parent_stack));
    }
    binary->_state = S_BEFORE_TID;
    preader->_eof = FALSE;

    iRETURN;
}

iERR _ion_reader_binary_get_depth(ION_READER *preader, SIZE *p_depth)
{
    ASSERT(preader && preader->type == ion_type_binary_reader);

    *p_depth = ION_COLLECTION_SIZE(&preader->typed_reader.binary._parent_stack);

    return IERR_OK;
}

iERR _ion_reader_binary_get_value_length(ION_READER *preader, SIZE *p_length)
{
    iENTER;
    ION_BINARY_READER *binary;
    SIZE               length;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_length);

    binary = &preader->typed_reader.binary;
    if (binary->_state == S_INVALID) FAILWITH(IERR_INVALID_STATE);

    // return -1 on eof (alternatively we could "throw" an eof error
    if (preader->_eof) {
        length = -1;
    }
    else {
        // this calculates the length of the actual value, including the TID byte and any extended length
        length  = binary->_value_len;
        length += (length < 14) ? 0 : ion_binary_len_var_uint_64(length); // length of length (when appropriate)
        length += 1; // tid byte

        if (binary->_annotation_start >= 0) {
            // if the value was annotated we need to add in the distance between
            // the start of the annotation and the start of the value, since the
            // annotation is part of the value.
            length += (SIZE)(binary->_value_start - binary->_annotation_start);
        }
    }
    *p_length = length;

    SUCCEED();
    iRETURN;
}

iERR _ion_reader_binary_get_value_offset(ION_READER *preader, POSITION *p_offset)
{
    iENTER;
    ION_BINARY_READER *binary;
    POSITION           offset;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_offset);

    binary = &preader->typed_reader.binary;
    if (binary->_state == S_INVALID) FAILWITH(IERR_INVALID_STATE);

    // return -1 on eof (alternatively we could "throw" an eof error
    if (preader->_eof) {
        offset = -1;
    }
    else {
        if (binary->_annotation_start >= 0) {
            // if the value was annotated we need to back up and include
            // the annotation, since the annotation is part of the value.
            offset = binary->_annotation_start;
        }
        else {
            offset = binary->_value_start;
        }
    }
    *p_offset = offset;
    SUCCEED();

    iRETURN;
}


iERR _ion_reader_binary_get_type(ION_READER *preader, ION_TYPE *p_value_type)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value_type != NULL);

    binary = &preader->typed_reader.binary;

    if (!(preader->_eof) && binary->_state == S_BEFORE_TID)
    {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_value_type = binary->_value_type;

    iRETURN;
}

iERR _ion_reader_binary_has_any_annotations(ION_READER *preader, BOOL *p_has_any_annotations)
{
    iENTER;
    ASSERT(preader && preader->type == ion_type_binary_reader);

    *p_has_any_annotations = (preader->typed_reader.binary._annotation_start != -1);
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_has_annotation(ION_READER *preader, ION_STRING *annotation, BOOL *p_annotation_found)
{
    iENTER;
    ION_BINARY_READER    *binary;
    BOOL                  found = FALSE;
    SID                  *psid, user_sid;
    ION_COLLECTION_CURSOR cursor;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    // we load the sid array (since this is binary)
    binary = &preader->typed_reader.binary;

    // now translate the users string into a local sid (since they gave us a string
    IONCHECK(_ion_symbol_table_find_by_name_helper(preader->_current_symtab, annotation, &user_sid, NULL, FALSE));
    if (user_sid == UNKNOWN_SID) {
        goto return_value;
    }

    // and now check the annotation list for the sid of the users string
    ION_COLLECTION_OPEN(&binary->_annotation_sids, cursor);
    for (;;) {
        ION_COLLECTION_NEXT(cursor, psid);
        if (!psid) break;
        if (*psid == user_sid) {
            found = TRUE;
            break;
        }
    }
    ION_COLLECTION_CLOSE(cursor);
    goto return_value; 

return_value:
    *p_annotation_found = found;
    SUCCEED();
    iRETURN;
}

iERR _ion_reader_binary_get_annotation_count(ION_READER *preader, int32_t *p_count)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    *p_count = ION_COLLECTION_SIZE(&binary->_annotation_sids);
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_get_an_annotation_sid(ION_READER *preader, int32_t idx, SID *p_sid)
{
    iENTER;
    ION_BINARY_READER    *binary;
    int                   ii;
    SID                  *psid = NULL;
    ION_COLLECTION_CURSOR cursor;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(idx >= 0);
    ASSERT(p_sid != NULL);

    binary = &preader->typed_reader.binary;

    if (idx >= ION_COLLECTION_SIZE(&binary->_annotation_sids)) 
    {
        FAILWITH(IERR_INVALID_ARG);
    }

    ION_COLLECTION_OPEN(&binary->_annotation_sids, cursor);
    for (ii=0; ii<=idx; ii++)
    {
        ION_COLLECTION_NEXT(cursor, psid);
        if (!psid) break;
    }
    ION_COLLECTION_CLOSE(cursor);
    if (!psid || ii != idx + 1) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_sid = *psid;

    iRETURN;
}

iERR _ion_reader_binary_string_copy_or_null(ION_READER *preader, ION_STRING *dst, ION_STRING *src)
{
    iENTER;
    if (src) {
        IONCHECK(ion_string_copy_to_owner(preader->_temp_entity_pool, dst, src));
    }
    else {
        ION_STRING_INIT(dst);
    }
    iRETURN;
}

iERR _ion_reader_binary_get_an_annotation(ION_READER *preader, int32_t idx, ION_STRING *p_str)
{
    iENTER;
    SID               sid;
    ION_STRING       *pstr;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_str != NULL);


    IONCHECK(_ion_reader_binary_get_an_annotation_sid(preader, idx, &sid));
    if (sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);

    IONCHECK(_ion_symbol_table_find_by_sid_helper(preader->_current_symtab, sid, &pstr));
    IONCHECK(_ion_reader_binary_string_copy_or_null(preader, p_str, pstr));

    iRETURN;
}

iERR _ion_reader_binary_get_annotations(ION_READER *preader, ION_STRING *p_annotations, SIZE max_count, SIZE *p_count)
{
    iENTER;
    ION_BINARY_READER    *binary;
    ION_STRING           *pstr;
    int                   ii, count;
    SID                  *psid;
    ION_COLLECTION_CURSOR cursor;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_annotations != NULL);
    ASSERT(p_count != NULL);

    binary = &preader->typed_reader.binary;

    count = ION_COLLECTION_SIZE(&binary->_annotation_sids);
    if (count > max_count) {
        FAILWITH(IERR_BUFFER_TOO_SMALL);
    }

    ION_COLLECTION_OPEN(&binary->_annotation_sids, cursor);
    for (ii=0; ;ii++) {
        ION_COLLECTION_NEXT(cursor, psid);
        if (!psid) break;
        if ((*psid) <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);

        IONCHECK(_ion_symbol_table_find_by_sid_helper(preader->_current_symtab, *psid, &pstr));
        IONCHECK(_ion_reader_binary_string_copy_or_null(preader, &p_annotations[ii], pstr));
    }

    ION_COLLECTION_CLOSE(cursor);
    goto return_value;

return_value:
    *p_count = count;
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_get_annotation_sids(ION_READER *preader, SID *p_sids, SIZE max_count, SIZE *p_count)
{
    iENTER;
    ION_BINARY_READER    *binary;
    int                   ii, count;
    SID                  *psid;
    ION_COLLECTION_CURSOR cursor;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    count = ION_COLLECTION_SIZE(&binary->_annotation_sids);
    if (count > max_count) {
        FAILWITH(IERR_BUFFER_TOO_SMALL);
    }

    ION_COLLECTION_OPEN(&binary->_annotation_sids, cursor);
    for (ii=0; ;ii++) {
        ION_COLLECTION_NEXT(cursor, psid);
        if (!psid) break;
        p_sids[ii++] = *psid;
    }

    ION_COLLECTION_CLOSE(cursor);
    goto return_value;

return_value:
    *p_count = count;
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_get_field_name(ION_READER *preader, ION_STRING **p_pstr)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    if (binary->_in_struct) {
        if (binary->_value_field_id <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);
        if (preader->_current_symtab == NULL)       FAILWITH(IERR_INVALID_STATE);

        IONCHECK(_ion_symbol_table_find_by_sid_helper(preader->_current_symtab, binary->_value_field_id, p_pstr));
    }
    else {
        *p_pstr = NULL;
        SUCCEED();
    }

    iRETURN;
}

iERR _ion_reader_binary_get_field_sid(ION_READER *preader, SID *p_sid)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    if (binary->_value_field_id <= UNKNOWN_SID && binary->_in_struct) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_sid = binary->_value_field_id;

    iRETURN;
}

iERR _ion_reader_binary_is_null(ION_READER *preader, BOOL *p_is_null)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid;
    BOOL               is_null;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid == TID_NULL) {
        is_null = TRUE;
    }
    else {
        is_null = (getLowNibble(binary->_value_tid) == ION_lnIsNull);
    }

    *p_is_null = is_null;

    iRETURN;
}

// this checks for the error condition of a value whose length
// extends beyond it container. This is only checked when we have
// a value in a container (struct, list, sexp). Typically this
// occurs with invalid binary, or a seek to wrong position.
iERR _ion_binary_reader_fits_container(ION_READER *preader, SIZE len)
{
    iENTER;
    ION_BINARY_READER *binary;
    POSITION           pos;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    
    binary = &preader->typed_reader.binary;
    if (binary->_local_end >= 0) {
        // we only care when the local end has been set (which is
        // set when we step into a container, struct, list, or sexp)
        pos = ion_stream_get_position(preader->istream);
        if (pos + len > binary->_local_end) {
            FAILWITH(IERR_UNEXPECTED_EOF);
        }
    }
    SUCCEED();

    iRETURN;
}


iERR _ion_reader_binary_read_null(ION_READER *preader, ION_TYPE *p_value)
{
    iENTER;
    ION_BINARY_READER *binary;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    *p_value = binary->_value_type;

    if (getLowNibble(binary->_value_tid) != ION_lnIsNull) {
        FAILWITH(IERR_INVALID_TOKEN);
    }
    // TODO: this setting of the state may be unnecessary 
    //       since the null is totaly contained in the type
    //       desc byte - can it hurt?
    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    iRETURN;
}

iERR _ion_reader_binary_read_bool(ION_READER *preader, BOOL *p_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    int tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_BOOL) {
        FAILWITH(IERR_INVALID_STATE);
    }

    switch (getLowNibble(binary->_value_tid)) {
    case ION_lnBooleanFalse:
        binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value
        *p_value = FALSE;
        break;
    case ION_lnBooleanTrue:
        binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value
        *p_value = TRUE;
        break;
    case ION_lnIsNull:
        FAILWITH(IERR_NULL_VALUE);
    default:
        FAILWITH(IERR_INVALID_STATE);
    }
    iRETURN;
}

iERR _ion_reader_binary_read_int32(ION_READER *preader, int32_t *p_value)
{
    iENTER;
    int64_t int64;

    ASSERT(p_value != NULL);

    IONCHECK(_ion_reader_binary_read_int64(preader, &int64));
    *p_value = (int32_t)int64;
    if (*p_value != int64) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    iRETURN;
}

iERR _ion_reader_binary_read_int64(ION_READER *preader, int64_t *p_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid, len;
    uint64_t           unsignedInt64 = 0;
    BOOL               is_negative;
    BOOL               is_null;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_NEG_INT && tid != TID_POS_INT) {
        FAILWITH(IERR_INVALID_STATE);
    }
    if (tid == TID_NULL) {
        is_null = TRUE;
    }
    else {
        is_null = (getLowNibble(binary->_value_tid) == ION_lnIsNull);
    }
    if(is_null) {
        FAILWITH(IERR_NULL_VALUE);
    }
    len = binary->_value_len;
    if (len > sizeof(int64_t)) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }

    IONCHECK(_ion_binary_reader_fits_container(preader, len));

    IONCHECK(ion_binary_read_uint_64(preader->istream, len, &unsignedInt64));

    is_negative = (tid == TID_NEG_INT)? TRUE: FALSE;
    IONCHECK(cast_to_int64(unsignedInt64, is_negative, p_value));
    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    iRETURN;
}


iERR _ion_reader_binary_read_ion_int(ION_READER *preader, ION_INT *p_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid, len;
    BOOL               is_negative;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_NEG_INT && tid != TID_POS_INT) {
        FAILWITH(IERR_INVALID_STATE);
    }

    len = binary->_value_len;
    // old, from pre ion_int days
    // if (len > sizeof(int64_t)) {
    //     FAILWITH(IERR_NUMERIC_OVERFLOW);
    // }

    IONCHECK(_ion_binary_reader_fits_container(preader, len));

	is_negative = (tid == TID_NEG_INT)? TRUE: FALSE;
    IONCHECK(ion_binary_read_ion_int(preader->istream, len, is_negative, p_value));

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    iRETURN;
}


iERR _ion_binary_read_mixed_int_helper(ION_READER *preader)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid, len, bits;
    uint64_t           unsignedInt64;
    ION_INT           *iint;
    BOOL               is_negative;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_NEG_INT && tid != TID_POS_INT) {
        FAILWITH(IERR_INVALID_STATE);
    }
    is_negative = (tid == TID_NEG_INT)? TRUE: FALSE;
    len = binary->_value_len;
    IONCHECK(_ion_binary_reader_fits_container(preader, len));

    bits = len * II_BITS_PER_BYTE; // hex digits would be the larger than decimal

    if (bits <= II_INT64_BIT_THRESHOLD) {
        preader->_int_helper._is_ion_int = FALSE;
        IONCHECK(ion_binary_read_uint_64(preader->istream, len, &unsignedInt64));
        IONCHECK(cast_to_int64(unsignedInt64, is_negative, &(preader->_int_helper._as_int64)));
    }
    else {
        preader->_int_helper._is_ion_int = TRUE;
        iint = &(preader->_int_helper._as_ion_int);
        if (!iint->_owner) {
            IONCHECK(ion_int_init(iint, preader));
        }
        IONCHECK(ion_binary_read_ion_int(preader->istream, len, is_negative, iint));
    }

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    iRETURN;
}

iERR _ion_reader_binary_read_double(ION_READER *preader, double *p_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    double             value;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_FLOAT) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        FAILWITH(IERR_NULL_VALUE);
    }

    IONCHECK(_ion_binary_reader_fits_container(preader, binary->_value_len));

    IONCHECK(ion_binary_read_double(preader->istream, binary->_value_len, &value));

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value
    *p_value = value;

    iRETURN;
}

iERR _ion_reader_binary_read_decimal(ION_READER *preader, decQuad *p_quad, decNumber **p_num)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_quad != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_DECIMAL) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        FAILWITH(IERR_NULL_VALUE);
    }
    
    IONCHECK(_ion_binary_reader_fits_container(preader, binary->_value_len));

    IONCHECK(ion_binary_read_decimal(preader->istream, binary->_value_len, &preader->_deccontext, p_quad, p_num));

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    iRETURN;
}

iERR _ion_reader_binary_read_timestamp(ION_READER *preader, iTIMESTAMP p_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    ION_TIMESTAMP      ti;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_TIMESTAMP) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        FAILWITH(IERR_NULL_VALUE);
    }

    IONCHECK(_ion_binary_reader_fits_container(preader, binary->_value_len));

    IONCHECK(ion_binary_read_timestamp(preader->istream, binary->_value_len, &preader->_deccontext, &ti));

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    memcpy(p_value, &ti, sizeof(ION_TIMESTAMP));

    iRETURN;
}

iERR _ion_reader_binary_read_symbol_sid_helper(ION_READER *preader, ION_BINARY_READER *binary, SID *p_value)
{
    iENTER;
    uint32_t           value;    // for the time being the SID is limited to 32 bits

    IONCHECK(_ion_binary_reader_fits_container(preader, binary->_value_len));

    if (binary->_value_len > sizeof(int32_t)) {
        FAILWITH(IERR_NUMERIC_OVERFLOW);
    }
    IONCHECK(ion_binary_read_uint_32(preader->istream, binary->_value_len, &value));

    *p_value = (SID)value;
    iRETURN;
}

iERR _ion_reader_binary_read_symbol_sid(ION_READER *preader, SID *p_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_value != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_SYMBOL) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        FAILWITH(IERR_NULL_VALUE);
    }

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value
    *p_value = binary->_value_symbol_id;

    iRETURN;
}

iERR _ion_reader_binary_read_symbol(ION_READER *preader, ION_SYMBOL *p_symbol)
{
    iENTER;
    SID sid;
    ION_STRING *text;

    ASSERT(p_symbol);
    IONCHECK(_ion_reader_binary_read_symbol_sid(preader, &sid));
    if (sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);
    IONCHECK(_ion_symbol_table_find_by_sid_helper(preader->_current_symtab, sid, &text));
    if (ION_STRING_IS_NULL(text)) {
        ION_STRING_INIT(&p_symbol->value);
    }
    else {
        ION_STRING_ASSIGN(&p_symbol->value, text);
    }
    p_symbol->sid = sid;
    p_symbol->psymtab = preader->_current_symtab;
    p_symbol->add_count = 1;
    iRETURN;
}

iERR _ion_reader_binary_get_string_length(ION_READER *preader, SIZE *p_length)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_length != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_STRING) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        *p_length = 0;
    }
	else {
		*p_length = binary->_value_len;
	}
	
    iRETURN;
}

// This reads the entire contents of the string (or string of a symbol id)
// in a single go, returning the value in an ION_STRING
iERR _ion_reader_binary_read_string(ION_READER *preader, ION_STRING *p_str)
{
    iENTER;
    ION_BINARY_READER *binary;
    ION_STRING        *pstr;
    int                tid;
    SIZE               str_len, bytes_read;
    SID                sid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_str != NULL);

    binary = &preader->typed_reader.binary;
    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_STRING && tid != TID_SYMBOL) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        FAILWITH(IERR_NULL_VALUE);
    }
    else {
        if (tid == TID_STRING) {
			str_len = binary->_value_len;
            if (p_str->length < str_len || !p_str->value) {
				p_str->value = ion_alloc_with_owner(preader->_temp_entity_pool, str_len);
				if (!p_str->value) FAILWITH(IERR_NO_MEMORY);
            }
			IONCHECK(_ion_reader_binary_read_string_bytes(preader, FALSE, p_str->value, str_len, &bytes_read));
            if (bytes_read != str_len) FAILWITH(IERR_UNEXPECTED_EOF);
			p_str->length = str_len;
            if (ION_STRING_IS_NULL(p_str)) {
                // Attempting to read a null value with this API is an error. Note that when the underlying value is a
                // symbol, a null ION_STRING may be returned to represent a symbol with unknown text.
                FAILWITH(IERR_NULL_VALUE);
            }
        }
        else if (tid == TID_SYMBOL) {
            IONCHECK(_ion_reader_binary_read_symbol_sid(preader, &sid));
            if (sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_SYMBOL);
            IONCHECK(_ion_symbol_table_find_by_sid_helper(preader->_current_symtab, sid, &pstr));
            IONCHECK(_ion_reader_binary_string_copy_or_null(preader, p_str, pstr));
        }
        else {
            FAILWITH(IERR_INVALID_STATE);
        }
    }

    binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value

    iRETURN;
}

// reads up to buf_max bytes from a string (not a symbol) this fails if the flag
// accept_partial is *not* true and there isn't enough room in the passed in buffer.
// if accept_partial is true it reads enough  bytes to fill the buffer or all 
// bytes remaining in the string if the buffer is large enough
iERR _ion_reader_binary_read_string_bytes(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length)
{
    iENTER;
    ION_BINARY_READER *binary;
    SIZE               length, read_len;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_buf != NULL);
    ASSERT(buf_max >= 0);
    ASSERT(p_length != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_STRING) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        read_len = 0;
    }
	else {
		length = binary->_value_len;
		if (buf_max < length) {
			if (accept_partial) {
				length = buf_max;
			}
			else {
				FAILWITH(IERR_BUFFER_TOO_SMALL);
			}
		}
		IONCHECK(_ion_binary_reader_fits_container(preader, length));

		IONCHECK(ion_stream_read(preader->istream, p_buf, length, &read_len));
		if (preader->options.skip_character_validation == FALSE) {
			IONCHECK(_ion_reader_binary_validate_utf8(p_buf, read_len, preader->_expected_remaining_utf8_bytes, &preader->_expected_remaining_utf8_bytes));
		}
		if (length < binary->_value_len) {
			binary->_value_len -= length;
		}
		else {
			binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value
		}
	}
    *p_length = read_len;

    iRETURN;
}

// throws error if the buffer (buf) contains an invalid utf8 sequence
// (I hate to do this, but it's for validation)
iERR _ion_reader_binary_validate_utf8(BYTE *buf, SIZE len, SIZE expected_remaining, SIZE *p_expected_remaining)
{
    iENTER;
    uint32_t c;
	
	// check for any expected "bytes following header" we didn't get around to reading in the last partial read
	while (expected_remaining > 0) {
		expected_remaining--;
		if (len < 1) goto end_of_len;
		len--;
		c = (int)*buf++;
		if (!ION_is_utf8_trailing_char_header(c)) goto bad_utf8;
    }
	
    while (len--) {
        c = (int)*buf++;
        switch (ION_UTF8_HEADER_BITS(c)) {
        case 31: // is invalid
            // fall through to bad_utf8;

        // 10xxx xxx == 16-23   ION_is_utf8_trailing_char_header(c)
        case 16: case 17: case 18: case 19:
        case 20: case 21: case 22: case 23: 
            goto bad_utf8;

        // 11110 xxx  5 bits = 30    ION_is_utf8_4byte_header(c)
        case 30: 
            if (len < 1) goto end_of_len_3;
            len--;
            c = (int)*buf++;
            if (!ION_is_utf8_trailing_char_header(c)) goto bad_utf8;
            // fall through to read next byte

        // 1110x xxx == 28-29  ION_is_utf8_3byte_header(c)
        case 28: case 29:
            if (len < 1) goto end_of_len_2;
            len--;
            c = (int)*buf++;
            if (!ION_is_utf8_trailing_char_header(c)) goto bad_utf8;
            // fall through to read next byte

        // 110xx xxx == 24-27  ION_is_utf8_2byte_header(c)
        case 24:
        case 25: case 26: case 27: 
            if (len < 1) goto end_of_len_1;
            len--;
            c = (int)*buf++;
            if (!ION_is_utf8_trailing_char_header(c)) goto bad_utf8;
            // fall through to read next byte

        // 0xxxx xxx == 0-15  ION_is_utf8_1byte_header(c)
        case 0: case 1: case 2: case 3: case 4:
        case 5: case 6: case 7: case 8: case 9:
        case 10: case 11: case 12: case 13: case 14:
        case 15: 
            // done - it's a keeper
            break;        
        }
    }
    goto end_of_len;

// expected_remaining is 0 when we're processing bytes
// here we count up how many are left to read so we can
// tell our caller (and they'll pass this in on the next call)
end_of_len_3:
	expected_remaining++;
	// fall through
end_of_len_2:
	expected_remaining++;
	// fall through
end_of_len_1:
	expected_remaining++;
	// fall through
end_of_len:
	*p_expected_remaining = expected_remaining;
	SUCCEED();

bad_utf8:
    FAILWITH(IERR_INVALID_UTF8);

    iRETURN;
}

iERR _ion_reader_binary_get_lob_size(ION_READER *preader, SIZE *p_length)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_length != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_state != S_BEFORE_CONTENTS) {
        FAILWITH(IERR_INVALID_STATE);
    }

    tid = getTypeCode(binary->_value_tid);
    if (tid != TID_BLOB && tid != TID_CLOB) {
        FAILWITH(IERR_INVALID_STATE);
    }

    if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
        FAILWITH(IERR_NULL_VALUE);
    }

    *p_length = binary->_value_len;

    iRETURN;
}
iERR _ion_reader_binary_read_lob_bytes(ION_READER *preader, BOOL accept_partial, BYTE *p_buf, SIZE buf_max, SIZE *p_length)
{
    iENTER;
    ION_BINARY_READER *binary;
    SIZE               length, read_len = 0;
    int                tid;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_buf != NULL);
    ASSERT(buf_max >= 0);
    ASSERT(p_length != NULL);

    binary = &preader->typed_reader.binary;

    if (binary->_value_len > 0) {

        if (binary->_state != S_BEFORE_CONTENTS) {
            FAILWITH(IERR_INVALID_STATE);
        }

        tid = getTypeCode(binary->_value_tid);
        if (tid != TID_BLOB && tid != TID_CLOB) {
            FAILWITH(IERR_INVALID_STATE);
        }

        if (getLowNibble(binary->_value_tid) == ION_lnIsNull) {
            FAILWITH(IERR_NULL_VALUE);
        }
	    length = binary->_value_len;
        if (buf_max < length) {
		    if (accept_partial) {
			    length = buf_max;
		    }
		    else {
                FAILWITH(IERR_BUFFER_TOO_SMALL);
            }
        }

        IONCHECK(_ion_binary_reader_fits_container(preader, length));

        IONCHECK(ion_stream_read(preader->istream, p_buf, length, &read_len));

        if (!accept_partial && (read_len < length)) {
            FAILWITH(IERR_UNEXPECTED_EOF);
        }

        // whatever else happens we can't read these bytes any longer
	    binary->_value_len -= length;
        if (binary->_value_len < 1) {
            binary->_state = S_BEFORE_TID; // now we (should be) just in front of the next value
	    }
    }
    *p_length = read_len;

    iRETURN;
}

// these are local routines that shouldn't need to be called
// from anywhere else - they are declared at the top of this file

iERR _ion_reader_binary_local_read_length(ION_READER *preader, int tid, int *p_length) 
{
    iENTER;
    uint32_t len;

    ASSERT(preader && preader->type == ion_type_binary_reader);

    switch (getTypeCode(tid)) {
    case TID_BOOL:   // 1
        len = 0;
        break;
    case TID_STRUCT:    // 13 D
        if (getLowNibble(tid) == 1) 
        {
            IONCHECK(ion_binary_read_var_uint_32(preader->istream, &len));
            if (len < 1) {
                FAILWITHMSG(IERR_INVALID_BINARY, "Sorted structs must have at least one field.");
            }
            break;
        }
        // fall through to the normal case of ln or varlen
    case TID_NULL: // Either null.null or NOP padding.
    case TID_POS_INT:
    case TID_NEG_INT:
    case TID_FLOAT:
    case TID_DECIMAL:
    case TID_TIMESTAMP:
    case TID_SYMBOL:
    case TID_STRING:
    case TID_CLOB:
    case TID_BLOB:
    case TID_LIST:
    case TID_SEXP:
    case TID_UTA:
        len = getLowNibble(tid);
        if (len == 14) {
            IONCHECK(ion_binary_read_var_uint_32(preader->istream, &len));
        }
        else if (len == 15) {
            len = 0;
        }
        break;
    default:
        FAILWITHMSG(IERR_INVALID_STATE, "unrecognized type encountered");
    }

    *p_length = (int)len;
    SUCCEED();

    iRETURN;
}

iERR _ion_reader_binary_local_process_possible_magic_cookie(ION_READER *preader, int td, BOOL *p_is_system_value)
{
    iENTER;
    ION_BINARY_READER *binary;
    int                b1, b2, b3;
    BOOL               is_system_value = FALSE;


    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(td == ION_VERSION_MARKER[0]);
    ASSERT(p_is_system_value);

    binary = &preader->typed_reader.binary;

    ION_GET(preader->istream, b1);
    if (b1 != ION_VERSION_MARKER[1]) {
        IONCHECK(ion_stream_unread_byte(preader->istream, b1));
        goto return_value;
    }

    ION_GET(preader->istream, b2);
    if (b2 != ION_VERSION_MARKER[2]) {
        IONCHECK(ion_stream_unread_byte(preader->istream, b2));
        IONCHECK(ion_stream_unread_byte(preader->istream, b1));
        goto return_value;
    }

    ION_GET(preader->istream, b3);
    if (b3 != ION_VERSION_MARKER[3]) {
        IONCHECK(ion_stream_unread_byte(preader->istream, b3));
        IONCHECK(ion_stream_unread_byte(preader->istream, b2));
        IONCHECK(ion_stream_unread_byte(preader->istream, b1));
        goto return_value;
    }

    // there's magic here!  start over with
    // a fresh new symbol table!
    IONCHECK(_ion_reader_reset_local_symbol_table(preader));
    binary->_state = S_BEFORE_TID;
    is_system_value = TRUE;

return_value:
    *p_is_system_value = is_system_value;
    iRETURN;
}

iERR _ion_reader_binary_local_process_possible_symbol_table(ION_READER *preader, BOOL *p_is_system_value)
{
    iENTER;
    BOOL     is_symbol_table = FALSE;
    ION_SYMBOL_TABLE *system;

    ASSERT(preader && preader->type == ion_type_binary_reader);
    ASSERT(p_is_system_value);

    preader->typed_reader.binary._state = S_BEFORE_CONTENTS;

    // TODO just check the first annotation, in accordance with the spec.
    IONCHECK(_ion_reader_has_annotation_helper(preader, &ION_SYMBOL_SYMBOL_TABLE_STRING, &is_symbol_table));
    if (is_symbol_table) {
        IONCHECK(_ion_symbol_table_get_system_symbol_helper(&system, ION_SYSTEM_VERSION));
        IONCHECK(_ion_symbol_table_load_helper(preader, preader->_catalog->owner, system, &preader->_current_symtab));
    }

    *p_is_system_value = is_symbol_table;

    iRETURN;
}
