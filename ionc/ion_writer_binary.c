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
//  general write algorithm is:
//  
//      offset = 0
//      if known length
//          write value into value_list buffers at offset
//          set length written
//      else if start collection
//          put patch on list and list node on stack
//          written = 0;
//      else if end collection
//          written = length of top of stack
//          pop stack
//          update node on top of stack with length written
//          written = 0
//      else
//          put patch on list and list node on stack
//          write portion that's known
//          set length written
//      }
//      update node on top of stack with length written
//      offset += written
//      if stack is empty
//          write values and patches
//      }
//  }
//  
//  write value and patches {
//      offset = 0
//      next_patch = head of patch list
//      next_value_buffer = head of value list
//      pos = 0
//      do {
//          while (offset < next_patch->offset) {
//              len = next_value_buffer.length - pos
//              if (len > next_patch->offset - offset) {
//                  len  = next_patch->offset - offset
//              }
//              write len from value [ pos to len ]
//              pos += len
//              if (pos >= next_value_buffer.length) {
//                  next_value_buffer = next of value list
//                  po = 0
//              }       
//              offset += len
//          }
//          write header
//          next_patch = next of patch list
//      }
//      while (next patch || next_value_buffer) 
//  

#include <decNumber.h>
#include "ion_internal.h"
#include "ion_writer_impl.h"


#define LOCAL_STACK_BUFFER_SIZE 256

iERR _ion_writer_binary_initialize(ION_WRITER *pwriter) 
{
    iENTER;

    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;

    pwriter->_in_struct              = FALSE;
    pwriter->_has_local_symbols      = FALSE;
    bwriter->_version_marker_written = FALSE;
    bwriter->_lob_in_progress        = tid_none;

    _ion_collection_initialize(pwriter, &bwriter->_patch_stack, sizeof(ION_BINARY_PATCH *));
    _ion_collection_initialize(pwriter, &bwriter->_patch_list,  sizeof(ION_BINARY_PATCH));
    _ion_collection_initialize(pwriter, &bwriter->_value_list, pwriter->options.allocation_page_size);

    // the _value_stream is the temporary output stream where we write the un-headered
    // values that will later be merged with the length prefixes in the users output
    // stream
    //IONCHECK( ion_output_stream_initialize_with_handler( bwriter->_value_stream, 
    //    _ion_writer_binary_output_stream_handler, pwriter ));

    IONCHECK(ion_stream_open_memory_only( &bwriter->_value_stream ));

    iRETURN;
}


iERR _ion_writer_binary_push_position(ION_WRITER *pwriter, int type_id)
{
    iENTER;
    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;
    ION_BINARY_PATCH  *patch, **ppatch;

    // first we create a new patch at the end of the patch list
    patch = (ION_BINARY_PATCH *)_ion_collection_append(&bwriter->_patch_list);
    patch->_length = 0;
    patch->_offset = (int)ion_stream_get_position(bwriter->_value_stream);   // TODO - this needs 64bit care
    patch->_type   = type_id;
    
    // then we push a pointer to the patch onto our active stack
    ppatch = (ION_BINARY_PATCH **)_ion_collection_push(&bwriter->_patch_stack);
    *ppatch = patch;
    SUCCEED();

    iRETURN;
}

iERR _ion_writer_binary_pop(ION_WRITER *pwriter) 
{
    iENTER;
    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;
    ION_BINARY_PATCH **ppatch;
    int patch_down;

    // pop the top of the patch stack.  We need to patch the length
    // of the length onto the remainer of the stack.  So we do that
    // after we pop it off the stack, if there's anything to patch.

    // we need to pass the added bytes that were patched onto
    // this value down to the next one on the stack

    ppatch = (ION_BINARY_PATCH **)_ion_collection_head( &bwriter->_patch_stack);

    patch_down = (*ppatch)->_length;
    if (patch_down >= ION_lnIsVarLen) {
        patch_down += ion_binary_len_var_uint_64( patch_down );
    }

    _ion_collection_pop_head( &bwriter->_patch_stack);

    if (patch_down > 0) {
        IONCHECK( _ion_writer_binary_patch_lengths( pwriter, patch_down ));
    }

    
    // CAS, was (dec 13,2009): _ion_collection_pop_head( &bwriter->_patch_stack);

    iRETURN;
}

iERR _ion_writer_binary_patch_lengths(ION_WRITER *pwriter, int added_length)
{
    iENTER;
    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;
    ION_BINARY_PATCH **ppatch;
//    ION_COLLECTION_CURSOR patch_cursor;

    ppatch = _ion_collection_head(&bwriter->_patch_stack);
    if (ppatch) {
        // we only patch the top of the stack right now
        // when we pop this off we'll patch the entries
        // below (by updating the next one and letting
        // it pass the length on)
        (*ppatch)->_length += added_length;
    }

#ifdef THE_OLD_VERSION
    if (ION_COLLECTION_IS_EMPTY(&bwriter->_patch_stack)) SUCCEED();

    ION_COLLECTION_OPEN(&bwriter->_patch_stack, patch_cursor);
    for (;;) {
        ION_COLLECTION_NEXT(patch_cursor, ppatch);
        if (ppatch == NULL) break;
        (*ppatch)->_length += added_length;
    }
    ION_COLLECTION_CLOSE( patch_cursor );
#endif

    SUCCEED();

    iRETURN;
}

iERR _ion_writer_binary_top_length(ION_WRITER *pwriter, int *plength) 
{
    iENTER;
    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;
    ION_BINARY_PATCH **ptop;
    ptop = (ION_BINARY_PATCH **)_ion_collection_head( &bwriter->_patch_stack);
    *plength = (*ptop)->_length;
    SUCCEED();
    iRETURN;
}

iERR _ion_writer_binary_top_position(ION_WRITER *pwriter, int *poffset) 
{
    iENTER;
    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;
    ION_BINARY_PATCH **ptop;
    ptop = (ION_BINARY_PATCH **)_ion_collection_head( &bwriter->_patch_stack);
    *poffset = (*ptop)->_offset;
    SUCCEED();
    iRETURN;
}

iERR _ion_writer_binary_start_value(ION_WRITER *pwriter, int value_length)
{
    iENTER;
    ION_BINARY_WRITER  *bwriter = &pwriter->_typed_writer.binary;
    ION_STREAM         *ostream = bwriter->_value_stream;
    int                 start, finish, patch_len;
    int                 sid_count, ii, annotations_len = 0;
    int                 annotation_len_o_len, total_ann_value_len;
    SID                 sid;

    // because we support start_lob, append_lob, finish_lob we have
    // to make sure someone doesn't start something if they haven't
    // finished with the lob they have in progress.
    if (bwriter->_lob_in_progress != tid_none) {
        FAILWITH(IERR_INVALID_STATE);
    }

    // remember where we start (so later we can look at where we
    // ended up in the output stream and calc the bytes written
    start = (int)ion_stream_get_position(ostream);  // TODO - this needs 64bit care
        
    // write field name
    if (pwriter->_in_struct) {
        IONCHECK( _ion_writer_get_field_name_as_sid_helper(pwriter, &sid));
        if (sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_STATE);
        IONCHECK( ion_binary_write_var_uint_64( ostream, sid ));
        IONCHECK( _ion_writer_clear_field_name_helper(pwriter));
    }

    // write annotations
    sid_count= pwriter->annotation_curr; // we could call _ion_writer_get_annotation_count but it seems like overkill
    if (sid_count > 0) {

        // FIRST add up the length of the annotation symbols as they'll appear in the buffer
        for (ii=0; ii<sid_count; ii++) {
            IONCHECK(_ion_writer_get_annotation_as_sid_helper(pwriter, ii, &sid));
            if (sid <= UNKNOWN_SID) FAILWITH(IERR_INVALID_STATE);
            annotations_len += ion_binary_len_var_uint_64( sid );
        }

        // THEN write the td byte, optional annotations, this is before the caller
        //      writes out the actual values (plain value) td byte and varlen
        //      an annotation is just like any parent collection header in that it needs
        //      to be in our stack for length patching purposes
        if (value_length == ION_BINARY_UNKNOWN_LENGTH) {
            // if we don't know the value length we push a patch point 
            // onto the backpatch stack - but first we patch our parent 
            // with the fieldid len and the annotation type desc byte
            finish = (int)ion_stream_get_position(ostream);  // TODO - this needs 64bit care
            patch_len = finish - start + ION_BINARY_TYPE_DESC_LENGTH;  // for the uta type desc byte we'll write in when we fill this out on outpu
            IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len ));
            start = finish; // we reset the patch lengths as we've accounted for the writting up until this point
            IONCHECK(_ion_writer_binary_push_position( pwriter, TID_UTA ));
        }
        else {
            // if we know the value length we can write the
            // annotation header out in full (and avoid the back patching)
            // <ann,ln><totallen><annlen><ann><valuetd,ln><valuelen><value>

            annotation_len_o_len = ion_binary_len_var_uint_64(annotations_len); // len(<annlen>)
            total_ann_value_len = annotation_len_o_len + annotations_len + value_length;
            if (total_ann_value_len < ION_lnIsVarLen) {
                ION_PUT(ostream, makeTypeDescriptor(TID_UTA, total_ann_value_len));
            }
            else {
                ION_PUT(ostream, makeTypeDescriptor(TID_UTA, ION_lnIsVarLen));
                // add the len of len to the patch total (since we just learned about it)
                IONCHECK( ion_binary_write_var_uint_64(ostream, total_ann_value_len));
            }
        }
            
        IONCHECK( ion_binary_write_var_uint_64(ostream, annotations_len));
        for (ii=0; ii<sid_count; ii++) {
            // note that len already has the sum of the actual lengths
            // added into it so that we could write it out in front
            IONCHECK(_ion_writer_get_annotation_as_sid_helper(pwriter, ii, &sid));
            IONCHECK( ion_binary_write_var_uint_64(ostream, sid));
        }
        // we patch any wrapper the annotation is in with whatever we wrote here
        IONCHECK( _ion_writer_clear_annotations_helper( pwriter ));
    }

    // now see how much was actually written to the output stream and, 
    // if anything was, we need to update the patch amount by that amount
    finish = (int)ion_stream_get_position(ostream);  // TODO - this needs 64bit care
    patch_len = finish - start;
    if (patch_len > 0) {
        IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len ));
    }

    iRETURN;
}

iERR _ion_writer_binary_close_value(ION_WRITER *pwriter) 
{
    iENTER;
    ION_BINARY_WRITER  *bwriter = &pwriter->_typed_writer.binary;
    ION_BINARY_PATCH  **ppatch;

    if (!ION_COLLECTION_IS_EMPTY(&bwriter->_patch_stack)) {
        // check for annotations, which we need to pop off now
        // since once we close a value out, we won't need to patch
        // the len of the annotation type desc it (might have) had
        ppatch = (ION_BINARY_PATCH **)_ion_collection_head(&bwriter->_patch_stack);
        if ((*ppatch)->_type == TID_UTA) {
            IONCHECK( _ion_writer_binary_pop(pwriter) );
        }
    }
    iRETURN;
}

iERR _ion_writer_binary_start_lob(ION_WRITER *pwriter, ION_TYPE lob_type)
{
    iENTER;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    switch ((intptr_t)lob_type) {
    case (intptr_t)tid_BLOB:
    case (intptr_t)tid_CLOB:  
        break;
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_UNKNOWN_LENGTH ));
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH ));
    IONCHECK( _ion_writer_binary_push_position( pwriter, ion_helper_get_tid_from_ion_type(lob_type) ));

    // we have to set the *after* we start the value so
    // start value doesn't get confused about two at once
    pwriter->_typed_writer.binary._lob_in_progress = lob_type;

    iRETURN;
}

iERR _ion_writer_binary_append_lob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    ION_TYPE lob_type;
    SIZE     written;

    if (!pwriter)   FAILWITH(IERR_BAD_HANDLE);
    if (!p_buf)     FAILWITH(IERR_INVALID_ARG);
    if (length < 1) FAILWITH(IERR_INVALID_ARG);
   
    lob_type = pwriter->_typed_writer.binary._lob_in_progress;
    switch((intptr_t)lob_type) {
    case (intptr_t)tid_BLOB:
    case (intptr_t)tid_CLOB:
        // for binary these are the same - just write out the data
        IONCHECK( ion_stream_write( pwriter->_typed_writer.binary._value_stream, p_buf, length, &written ));
        if (written != length) FAILWITH(IERR_WRITE_ERROR);
        IONCHECK( _ion_writer_binary_patch_lengths( pwriter, length ));
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

iERR _ion_writer_binary_finish_lob(ION_WRITER *pwriter)
{
    iENTER;
    ION_TYPE lob_type;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    lob_type = pwriter->_typed_writer.binary._lob_in_progress;
    switch ((intptr_t)lob_type) {
    case (intptr_t)tid_CLOB:
    case (intptr_t)tid_BLOB:
        // just checking to make sure we're really working on a lob of some sort
        break;    
    default:
        // and if not ... that's a bad thing
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK( _ion_writer_binary_pop( pwriter ));
    IONCHECK( _ion_writer_binary_close_value( pwriter ));

	// since we've closed out the lob we need to tell our
	// binary (and only the binary writer cares about this)
	// writer we've finished the lob.
	pwriter->_typed_writer.binary._lob_in_progress = tid_none;

    iRETURN;
}

iERR _ion_writer_binary_start_container(ION_WRITER *pwriter, ION_TYPE container_type)
{
    iENTER;

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_UNKNOWN_LENGTH ));
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH ));
    IONCHECK( _ion_writer_binary_push_position( pwriter, ion_helper_get_tid_from_ion_type(container_type) ));
    pwriter->_in_struct = (container_type == tid_STRUCT);

    iRETURN;
}

iERR _ion_writer_binary_finish_container(ION_WRITER *pwriter)
{
    iENTER;
    ION_COLLECTION    *stack = &pwriter->_typed_writer.binary._patch_stack;
    ION_BINARY_PATCH **ptop;
    BOOL               in_struct;

    IONCHECK( _ion_writer_binary_pop( pwriter ));
    IONCHECK( _ion_writer_binary_close_value( pwriter ));

    if (ION_COLLECTION_IS_EMPTY(stack)) {
        in_struct = FALSE;
        if (pwriter->options.flush_every_value) {
            IONCHECK(_ion_writer_binary_flush_to_output(pwriter));
        }
    }
    else {
        ptop = (ION_BINARY_PATCH **)_ion_collection_head( stack );
        in_struct = ((*ptop)->_type == TID_STRUCT);
    }
    pwriter->_in_struct = in_struct;

    iRETURN;
}

iERR _ion_writer_binary_write_null(ION_WRITER *pwriter)
{
    iENTER;

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_TYPE_DESC_LENGTH ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(TID_NULL, ION_lnIsNull ));
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH ));

    iRETURN;
}


iERR _ion_writer_binary_write_typed_null(ION_WRITER *pwriter, ION_TYPE type)
{
    iENTER;
    int tid = ion_helper_get_tid_from_ion_type(type);

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_TYPE_DESC_LENGTH ) );
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(tid, ION_lnIsNull ) );
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH ) );

    iRETURN;
}

iERR _ion_writer_binary_write_bool(ION_WRITER *pwriter, BOOL value)
{
    iENTER;
    int td_byte = value ? IonTrue : IonFalse;

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_TYPE_DESC_LENGTH ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, td_byte );
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH ));

    iRETURN;
}

iERR _ion_writer_binary_write_int64(ION_WRITER *pwriter, int64_t value)
{
    iENTER;
    int len = 0, td = 0;
    uint64_t unsignedValue = 0;

    if (value < 0) {
        td =  TID_NEG_INT;
    }
    else {
        td = TID_POS_INT;
    }
    unsignedValue = abs_int64(value);
    len = ion_binary_len_uint_64(unsignedValue);

    assert(len < ION_lnIsVarLen);

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_TYPE_DESC_LENGTH + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(td, len));
    if (len > 0) {
        ion_binary_write_uint_64(pwriter->_typed_writer.binary._value_stream, unsignedValue);
    }
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH + len ));

    iRETURN;
}


iERR _ion_writer_binary_write_ion_int(ION_WRITER *pwriter, ION_INT *iint)
{
    iENTER;
    int  offset, ln, patch_len, len = 0, td = 0;
    SIZE bytes_written, written;
    BYTE buffer[LOCAL_STACK_BUFFER_SIZE];

    if (iint->_signum < 0) {
        td =  TID_NEG_INT;
    }
    else {
        td = TID_POS_INT;
    }

    patch_len = ION_BINARY_TYPE_DESC_LENGTH;
    if (_ion_int_is_zero(iint)) {
        len = 0;
        ln = 0;
    }
    else {
        len = _ion_int_abs_bytes_length_helper(iint);
        if (len < ION_lnIsVarLen) {
            ln = len;
        }
        else {
            ln = ION_lnIsVarLen;
            patch_len += ion_binary_len_var_uint_64(len);
        }
    }

    IONCHECK( _ion_writer_binary_start_value( pwriter, patch_len + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(td, ln) );
    if (ln == ION_lnIsVarLen) {
        IONCHECK( ion_binary_write_var_uint_64( pwriter->_typed_writer.binary._value_stream, len ));
    }

    if (len > 0) {
        // here we write any values (other than 0)
        for (offset = 0; offset < len; offset += bytes_written) {
            IONCHECK(ion_int_to_abs_bytes(iint, offset, buffer, LOCAL_STACK_BUFFER_SIZE, &bytes_written));
            ASSERT(bytes_written > 0);
            IONCHECK( ion_stream_write( pwriter->_typed_writer.binary._value_stream, buffer, bytes_written, &written ) );
            if (written != bytes_written) FAILWITH(IERR_WRITE_ERROR);
        }
    }

    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, patch_len + len ));

    iRETURN;
}


iERR _ion_writer_binary_write_double(ION_WRITER *pwriter, double value)
{
    iENTER;
    int len;

    len = ion_binary_len_ion_float(value);

    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_TYPE_DESC_LENGTH + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(TID_FLOAT, len) );
    if (len > 0) {
        IONCHECK( ion_binary_write_float_value( pwriter->_typed_writer.binary._value_stream, value ));
    }
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, ION_BINARY_TYPE_DESC_LENGTH + len ));

    iRETURN;
}

iERR _ion_writer_binary_write_decimal_helper(ION_STREAM *pstream, ION_INT *mantissa, SIZE mantissa_len,
                                             int32_t exponent) {
    iENTER;
    BYTE int_bytes[LOCAL_STACK_BUFFER_SIZE];
    BYTE *chunk_start = NULL;
    SIZE offset, chunk_len, bytes_written, stream_written;

    ASSERT(pstream != NULL);

    IONCHECK(ion_binary_write_var_int_64(pstream, exponent));

    for (offset = 0; offset < mantissa_len; offset += chunk_len) {
        int_bytes[0] = 0;
        if (offset == 0) {
            // Leave an empty space in the beginning for a potential extra sign byte.
            chunk_start = int_bytes + 1;
            chunk_len = LOCAL_STACK_BUFFER_SIZE - 1;
        }
        else {
            chunk_start = int_bytes;
            chunk_len = LOCAL_STACK_BUFFER_SIZE;
        }
        IONCHECK(ion_int_to_abs_bytes(mantissa, offset, chunk_start, chunk_len, &bytes_written));
        ASSERT(bytes_written > 0);
        chunk_len = bytes_written;

        if (offset == 0) {
            if (*chunk_start & 0x80) {
                // The highest bit of the most significant byte is set. An extra byte is needed for the sign.
                chunk_start = int_bytes;
                chunk_len++;
            }
            // Set the sign bit.
            *chunk_start |= (mantissa->_signum < 0) ? (BYTE)0x80 : (BYTE)0;
        }
        IONCHECK(ion_stream_write(pstream, chunk_start, chunk_len, &stream_written));
        if (stream_written != chunk_len) FAILWITH(IERR_WRITE_ERROR);
    }

    iRETURN;
}

iERR _ion_writer_binary_write_decimal_small_helper(ION_STREAM *pstream, uint64_t int_mantissa, int32_t exponent,
                                                   BOOL is_negative) {
    iENTER;

    ASSERT(pstream != NULL);

    // Could be 0e10, -0d0 or true 0 "0d0"
    if (int_mantissa == 0) {
        if (is_negative) {
            IONCHECK( ion_binary_write_var_int_64( pstream, exponent ));
            IONCHECK( ion_binary_write_int_64(pstream, 0, TRUE));
        }
        else if (exponent != 0) {
            IONCHECK( ion_binary_write_var_int_64( pstream, exponent ));
            // 0 mantissa does not need to be written out.
        }
        else {
            // a "true" 0 we already wrote out as the low nibble 0
            // If the value is zero ( i.e., 0d0) then L of Type Value field is zero and there are no length or representation fields.
        }
        SUCCEED();
    }

    // we write out the exponent and then the signed unscaled bits

    IONCHECK(ion_binary_write_var_int_64(pstream, exponent));
    IONCHECK(ion_binary_write_int_64_unsigned(pstream, int_mantissa, is_negative));

    iRETURN;
}

iERR _ion_writer_binary_decimal_quad_len_and_mantissa(ION_WRITER *pwriter, decQuad *value, decQuad *mantissa,
                                                      decContext *context, int32_t exponent, ION_INT *p_int_mantissa,
                                                      SIZE *p_mantissa_len, SIZE *p_len) {
    iENTER;

    ASSERT(!decQuadIsZero(value));
    ASSERT(decQuadIsInteger(mantissa));

    IONCHECK(ion_int_init(p_int_mantissa, pwriter));
    IONCHECK(ion_int_from_decimal(p_int_mantissa, mantissa, context));
    *p_len += ion_binary_len_var_int_64(exponent);
    *p_mantissa_len = _ion_int_abs_bytes_signed_length_helper(p_int_mantissa);
    *p_len += *p_mantissa_len;
    iRETURN;
}

iERR _ion_writer_binary_decimal_number_len_and_mantissa(ION_WRITER *pwriter, decNumber *value, decContext *context,
                                                        ION_INT *p_int_mantissa, SIZE *p_mantissa_len, SIZE *p_len) {
    iENTER;
    ASSERT(!decNumberIsZero(value));

    IONCHECK(ion_int_init(p_int_mantissa, pwriter));
    IONCHECK(_ion_int_from_decimal_number(p_int_mantissa, value, context));
    *p_len += ion_binary_len_var_int_64(value->exponent);
    *p_mantissa_len = _ion_int_abs_bytes_signed_length_helper(p_int_mantissa);
    *p_len += *p_mantissa_len;
    iRETURN;
}

iERR _ion_writer_binary_decimal_small_len(uint64_t mantissa, int32_t exponent, BOOL is_negative, SIZE *p_len) {
    iENTER;
    // Could be 0e10, -0d0 or true 0 "0d0"
    if (mantissa == 0) {
        if (is_negative) {
            *p_len += ion_binary_len_var_int_64(exponent);
            *p_len += 1;  // the size of 1 == signed int 0 (+ or -)
        }
        else if (exponent != 0) {
            *p_len += ion_binary_len_var_int_64(exponent);
        }
        // Else: a "true" 0 we write out as the low nibble 0.
        SUCCEED();
    }

    *p_len += ion_binary_len_var_int_64(exponent);
    *p_len += ion_binary_len_int_64_unsigned(mantissa);

    iRETURN;
}

// TODO this should be used wherever this code is duplicated inline (a lot of places).
iERR _ion_writer_binary_write_header(ION_WRITER *pwriter, int tid, int len, int *p_patch_len)
{
    iENTER;
    int ln;

    ASSERT(p_patch_len);

    *p_patch_len = ION_BINARY_TYPE_DESC_LENGTH;

    if (len < ION_lnIsVarLen) {
        ln = len;
    }
    else {
        ln = ION_lnIsVarLen;
        *p_patch_len += ion_binary_len_var_uint_64(len);
    }

    IONCHECK( _ion_writer_binary_start_value( pwriter, *p_patch_len + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(tid, ln) );
    if (ln == ION_lnIsVarLen) {
        IONCHECK( ion_binary_write_var_uint_64( pwriter->_typed_writer.binary._value_stream, len ));
    }
    iRETURN;
}

/**
 * Write a decimal with a mantissa that does not fit in 64 bits.
 */
iERR _ion_writer_binary_write_decimal_quad_helper(ION_WRITER *pwriter, decQuad *value, decQuad *dec_mantissa, int32_t exponent)
{
    iENTER;
    ION_INT int_mantissa;
    SIZE int_mantissa_len;
    int len = 0, patch_len;

    IONCHECK(_ion_writer_binary_decimal_quad_len_and_mantissa(pwriter, value, dec_mantissa, &pwriter->deccontext,
                                                              exponent, &int_mantissa, &int_mantissa_len, &len));
    IONCHECK(_ion_writer_binary_write_header(pwriter, TID_DECIMAL, len, &patch_len));
    IONCHECK(_ion_writer_binary_write_decimal_helper(pwriter->_typed_writer.binary._value_stream, &int_mantissa,
                                                     int_mantissa_len, exponent));
    IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len + len ));
    iRETURN;
}

/**
 * Write a decimal with a mantissa that fits in 64 bits.
 */
iERR _ion_writer_binary_write_decimal_small(ION_WRITER *pwriter, uint64_t mantissa, int32_t exponent, BOOL is_negative)
{
    iENTER;
    int len = 0, patch_len;

    IONCHECK(_ion_writer_binary_decimal_small_len(mantissa, exponent, is_negative, &len));
    IONCHECK(_ion_writer_binary_write_header(pwriter, TID_DECIMAL, len, &patch_len));
    IONCHECK(_ion_writer_binary_write_decimal_small_helper(pwriter->_typed_writer.binary._value_stream, mantissa,
                                                           exponent, is_negative));
    IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len + len ));

    iRETURN;
}

iERR _ion_writer_binary_write_decimal_number_helper(ION_WRITER *pwriter, decNumber *value)
{
    iENTER;
    ION_INT int_mantissa;
    SIZE int_mantissa_len;
    int len = 0, patch_len;
    IONCHECK(_ion_writer_binary_decimal_number_len_and_mantissa(pwriter, value, &pwriter->deccontext, &int_mantissa,
                                                                &int_mantissa_len, &len));
    IONCHECK(_ion_writer_binary_write_header(pwriter, TID_DECIMAL, len, &patch_len));
    IONCHECK(_ion_writer_binary_write_decimal_helper(pwriter->_typed_writer.binary._value_stream, &int_mantissa,
                                                     int_mantissa_len, value->exponent));
    IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len + len ));
    iRETURN;
}

iERR _ion_writer_binary_decimal_quad_components(const decQuad *value, decContext *context, uint64_t *p_int_mantissa,
                                                decQuad *dec_mantissa, int32_t *p_exp, BOOL *p_is_negative,
                                                BOOL *p_overflow) {
    iENTER;

    ASSERT(value);
    ASSERT(p_int_mantissa);
    ASSERT(p_exp);
    ASSERT(p_is_negative);
    ASSERT(p_overflow);

    // Could be 0e10, -0d0 or true 0 "0d0"
    if (decQuadIsZero(value)) {
        *p_exp = decQuadGetExponent(value);
        *p_int_mantissa = 0;
        *p_is_negative = decQuadIsSigned(value);
        *p_overflow = FALSE;
        SUCCEED();
    }
    ion_quad_get_exponent_and_shift(value, context, dec_mantissa, p_exp);
    *p_int_mantissa = decQuadToUInt64(dec_mantissa, context, p_overflow, p_is_negative);

    iRETURN;
}

iERR _ion_writer_binary_write_decimal_quad(ION_WRITER *pwriter, decQuad *value)
{
    iENTER;
    uint64_t small_mantissa;
    decQuad big_mantissa;
    int32_t exponent;
    BOOL is_negative, overflow;

    if (value == NULL) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_DECIMAL));
        SUCCEED();
    }

    IONCHECK(_ion_writer_binary_decimal_quad_components(value, &pwriter->deccontext, &small_mantissa, &big_mantissa,
                                                        &exponent, &is_negative, &overflow));

    if (overflow) {
        IONCHECK(_ion_writer_binary_write_decimal_quad_helper(pwriter, value, &big_mantissa, exponent));
    }
    else {
        IONCHECK(_ion_writer_binary_write_decimal_small(pwriter, small_mantissa, exponent, is_negative));
    }

    iRETURN;
}

iERR _ion_writer_binary_write_decimal_number(ION_WRITER *pwriter, decNumber *value)
{
    iENTER;
    uint64_t small_mantissa = 0;
    int i, dec_shift = 1;
    if (value == NULL) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_DECIMAL));
        SUCCEED();
    }
    // 2^64 - 1 is 20 decimal digits, so anything below 20 will fit in 64 bits. This is an optimization, so it's not
    // important to identify the 20-digit values that could fit in 64 bits.
    if (value->digits > 19) {
        IONCHECK(_ion_writer_binary_write_decimal_number_helper(pwriter, value));
    }
    else {
        for (i = 0; i < DECDPUN; i++) {
            dec_shift *= 10;
        }
        for (i = 0; i < value->digits; i++) {
            small_mantissa *= dec_shift;
            small_mantissa += value->lsu[i];
        }
        IONCHECK(_ion_writer_binary_write_decimal_small(pwriter, small_mantissa, value->exponent, decNumberIsNegative(value)));

    }
    iRETURN;
}

iERR _ion_writer_binary_write_timestamp_without_fraction_helper(ION_WRITER *pwriter, ION_TIMESTAMP *ptime)
{
    iENTER;
    ION_STREAM *pstream = pwriter->_typed_writer.binary._value_stream;
    ION_TIMESTAMP ptime_utc; // Upon UTC conversion, do not overwrite the user input.

    ASSERT(pstream != NULL);

    if (NULL == ptime) {
        // nothing else to do here - and the timestamp is be NULL
        SUCCEED();
    }

    // first we write out the local offset (and we write a -0 if it is not known)
    if (HAS_TZ_OFFSET(ptime)) {
        IONCHECK(ion_binary_write_var_int_64(pstream, ptime->tz_offset));
        IONCHECK(_ion_timestamp_initialize(&ptime_utc));
        IONCHECK(_ion_timestamp_to_utc(ptime, &ptime_utc)); // Binary timestamps are stored in UTC with local offset intact.
        ptime = &ptime_utc;
    }
    else {
        ION_PUT( pstream, ION_BINARY_VAR_INT_NEGATIVE_ZERO );
    }

    if (IS_FLAG_ON(ptime->precision, ION_TS_YEAR)) {
        // year is from 0001 to 9999
        // or 0x1 to 0x270F or 14 bits - 1 or 2 bytes
        IONCHECK(ion_binary_write_var_uint_64(pstream, ptime->year));
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MONTH)) {
        IONCHECK(ion_binary_write_var_uint_64(pstream, ptime->month));
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_DAY)) {
        IONCHECK(ion_binary_write_var_uint_64(pstream, ptime->day));
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MIN)) {
        IONCHECK(ion_binary_write_var_uint_64(pstream, ptime->hours));
        IONCHECK(ion_binary_write_var_uint_64(pstream, ptime->minutes));
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_SEC)) {
        IONCHECK(ion_binary_write_var_uint_64(pstream, ptime->seconds));
    }
    iRETURN;
}

int _ion_writer_binary_timestamp_len_without_fraction( ION_TIMESTAMP *ptime)
{
    int len;

    if (NULL == ptime) {
        // nothing to do for a null.timestamp, it's all in the td byte
        return 0;
    }

    // first we write out the local offset (and we write a -0 if it is not known)
    if (HAS_TZ_OFFSET(ptime)) {
        len = ion_binary_len_var_int_64(ptime->tz_offset);
    }
    else {
        len = 1; // len of -0 byte
    }

    //if the value isn't null we always have year
    len += ion_binary_len_var_uint_64(ptime->year);
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MONTH)) {
        len += 1; // month always fits in 1 byte
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_DAY)) {
        len += 1; // day always fits in 1 byte
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_MIN)) {
        len += 1; // hours take 1 byte each
        len += 1; // and minutes take 1 byte each
    }
    if (IS_FLAG_ON(ptime->precision, ION_TT_BIT_SEC)) {
        len += 1; // seconds are also 1 byte each
    }
    return len;
}

/**
 * Write a timestamp fraction with a mantissa that does not fit in 64 bits.
 */
iERR _ion_writer_binary_write_timestamp_fraction_quad(ION_WRITER *pwriter, ION_TIMESTAMP *value, decQuad *dec_mantissa, int32_t exponent)
{
    iENTER;
    ION_INT int_mantissa;
    SIZE int_mantissa_len;
    int len, patch_len;

    len = _ion_writer_binary_timestamp_len_without_fraction(value);
    IONCHECK(_ion_writer_binary_decimal_quad_len_and_mantissa(pwriter, &value->fraction, dec_mantissa,
                                                              &pwriter->deccontext, exponent, &int_mantissa,
                                                              &int_mantissa_len, &len));

    IONCHECK(_ion_writer_binary_write_header(pwriter, TID_TIMESTAMP, len, &patch_len));
    IONCHECK(_ion_writer_binary_write_timestamp_without_fraction_helper(pwriter, value));
    IONCHECK(_ion_writer_binary_write_decimal_helper(pwriter->_typed_writer.binary._value_stream, &int_mantissa,
                                                     int_mantissa_len, exponent));
    IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len + len ));
    iRETURN;
}

/**
 * Write a timestamp fraction with a mantissa that fits in 64 bits.
 */
iERR _ion_writer_binary_write_timestamp_fraction_small(ION_WRITER *pwriter, ION_TIMESTAMP *value, uint64_t mantissa, int32_t exponent, BOOL is_negative)
{
    iENTER;
    int len = 0, patch_len;

    len = _ion_writer_binary_timestamp_len_without_fraction(value);
    IONCHECK(_ion_writer_binary_decimal_small_len(mantissa, exponent, is_negative, &len));

    IONCHECK(_ion_writer_binary_write_header(pwriter, TID_TIMESTAMP, len, &patch_len));
    IONCHECK(_ion_writer_binary_write_timestamp_without_fraction_helper(pwriter, value));
    IONCHECK(_ion_writer_binary_write_decimal_small_helper(pwriter->_typed_writer.binary._value_stream, mantissa,
                                                           exponent, is_negative));
    IONCHECK(_ion_writer_binary_patch_lengths( pwriter, patch_len + len ));

    iRETURN;
}

iERR _ion_writer_binary_write_timestamp_with_fraction( ION_WRITER *pwriter, ION_TIMESTAMP *ptime)
{
    iENTER;
    uint64_t small_mantissa;
    decQuad big_mantissa;
    int32_t exponent;
    BOOL is_negative, overflow;

    ASSERT(pwriter != NULL);
    ASSERT(IS_FLAG_ON(ptime->precision, ION_TT_BIT_FRAC));

    IONCHECK(_ion_writer_binary_decimal_quad_components(&ptime->fraction, &pwriter->deccontext, &small_mantissa,
                                                        &big_mantissa, &exponent, &is_negative, &overflow));

    if (overflow) {
        IONCHECK(_ion_writer_binary_write_timestamp_fraction_quad(pwriter, ptime, &big_mantissa, exponent));
    }
    else {
        IONCHECK(_ion_writer_binary_write_timestamp_fraction_small(pwriter, ptime, small_mantissa, exponent, is_negative));
    }

    iRETURN;
}

iERR _ion_writer_binary_write_timestamp_without_fraction(ION_WRITER *pwriter, iTIMESTAMP value)
{
    iENTER;
    int len, patch_len;

    len = _ion_writer_binary_timestamp_len_without_fraction(value);

    IONCHECK( _ion_writer_binary_write_header(pwriter, TID_TIMESTAMP, len, &patch_len));
    IONCHECK( _ion_writer_binary_write_timestamp_without_fraction_helper( pwriter, value));
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, patch_len + len ));
    iRETURN;
}

iERR _ion_writer_binary_write_timestamp(ION_WRITER *pwriter, iTIMESTAMP value)
{
    iENTER;

    if (value == NULL) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_TIMESTAMP));
        SUCCEED();
    }
    if (IS_FLAG_ON(value->precision, ION_TT_BIT_FRAC)) {
        IONCHECK(_ion_writer_binary_write_timestamp_with_fraction(pwriter, value));
    }
    else {
        IONCHECK(_ion_writer_binary_write_timestamp_without_fraction(pwriter, value));
    }
    iRETURN;
}

iERR _ion_writer_binary_write_string(ION_WRITER *pwriter, ION_STRING *pstr )
{
    iENTER;
    int  len, ln;
    int  patch_len;
    SIZE written;

    if (!pstr || !pstr->value) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_STRING));
        SUCCEED();
    }

    patch_len = ION_BINARY_TYPE_DESC_LENGTH;
    len = pstr->length;

    if (len < ION_lnIsVarLen) {
        ln = len;
    }
    else {
        ln = ION_lnIsVarLen;
        patch_len += ion_binary_len_var_uint_64(len);
    }

    IONCHECK( _ion_writer_binary_start_value( pwriter, patch_len + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(TID_STRING, ln) );
    if (ln == ION_lnIsVarLen) {
        IONCHECK( ion_binary_write_var_uint_64( pwriter->_typed_writer.binary._value_stream, len ));
    }
    IONCHECK( ion_stream_write( pwriter->_typed_writer.binary._value_stream, pstr->value, len, &written ));
    if (written != len) FAILWITH(IERR_WRITE_ERROR);
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, patch_len + len ));

    iRETURN;
}

iERR _ion_writer_binary_write_symbol_id(ION_WRITER *pwriter, SID sid)
{
    iENTER;
    if (sid == ION_SYS_SID_IVM && pwriter->depth == 0 && pwriter->annotation_count == 0) {
        SUCCEED(); // At the top level, writing a symbol value that looks like the IVM is a no-op.
    }
    int  len = ion_binary_len_uint_64(sid);
    ASSERT( len < ION_lnIsVarLen );

    // Write symbol type descriptor and int value out and patch lens.
    IONCHECK( _ion_writer_binary_start_value( pwriter, ION_BINARY_TYPE_DESC_LENGTH + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(TID_SYMBOL, len));
    if (sid > 0) {
        IONCHECK(ion_binary_write_uint_64(pwriter->_typed_writer.binary._value_stream, sid));
    }
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, len + ION_BINARY_TYPE_DESC_LENGTH ));

    iRETURN;
}

iERR _ion_writer_binary_write_symbol(ION_WRITER *pwriter, ION_STRING *pstr )
{
    iENTER;
    SID sid = UNKNOWN_SID;

    if (!pstr || !pstr->value) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_SYMBOL));
        SUCCEED();
    }

    IONCHECK( _ion_writer_make_symbol_helper(pwriter, pstr, &sid ));
    ASSERT(sid != UNKNOWN_SID);

    IONCHECK( _ion_writer_binary_write_symbol_id(pwriter, sid));

    iRETURN;
}


iERR _ion_writer_binary_write_clob(ION_WRITER *pwriter, BYTE *pbuf, SIZE len)
{
    iENTER;
    int  patch_len = ION_BINARY_TYPE_DESC_LENGTH;
    int  ln = len;
    SIZE written;

    if (pbuf == NULL) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_CLOB));
        SUCCEED();
    }
 
    if (len >= ION_lnIsVarLen) {
        ln = ION_lnIsVarLen;
        patch_len += ion_binary_len_var_uint_64(len);
    }

    IONCHECK( _ion_writer_binary_start_value( pwriter, patch_len + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(TID_CLOB, ln) );
        
    if (len >= ION_lnIsVarLen) {
        IONCHECK( ion_binary_write_var_uint_64( pwriter->_typed_writer.binary._value_stream, len ));
    }
    IONCHECK( ion_stream_write( pwriter->_typed_writer.binary._value_stream, pbuf, len, &written ));
    if (written != len) FAILWITH(IERR_WRITE_ERROR);
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, patch_len + len ));

    iRETURN;
}

iERR _ion_writer_binary_write_blob(ION_WRITER *pwriter, BYTE *pbuf, SIZE len)
{
    iENTER;
    int  patch_len = ION_BINARY_TYPE_DESC_LENGTH;
    int  ln = len;
    SIZE written;

    if (pbuf == NULL) {
        IONCHECK(_ion_writer_binary_write_typed_null(pwriter, tid_BLOB));
        SUCCEED();
    }

    if (len >= ION_lnIsVarLen) {
        ln = ION_lnIsVarLen;
        patch_len += ion_binary_len_var_uint_64(len);
    }

    IONCHECK( _ion_writer_binary_start_value( pwriter, patch_len + len ));
    ION_PUT( pwriter->_typed_writer.binary._value_stream, makeTypeDescriptor(TID_BLOB, ln) );
        
    if (len >= ION_lnIsVarLen) {
        IONCHECK( ion_binary_write_var_uint_64( pwriter->_typed_writer.binary._value_stream, len ));
    }
    IONCHECK( ion_stream_write( pwriter->_typed_writer.binary._value_stream, pbuf, len, &written ));
    if (written != len) FAILWITH(IERR_WRITE_ERROR);
    IONCHECK( _ion_writer_binary_patch_lengths( pwriter, patch_len + len ));

    iRETURN;
}

iERR _ion_writer_binary_write_one_value(ION_WRITER *pwriter, ION_READER *preader)
{
    iENTER;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);
    if (!preader) FAILWITH(IERR_INVALID_ARG);

    FAILWITH(IERR_NOT_IMPL);

    iRETURN;
}

iERR _ion_writer_binary_write_all_values(ION_WRITER *pwriter, ION_READER *preader)
{
    iENTER;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);
    if (!preader) FAILWITH(IERR_INVALID_ARG);

    FAILWITH(IERR_NOT_IMPL);

    iRETURN;
}

iERR _ion_writer_binary_close(ION_WRITER *pwriter)
{
    iENTER;
    ION_BINARY_WRITER *bwriter;
    BOOL               patches, values;

    ASSERT(pwriter);

    bwriter = &pwriter->_typed_writer.binary;

    patches = !ION_COLLECTION_IS_EMPTY(&bwriter->_patch_list);
    values  = ion_stream_get_position(bwriter->_value_stream) != 0;
    if (patches || values) {
        IONCHECK(_ion_writer_binary_flush_to_output(pwriter));
    }
    IONCHECK(ion_stream_flush(pwriter->output));
    IONCHECK(ion_stream_close( bwriter->_value_stream ));

    iRETURN;
}

iERR _ion_writer_binary_flush_to_output(ION_WRITER *pwriter) 
{
    iENTER;
 
    int                pos, buffer_length;
    int                patch_pos;
    int                len;
    SIZE               written;
    BOOL               has_imports, needs_local_symbol_table;

    ION_BINARY_PATCH  *ppatch;
    ION_STREAM        *out = pwriter->output;
    ION_STREAM        *values_in;
    ION_BINARY_WRITER *bwriter = &pwriter->_typed_writer.binary;

    has_imports = (pwriter->symbol_table && !ION_COLLECTION_IS_EMPTY(&pwriter->symbol_table->import_list));
    needs_local_symbol_table = (pwriter->_has_local_symbols || has_imports);

    if (!bwriter->_version_marker_written
     || needs_local_symbol_table
    ) {
        IONCHECK( ion_stream_write( out, ION_VERSION_MARKER, ION_VERSION_MARKER_LENGTH, &written ));
        if (written != ION_VERSION_MARKER_LENGTH) FAILWITH(IERR_WRITE_ERROR);
        bwriter->_version_marker_written = TRUE; // just so we remember
    }
    
    if (needs_local_symbol_table) {

        // we have (we could have but didn't) saved the value stack before recursing 
        // into the symbol table "write" but since we want to write to the stream we're
        // in the middle of writing to we'd have messed it all up !
        // 
        // so we use the version of write_symbol_table below that precomputes all the 
        // lengths so it doesn't need an extra buffer and writes into out stream.  It
        // does take two passes over the symbol table contents to calc the length so
        // that the length can be written in the front - but this is a local table so
        // it shouldn't be too bad to do it this way.

        //not: IONCHECK( ion_symbol_table_write( PTR_TO_HANDLE(pwriter), pwriter->symbol_table ));
        IONCHECK(_ion_writer_binary_serialize_symbol_table(pwriter->symbol_table, out, &len));
    }

    // we free the local symbol table as we'll be starting fresh now on the next value
    // this is a no-op in the event the local table is not local (shared or simply absent)
    // this isn't the same as _no_local_symbols because the local table gets allocated
    // before any symbols are added to it
    IONCHECK( _ion_writer_free_local_symbol_table( pwriter ));

    // 
    values_in = bwriter->_value_stream;
    buffer_length = (int)ion_stream_get_position( values_in );  // TODO - this needs 64bit care

    // rewind the value stream we have been writing into
    IONCHECK(ion_stream_seek(values_in, 0));
    pos = 0;

    ppatch = (ION_BINARY_PATCH *)_ion_collection_head( &bwriter->_patch_list );
    patch_pos = (ppatch != NULL) ? ppatch->_offset : buffer_length;

    while (pos < buffer_length) {
        // we write pending patches until the pending patch is further downstream
        while (patch_pos <= pos) {
            IONCHECK( ion_binary_write_type_desc_with_length( out, ppatch->_type, ppatch->_length ));

            _ion_collection_pop_head( &bwriter->_patch_list );
            ppatch = (ION_BINARY_PATCH *)_ion_collection_head( &bwriter->_patch_list );
            patch_pos = (ppatch != NULL) ? ppatch->_offset : buffer_length;
        }

        // the patch is in front of us so we write the value stream until we're
        // at the next patch (or we finally run out of input values to write
        // patches are always embedded in the stream, so we always finish with
        // data values (not a patch). write_stream won't write more than the
        // input stream holds so when patch_pos is past the end it simply writes
        // everything that's left
        len = patch_pos - pos;
        IONCHECK( ion_stream_write_stream( out, values_in, len, &written ) );
        if (written != len) FAILWITH(IERR_WRITE_ERROR);
        pos += len;
    }

    while (ppatch) {
        IONCHECK( ion_binary_write_type_desc_with_length( out, ppatch->_type, ppatch->_length ));
        _ion_collection_pop_head( &bwriter->_patch_list );
        ppatch = (ION_BINARY_PATCH *)_ion_collection_head( &bwriter->_patch_list );
    }

    // reset the patches list and the value streams buffers (recycling them)
    _ion_collection_reset( &bwriter->_patch_list );
    _ion_collection_reset( &bwriter->_value_list );

    // the clear the symbol table as we're starting over
    pwriter->symbol_table = NULL;

    // and finally re-initialize the value stream to reset it
    IONCHECK( ion_stream_seek( values_in, 0 ));
    IONCHECK( ion_stream_truncate( values_in ));

    // and if we've cleared everything out, we'll need a fresh version marker if we ever restart
    bwriter->_version_marker_written = FALSE;

    iRETURN;
}

//
// these routines serialize a local symbol table out to an output stream
// these are NOT the same as the routine in symbol table that does this
// as this version does not generate a second in memory copy when the
// output stream is binary.  And this only emits a binary version (since
// the text version of a local symbol table isn't usually valuable)
//
// however this requires two passes over the table to first calculate
// the lengths of the various variable length values (notably the import
// list and the symbol list) so that the length prefixes can be written
// before the data.  memory vs speed.
//
// it's also important to note that while writing to the binary output
// stream you will often generate a local symbol table that has to preceed
// the value.  You can simply (recursively) pass that writer to the
// symbol table to do the writing as you will need to do so when there is
// a stack of pending values.  Starting to write a symbol table value
// would stomp of the pending values.  Thus was this born.  <sigh>
//
// as with the rest of the writer this was ported from the Java streaming
// writer as it's very touchy.  As such it has been simplified somewhat.
 
iERR _ion_writer_binary_calc_serialized_symbol_table_length(ION_WRITER *pwriter, int *p_length)
{
    iENTER;
    ION_SYMBOL_TABLE *psymtab = pwriter->symbol_table;

    IONCHECK(_ion_writer_binary_serialize_symbol_table(psymtab, NULL, p_length));

    iRETURN;
}

iERR _ion_writer_binary_serialize_symbol_table(ION_SYMBOL_TABLE *psymtab, ION_STREAM *out, int *p_length)
{
    iENTER;

    ION_SYMBOL_TABLE_IMPORT *import;
    ION_SYMBOL              *symbol;
    ION_COLLECTION_CURSOR    import_cursor;
    ION_COLLECTION_CURSOR    symbol_cursor;

    int output_start, output_finish, total_len;
    int import_len, import_list_len, import_header_len;
    int table_struct_len, table_header_len;
    int annotated_value_len, annotation_header_len;

    int symbol_list_len, symbol_header_len;
    int  tid;
    BOOL must_use_struct = FALSE;
    SID  sid;
        
    // first calculate the length of the bits and pieces we will be
    // writing out in the second phase.  We do this all in one big
    // hairy method so that we can remember the lengths of most of
    // these bits and pieces so that we have to recalculate them as
    // we go to write out the typedesc headers when we write out the
    // values themselves.

    // this routine is intended for us ONLY on local symbol tables
    // the normal high level routines should be used for other
    // symbol tables instead.
    ASSERT(psymtab != NULL);
    ASSERT(ION_STRING_IS_NULL(&psymtab->name) == TRUE);
    ASSERT(psymtab->version < 1);

    import_list_len = import_header_len = 0;

    if (!ION_COLLECTION_IS_EMPTY(&psymtab->import_list)) {
        ION_COLLECTION_OPEN(&psymtab->import_list, import_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(import_cursor, import);
            if (!import) break;
            import_len = ion_writer_binary_serialize_import_struct_length(import);
            if (import_len >= ION_lnIsVarLen) {
              import_len += ion_binary_len_var_uint_64(import_len); // the overflow length if this is longer than 14
            }
            import_len += ION_BINARY_TYPE_DESC_LENGTH; // and the type descriptor
            import_list_len += import_len;
        }
        ION_COLLECTION_CLOSE(import_cursor);

        ASSERT( import_list_len > 0 );  // if there weren't any imports how did we get here?
        import_header_len = ION_BINARY_TYPE_DESC_LENGTH + 1;  // fieldid(imports) + typedesc for array
        if (import_list_len >= ION_lnIsVarLen) {
            import_header_len += ion_binary_len_var_uint_64(import_list_len);
        }
    }
       
    symbol_list_len = symbol_header_len = 0;
    if (!ION_COLLECTION_IS_EMPTY(&psymtab->symbols)) {
        sid = 0;
        ION_COLLECTION_OPEN(&psymtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, symbol);
            if (!symbol) break;
            if (symbol_list_len > 0 && symbol->sid != sid + 1) {
                must_use_struct = TRUE;
            }
            sid = symbol->sid;
            if (symbol->psymtab != psymtab) continue;
            symbol_list_len += ion_writer_binary_serialize_symbol_length(symbol);
        }
        ION_COLLECTION_CLOSE(symbol_cursor);

        if (must_use_struct) {
            // if we have discontiguous symbols we use the struct form
            // of the symbol list - so we need to make room for sid's
            ION_COLLECTION_OPEN(&psymtab->symbols, symbol_cursor);
            for (;;) {
                ION_COLLECTION_NEXT(symbol_cursor, symbol);
                if (!symbol) break;
                sid = symbol->sid;
                if (symbol->psymtab != psymtab) continue;
                symbol_list_len += ion_binary_len_var_uint_64(sid);
            }
            ION_COLLECTION_CLOSE(symbol_cursor);
        }

        if (symbol_list_len > 0) {
            symbol_header_len = ION_BINARY_TYPE_DESC_LENGTH + 1; // fldid + typedesc
            if (symbol_list_len >= ION_lnIsVarLen) {
                symbol_header_len += ion_binary_len_var_uint_64(symbol_list_len);
            }
        }
    }

    table_struct_len  = import_header_len + import_list_len;
    table_struct_len += symbol_header_len + symbol_list_len;
    table_header_len = ION_BINARY_TYPE_DESC_LENGTH;  // tid_struct
    if (table_struct_len >= ION_lnIsVarLen) {
        table_header_len += ion_binary_len_var_uint_64(table_struct_len);
    }

    // $ion_symbol_table::{ ... }
    // <anntd(1)>{<total_len?>}<ann_len(1)><anns(1)> <td(1)>{<contentlen?>}<content>
    annotated_value_len  = 1 + 1; // symbol id ($ion_symbol_table) + length of 1
    annotated_value_len += table_header_len + table_struct_len;
    annotation_header_len = ION_BINARY_TYPE_DESC_LENGTH;
    if (annotated_value_len >= ION_lnIsVarLen) {
        annotation_header_len += ion_binary_len_var_uint_64(annotated_value_len);
    }

    total_len = annotation_header_len + annotated_value_len;
    if (p_length) {
        *p_length = total_len;
    }
        
    // trick to just get the length and gaurantee it's the same length
    if (out == NULL) {
        SUCCEED();
    }

    // -------------------------------------------------------------------------
    // now that we know how long most everything is we can write the symbol table
    // data out in one single forward pass - with appropriate length prefixes

    output_start = (int)ion_stream_get_position(out);  // TODO - this needs 64bit care

    // write out the annotation, struct type desc and overall length
    if (annotated_value_len >= ION_lnIsVarLen) {
        ION_PUT(out, makeTypeDescriptor(TID_UTA, ION_lnIsVarLen));
        IONCHECK(ion_binary_write_var_uint_64(out, annotated_value_len));  // symtab has overflow length7
    }
    else {
        ION_PUT(out, makeTypeDescriptor(TID_UTA, annotated_value_len));
    }

    ION_PUT(out, ION_BINARY_MAKE_1_BYTE_VAR_INT(1)); // len of the annotations in the annotation "list" (i.e. 1 * 1 byte symbol)
    ION_PUT(out, ION_BINARY_MAKE_1_BYTE_VAR_INT(ION_SYS_SID_SYMBOL_TABLE));

    if (table_struct_len >= ION_lnIsVarLen) {
        ION_PUT(out, makeTypeDescriptor(TID_STRUCT, ION_lnIsVarLen));
        IONCHECK(ion_binary_write_var_uint_64(out, table_struct_len));  // symtab has overflow length
    }
    else {
        ION_PUT(out, makeTypeDescriptor(TID_STRUCT, table_struct_len));
    }

    // now write imports (if we have any)
    if (import_list_len > 0) {
        // write import field id, list type desc and maybe overflow length
        ION_PUT(out, ION_BINARY_MAKE_1_BYTE_VAR_INT(ION_SYS_SID_IMPORTS)); // field sid
        if (import_list_len >= ION_lnIsVarLen) {
            ION_PUT(out, makeTypeDescriptor(TID_LIST, ION_lnIsVarLen));
            IONCHECK(ion_binary_write_var_uint_64(out, import_list_len));  // symtab has overflow length
        }
        else {
            ION_PUT(out, makeTypeDescriptor(TID_LIST, import_list_len));
        }

        ION_COLLECTION_OPEN(&psymtab->import_list, import_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(import_cursor, import);
            if (!import) break;

            import_len = ion_writer_binary_serialize_import_struct_length(import);
            if (import_len >= ION_lnIsVarLen) {
                ION_PUT(out, makeTypeDescriptor(TID_STRUCT, ION_lnIsVarLen));
                IONCHECK(ion_binary_write_var_uint_64(out, import_len));  // symtab has overflow length
            }
            else {
                ION_PUT(out, makeTypeDescriptor(TID_STRUCT, import_len));
            }
            // now we write the name, version, and max id
            IONCHECK(ion_binary_write_string_with_field_sid(out, ION_SYS_SID_NAME, &import->name));
            IONCHECK(ion_binary_write_int32_with_field_sid(out, ION_SYS_SID_VERSION, import->version));
            if (import->max_id > 0) {
                IONCHECK(ion_binary_write_int32_with_field_sid(out, ION_SYS_SID_MAX_ID, import->max_id));
            }
        }
        ION_COLLECTION_CLOSE(import_cursor);
    }

    // and finally write the local symbols (which is really the most important part)
    if (symbol_list_len > 0) {
        ION_PUT(out, ION_BINARY_MAKE_1_BYTE_VAR_INT(ION_SYS_SID_SYMBOLS)); // field sid
        // write import field id, list type desc and maybe overflow length
        tid = must_use_struct ? TID_STRUCT : TID_LIST;
        if (symbol_list_len >= ION_lnIsVarLen) {
            ION_PUT(out, makeTypeDescriptor(tid, ION_lnIsVarLen));
            IONCHECK(ion_binary_write_var_uint_64(out, symbol_list_len));  // symtab has overflow length
        }
        else {
            ION_PUT(out, makeTypeDescriptor(tid, symbol_list_len));
        }

        //  write the strings out
        ION_COLLECTION_OPEN(&psymtab->symbols, symbol_cursor);
        for (;;) {
            ION_COLLECTION_NEXT(symbol_cursor, symbol);
            if (!symbol) break;
            if (symbol->psymtab != psymtab) continue;
            if (must_use_struct) {
                IONCHECK(ion_binary_write_var_uint_64(out, symbol->sid)); // the sid is the field id here
            }
            IONCHECK(ion_binary_write_string_with_td_byte(out, &symbol->value));
        }
        ION_COLLECTION_CLOSE(symbol_cursor);
    }

    output_finish = (int)ion_stream_get_position(out);  // TODO - this needs 64bit care
    if (output_finish - output_start != total_len) {
        FAILWITH(IERR_INVALID_STATE);
    }

    iRETURN;
}

int ion_writer_binary_serialize_import_struct_length(ION_SYMBOL_TABLE_IMPORT *import)
{
    int len;

    len = import->name.length;
    if (len >= ION_lnIsVarLen) {
        len += ion_binary_len_var_uint_64(len);  // string has overflow length
    }
    len += 1 + ION_BINARY_TYPE_DESC_LENGTH; // field id (name)  + type desc
    len += 1 + ION_BINARY_TYPE_DESC_LENGTH + ion_binary_len_uint_64(import->version); // field id(version) + type desc + int
    if (import->max_id > 0) {
        len += 1 + ION_BINARY_TYPE_DESC_LENGTH + ion_binary_len_uint_64(import->max_id); // field id(max_id) + type desc + int
    }

    // cas 1 sept 2012 - this should be done by the caller! 
    // now len is the length of the content of the import struct
    // see if it's too big for a low nibble length
    // if (len >= ION_lnIsVarLen) {
    //    len += ion_binary_len_var_uint_64(len);  // whole struct has overflow length
    // }
    // len += ION_BINARY_TYPE_DESC_LENGTH; // type desc ion_struct

    return len;
}

int ion_writer_binary_serialize_symbol_length(ION_SYMBOL *symbol)
{
    int len;

    len = symbol->value.length;
    if (len >= ION_lnIsVarLen) {
        len += ion_binary_len_var_uint_64(len);  // string has overflow length
    }
    len += ION_BINARY_TYPE_DESC_LENGTH;  // type desc byte
    return len;
}
