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
// internal impl of the text writer
// these are non-public routines used by the text writer
//

#include "ion_internal.h"
#include <float.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#if defined(_MSC_VER)
#define FLOAT_CLASS(x) _fpclass(x)
#elif defined(__GNUC__)
#define FLOAT_CLASS(x) fpclassify(x)
#else
#error Unsupported Platform
#endif

#define LOCAL_INT_CHAR_BUFFER_LENGTH   257

iERR _ion_writer_text_initialize(ION_WRITER *pwriter)
{
    iENTER;

    pwriter->_typed_writer.text._no_output = TRUE;
    pwriter->_typed_writer.text._separator_character = ' ';

if (_debug_on) pwriter->_typed_writer.text._separator_character = '\n';

    pwriter->_typed_writer.text._top = 0;
    pwriter->_typed_writer.text._stack_size = DEFAULT_WRITER_STACK_DEPTH;
    IONCHECK(ion_temp_buffer_alloc(&pwriter->temp_buffer
                                  ,DEFAULT_WRITER_STACK_DEPTH * sizeof(*(TEXTWRITER(pwriter)->_stack_parent_type))
                                  ,(void **)&TEXTWRITER(pwriter)->_stack_parent_type)
    );
    IONCHECK(ion_temp_buffer_alloc(&pwriter->temp_buffer
            ,DEFAULT_WRITER_STACK_DEPTH * sizeof(*(TEXTWRITER(pwriter)->_stack_flags))
            ,(void **)&TEXTWRITER(pwriter)->_stack_flags)
    );
    
    iRETURN;
}

iERR _ion_writer_text_grow_stack(ION_WRITER *pwriter)
{
    iENTER;

    int       old_type_size = TEXTWRITER(pwriter)->_stack_size * sizeof(*(TEXTWRITER(pwriter)->_stack_parent_type));
    int       old_flag_size = TEXTWRITER(pwriter)->_stack_size * sizeof(*(TEXTWRITER(pwriter)->_stack_flags));
    int       new_type_size = 2 * old_type_size;
    int       new_flag_size = 2 * old_flag_size;
    ION_TYPE *pnew_types;
    BYTE     *pnew_flags;

    IONCHECK(ion_temp_buffer_alloc(&pwriter->temp_buffer, new_type_size, (void **)&pnew_types));
    IONCHECK(ion_temp_buffer_alloc(&pwriter->temp_buffer, new_flag_size, (void **)&pnew_flags));

    memcpy(pnew_types, TEXTWRITER(pwriter)->_stack_parent_type, old_type_size);
    memcpy(pnew_flags, TEXTWRITER(pwriter)->_stack_flags, old_flag_size);

    TEXTWRITER(pwriter)->_stack_parent_type = pnew_types;
    TEXTWRITER(pwriter)->_stack_flags = pnew_flags;

    TEXTWRITER(pwriter)->_stack_size *= 2;

    iRETURN;
}

iERR _ion_writer_text_push(ION_WRITER *pwriter, ION_TYPE type)
{
    iENTER;

    if (TEXTWRITER(pwriter)->_top >= TEXTWRITER(pwriter)->_stack_size) {
        IONCHECK(_ion_writer_text_grow_stack(pwriter));
    }

    // set [top] of the stack to the right values, we'll
    // push the top later (top is effectively - next stack entry)
    TEXTWRITER(pwriter)->_stack_parent_type[TEXTWRITER(pwriter)->_top] = type;
    ION_TEXT_WRITER_SET_IN_STRUCT(pwriter->_in_struct);
    ION_TEXT_WRITER_SET_PENDING_COMMA(TEXTWRITER(pwriter)->_pending_separator);

    switch ((intptr_t)type) {
    case (intptr_t)tid_SEXP:
        TEXTWRITER(pwriter)->_separator_character = ' ';
        break;
    case (intptr_t)tid_LIST:
    case (intptr_t)tid_STRUCT:
        TEXTWRITER(pwriter)->_separator_character = ',';
        break;
    default:
        TEXTWRITER(pwriter)->_separator_character = ION_TEXT_WRITER_IS_PRETTY() ? '\n' : ' ';
    break;
    }
    TEXTWRITER(pwriter)->_top++;

    iRETURN;
}

iERR _ion_writer_text_pop(ION_WRITER *pwriter, ION_TYPE *ptype)
{
    iENTER;
    ION_TYPE parenttype, type;

    if (!TEXTWRITER(pwriter)->_top) FAILWITH(IERR_INVALID_STATE);

    TEXTWRITER(pwriter)->_top--;
    type = TEXTWRITER(pwriter)->_stack_parent_type[TEXTWRITER(pwriter)->_top];  // popped parent

    parenttype = (TEXTWRITER(pwriter)->_top > 0) ? TEXTWRITER(pwriter)->_stack_parent_type[TEXTWRITER(pwriter)->_top - 1] : tid_SEXP;
    switch ((intptr_t)parenttype) {
    case (intptr_t)tid_SEXP:
        TEXTWRITER(pwriter)->_separator_character = ' ';
        break;
    case (intptr_t)tid_LIST:
    case (intptr_t)tid_STRUCT:
        TEXTWRITER(pwriter)->_separator_character = ',';
        break;
    default:
        TEXTWRITER(pwriter)->_separator_character = (ION_TEXT_WRITER_IS_PRETTY()) ? '\n' : ' ';
        break;
    }

    *ptype = type;

    iRETURN;
}

iERR _ion_writer_text_top_type(ION_WRITER *pwriter, ION_TYPE *ptype)
{
    iENTER;
    int top_idx;

    if (!TEXTWRITER(pwriter)->_top) FAILWITH(IERR_INVALID_STATE);

    top_idx = TEXTWRITER(pwriter)->_top - 1;
    *ptype = TEXTWRITER(pwriter)->_stack_parent_type[top_idx];  // the *not* popped parent

    iRETURN;
}

iERR _ion_writer_text_print_leading_white_space(ION_WRITER *pwriter)
{
    iENTER;
    int ii;

    for (ii=0; ii<TEXTWRITER(pwriter)->_top; ii++) {
        ION_TEXT_WRITER_APPEND_CHAR(' ');
        ION_TEXT_WRITER_APPEND_CHAR(' ');
    }

    iRETURN;
}

iERR _ion_writer_text_close_collection(ION_WRITER *pwriter, BYTE close_char)
{
    iENTER;

    if (ION_TEXT_WRITER_IS_PRETTY()) {
        ION_TEXT_WRITER_APPEND_EOL();
       _ion_writer_text_print_leading_white_space(pwriter);
    }
    ION_TEXT_WRITER_APPEND_CHAR(close_char);

    iRETURN;
}

iERR _ion_writer_text_start_value(ION_WRITER *pwriter)
{
    iENTER;
    ION_STRING str;
    int ii, count;

    if (TEXTWRITER(pwriter)->_pending_blob_bytes > 0) {
        // you can't start a value if you have left
        // over blob bytes that haven't be dealt with
        // i.e. the user forgot to call finish_lob
        FAILWITH(IERR_INVALID_STATE);
    }

    if (ION_TEXT_WRITER_IS_PRETTY()) {
        if (TEXTWRITER(pwriter)->_pending_separator) {
            switch (TEXTWRITER(pwriter)->_separator_character) {
            case 0:
            case ' ':
            case '\n':
                // no need to write a whitespace separator if 
                // we're going to follow it with a new line anyway
                break;
            default:
                ION_TEXT_WRITER_APPEND_CHAR((BYTE)TEXTWRITER(pwriter)->_separator_character);
                break;
            }
        }
        if (!TEXTWRITER(pwriter)->_no_output) {
            ION_TEXT_WRITER_APPEND_EOL();
        }
        IONCHECK(_ion_writer_text_print_leading_white_space(pwriter));
    }
    else if (TEXTWRITER(pwriter)->_pending_separator) {
        ION_TEXT_WRITER_APPEND_CHAR((BYTE)TEXTWRITER(pwriter)->_separator_character);
    }

    if (TEXTWRITER(pwriter)->_no_output) {
        TEXTWRITER(pwriter)->_no_output = FALSE; // from this point on we aren't fresh
        // TODO: should we emit a $ion_1_0 at this point?
    }

    // write field name
    if (pwriter->_in_struct) {
        IONCHECK(_ion_writer_get_field_name_as_string_helper(pwriter, &str));
        IONCHECK(_ion_writer_text_append_symbol_string(pwriter->output, &str, pwriter->options.escape_all_non_ascii));
        ION_TEXT_WRITER_APPEND_CHAR(':');
        IONCHECK(_ion_writer_clear_field_name_helper(pwriter));
    }

    // write annotations
    count = pwriter->annotation_curr;
    if (count > 0) {
        for (ii=0; ii<count; ii++) {
            IONCHECK(_ion_writer_get_annotation_as_string_helper(pwriter, ii, &str));
            IONCHECK(_ion_writer_text_append_symbol_string(pwriter->output, &str, pwriter->options.escape_all_non_ascii));
            ION_TEXT_WRITER_APPEND_CHAR(':');
            ION_TEXT_WRITER_APPEND_CHAR(':');
        }
        IONCHECK(_ion_writer_clear_annotations_helper(pwriter));
    }

    iRETURN;
}

iERR _ion_writer_text_close_value(ION_WRITER *pwriter)
{
    iENTER;

    if (pwriter->options.flush_every_value) {
        IONCHECK(ion_stream_flush(pwriter->output));
    }
    TEXTWRITER(pwriter)->_pending_separator = TRUE;

    iRETURN;
}


iERR _ion_writer_text_write_null(ION_WRITER *pwriter)
{
    iENTER;

    IONCHECK(_ion_writer_text_start_value(pwriter));

    //_output.append("null");
    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "null"));

    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_write_typed_null(ION_WRITER *pwriter, ION_TYPE type)
{
    iENTER;
    char *image = NULL;

    IONCHECK(_ion_writer_text_start_value(pwriter));

    switch ((intptr_t)type) {
    case (intptr_t)tid_NULL:      image = "null";           break;
    case (intptr_t)tid_BOOL:      image = "null.bool";      break;
    case (intptr_t)tid_INT:       image = "null.int";       break;
    case (intptr_t)tid_FLOAT:     image = "null.float";     break;
    case (intptr_t)tid_DECIMAL:   image = "null.decimal";   break;
    case (intptr_t)tid_TIMESTAMP: image = "null.timestamp"; break;
    case (intptr_t)tid_SYMBOL:    image = "null.symbol";    break;
    case (intptr_t)tid_STRING:    image = "null.string";    break;
    case (intptr_t)tid_BLOB:      image = "null.blob";      break;
    case (intptr_t)tid_CLOB:      image = "null.clob";      break;
    case (intptr_t)tid_SEXP:      image = "null.sexp";      break;
    case (intptr_t)tid_LIST:      image = "null.list";      break;
    case (intptr_t)tid_STRUCT:    image = "null.struct";    break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    // _output.append(nullimage);
    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));

    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_write_bool(ION_WRITER *pwriter, BOOL value)
{
    iENTER;
    char *image;

    IONCHECK(_ion_writer_text_start_value(pwriter));

    image = value ? "true" : "false";
    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));
    
    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_write_int64(ION_WRITER *pwriter, int64_t value)
{
    iENTER;
    char int_image[MAX_INT64_LENGTH + 1], *cp;  // +1 for null terminator
    int  is_negative = FALSE, digit;
    int64_t next;

    IONCHECK(_ion_writer_text_start_value(pwriter));

    // we'll be writting the sign at the beginning, so just
    // save it off for the moment
    if (value < 0) {
        is_negative = TRUE;
    }

    // we'll be writing the digits backwards, so we first null
    // terminate our output buffer
    cp = int_image + MAX_INT64_LENGTH;
    *cp = 0;
    cp--;

    if (!value) {
        // just dodge the edge case with leading zeros
        *cp = '0';
        cp--;
    }
    else {
        // this isn't the most efficient but is should be right
        while (value) {
            next = value / 10;
            digit = value % 10;
            if (is_negative) {
                digit = -digit;
            }
            *cp = (char)digit + '0';
            cp--;
            value = next;
        }

        // only non-zero values can be negative
        if (is_negative) {
            ION_TEXT_WRITER_APPEND_CHAR('-');
        }
    }

    cp++;  // we always back up one too far

    // now write the chars out in the right order
    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, cp));

    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_write_ion_int(ION_WRITER *pwriter, ION_INT *iint)
{
    iENTER;
    char      int_image_local_buffer[LOCAL_INT_CHAR_BUFFER_LENGTH + 1];  // +1 for null terminator
    char     *int_image = &int_image_local_buffer[0];
    char     *cp, *end;
    int       is_negative = FALSE, decimal_digits;
    II_DIGIT  small_copy[II_SMALL_DIGIT_ARRAY_LENGTH];
    II_DIGIT *digits = NULL, remainder;
    SIZE    digits_length;


    IONCHECK(_ion_writer_text_start_value(pwriter));

    decimal_digits = _ion_int_get_char_len_helper(iint);
    if (iint->_signum < 0) decimal_digits++; // TODO duplicated in previous call? Check
    if (decimal_digits < LOCAL_INT_CHAR_BUFFER_LENGTH) {
        end = &int_image_local_buffer[LOCAL_INT_CHAR_BUFFER_LENGTH];
    }
    else {
        int_image = ion_xalloc(decimal_digits + 2);
        end = int_image + decimal_digits + 1;
    }

    // we'll be writing the digits backwards, so we first null
    // terminate our output buffer
    cp = end;
    *cp = 0;
    cp--;

    digits_length = iint->_len;
    // don't mutate the user's data.
    digits = _ion_int_buffer_temp_copy(iint->_digits, digits_length, small_copy, II_SMALL_DIGIT_ARRAY_LENGTH);
    if (digits == NULL) {
        FAILWITH(IERR_NO_MEMORY);
    }
    if (_ion_int_is_zero_bytes(digits, digits_length)) {
        // just dodge the edge case with leading zeros
        *cp = '0';
        cp--;
    }
    else {
        // this isn't the most efficient but is should be right

        for (;;) {
            if (_ion_int_is_zero_bytes(digits, digits_length)) break;
            IONCHECK(_ion_int_divide_by_digit(digits, digits_length, II_STRING_BASE, &remainder));
            ASSERT(remainder >= 0 && remainder <= 9);
            *cp = (BYTE)((remainder & 0xff)+'0');
            cp--;
        }

        // only non-zero values can be negative
        if (iint->_signum < 0) {
            ION_TEXT_WRITER_APPEND_CHAR('-');
        }
    }

    cp++;  // we always back up one too far

    // now write the chars out in the right order
    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, cp));

    IONCHECK(_ion_writer_text_close_value(pwriter));

fail:
    if (int_image != &int_image_local_buffer[0]) {
        ion_xfree(int_image);
    }
    _ion_int_free_temp(digits, small_copy);
    RETURN(__file__, __line__, __count__, err);
}

iERR _ion_writer_text_write_double(ION_WRITER *pwriter, double value)
{
    iENTER;
    char image[64], *mark;
    int  fpc;

    IONCHECK(_ion_writer_text_start_value(pwriter));

    fpc = FLOAT_CLASS(value);
    switch(fpc) {
#if defined(_MSC_VER)
    case _FPCLASS_SNAN:   /* signaling NaN     0x0001  */
    case _FPCLASS_QNAN:   /* quiet NaN         0x0002  */
#elif defined(__GNUC__)
    case FP_NAN:
#endif
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "nan"));
        break;
#if defined(_MSC_VER)
    case _FPCLASS_PINF:   /* positive infinity 0x0200  */
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "+inf"));
        break;
    case _FPCLASS_NINF:   /* negative infinity 0x0004  */
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "-inf"));
        break;
    case _FPCLASS_PZ:     /* +0                0x0040  */
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "0e0"));
        break;
    case _FPCLASS_NZ:     /* -0                0x0020  */
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "-0e0"));
        break;
#elif defined(__GNUC__)
    case FP_INFINITE:
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, value == INFINITY ? "+inf" : "-inf"));
        break;
    case FP_ZERO:
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, signbit(value) ? "-0e0" : "0e0"));
        break;
#endif
#if defined(_MSC_VER)
    case _FPCLASS_NN:     /* negative normal   0x0008  */
    case _FPCLASS_ND:     /* negative denormal 0x0010  */
    case _FPCLASS_PD:     /* positive denormal 0x0080  */
    case _FPCLASS_PN:     /* positive normal   0x0100  */
#elif defined(__GNUC__)
    case FP_NORMAL:
    case FP_SUBNORMAL:
#endif

        // TODO this is a terrible way to convert this!
        sprintf(image, "%20g", value);
        assert(strlen(image) < sizeof(image));
        mark = strchr(image, 'e');
        if (!mark) {
            strcat(image, "e+0");
        }
        for (mark = image; *mark == ' '; mark++) ; // strip leading spaces
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, mark));    
        break;

    default:
        FAILWITH(IERR_UNRECOGNIZED_FLOAT);
    }

    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_write_decimal(ION_WRITER *pwriter, decQuad *value)
{
    iENTER;
    char image[DECQUAD_String];
    char *dec, *exp, *start;
    BOOL is_negative;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    IONCHECK(_ion_writer_text_start_value(pwriter));

    // if dec the pointer is null, that's a null value lou
    if (value == NULL) {
        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_DECIMAL));
        SUCCEED();
    }

    is_negative = decQuadIsSigned(value);
    if (decQuadIsInfinite(value)) {
        start = is_negative ? "-inf" : "+inf";
    }
    else if (decQuadIsNaN(value)) {
        start = "nan";
    }
    else if (decQuadIsZero(value) && !is_negative && decQuadDigits(value) != 1) {
        start = "0d0";
    }
    else {
        start = decQuadToString(value, image);
        exp = strchr(image, 'E');
        dec = strchr(image, '.');
        if (exp) *exp = 'd';
        if (!dec && !exp) {
            strcat(image, "d+0");
        }
    }

    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, start));
    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_write_timestamp(ION_WRITER *pwriter, iTIMESTAMP value)
{
    iENTER;
    char temp[ION_TIMESTAMP_STRING_LENGTH + 1];
    SIZE output_length;

    ASSERT(pwriter);

    if (!value) {
        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_TIMESTAMP));
    }
    else {
        IONCHECK(_ion_writer_text_start_value(pwriter));

        // the timestamp utility routine does most of the work
        IONCHECK(ion_timestamp_to_string(value, temp, (SIZE)sizeof(temp), &output_length, &pwriter->deccontext));
        temp[output_length] = '\0';

        // and our helper does the rest
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, temp));
        IONCHECK(_ion_writer_text_close_value(pwriter));
    }

    iRETURN;
}

iERR _ion_writer_text_write_symbol_id(ION_WRITER *pwriter, SID sid)
{
    iENTER;
    ION_STRING       *pstr;
    ION_SYMBOL_TABLE *symtab;
    ION_STREAM       *poutput;
    SIZE              written;

    ASSERT(pwriter);

    // bad, very bad: ION_STRING_INIT(&pstr);

    // if they passed us a reasonable sid we'll look it up (otherwise str will still be null)
    if (sid != UNKNOWN_SID) {
        IONCHECK(_ion_writer_get_symbol_table_helper(pwriter, &symtab));
        IONCHECK(_ion_symbol_table_find_by_sid_helper(symtab, sid, &pstr));
    }

    if (!pstr || !pstr->value) {
        // no buffer, this is a null string, so that's what we'll output
        if (pstr->length != 0) FAILWITH(IERR_INVALID_ARG);
        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_SYMBOL));
    }
    else {
        ASSERT(pstr->length >= 0);
        poutput = pwriter->output;

        IONCHECK(_ion_writer_text_start_value(pwriter));

        // write the symbol with, or without, quotes (and escaping) as appropriate
        if (_ion_symbol_needs_quotes_string(pstr)) {
            ION_PUT(poutput, '\'');
            if (pwriter->options.escape_all_non_ascii) {
                IONCHECK(_ion_writer_text_append_escaped_string(poutput, pstr, '"'));
            }
            else {
                IONCHECK(_ion_writer_text_append_escaped_string_utf8(poutput, pstr, '"'));
            }
            ION_PUT(poutput, '\'');
        }
        else {
            // no quotes means no escapes means we get to write the bytes out as is
            IONCHECK(ion_stream_write(poutput, pstr->value, pstr->length, &written));
            if (written != pstr->length) FAILWITH(IERR_WRITE_ERROR);
        }
        IONCHECK(_ion_writer_text_close_value(pwriter));
    }

    iRETURN;
}

iERR _ion_writer_text_write_symbol(ION_WRITER *pwriter, iSTRING pstr)
{
    iENTER;
    ION_STREAM *poutput;
    SIZE        written;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    if (!pstr || !pstr->value) {
        // no buffer, this is a null symbol, so that's what we'll output
        if (pstr->length != 0) FAILWITH(IERR_INVALID_ARG);

        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_SYMBOL));
    }
    else {
        if (pstr->length < 0) FAILWITH(IERR_INVALID_ARG);
        poutput = pwriter->output;

        IONCHECK(_ion_writer_text_start_value(pwriter));

        if (_ion_symbol_needs_quotes_string(pstr)) {
            ION_PUT(poutput, '\'');
            if (pwriter->options.escape_all_non_ascii) {
                IONCHECK(_ion_writer_text_append_escaped_string(poutput, pstr, '\''));
            }
            else {
                IONCHECK(_ion_writer_text_append_escaped_string_utf8(poutput, pstr, '\''));
            }
            ION_PUT(poutput, '\'');
        }
        else {
            IONCHECK(ion_stream_write(poutput, pstr->value, pstr->length, &written));
            if (written != pstr->length) FAILWITH(IERR_WRITE_ERROR);
        }

        IONCHECK(_ion_writer_text_close_value(pwriter));
    }

    iRETURN;
}

iERR _ion_writer_text_write_string(ION_WRITER *pwriter, iSTRING str)
{
    iENTER;
    ION_STREAM *poutput;

    if (ION_STRING_IS_NULL(str)) {
        // no buffer, this is a null string, so that's what we'll output
        if (str->length != 0) FAILWITH(IERR_INVALID_ARG);

        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_STRING));
    }
    else {
        if (str->length < 0) FAILWITH(IERR_INVALID_ARG);
        poutput = pwriter->output;

        IONCHECK(_ion_writer_text_start_value(pwriter));

        ION_PUT(poutput, '\"');
        if (pwriter->options.escape_all_non_ascii) {
            IONCHECK(_ion_writer_text_append_escaped_string(poutput, str, '"'));
        }
        else {
            IONCHECK(_ion_writer_text_append_escaped_string_utf8(poutput, str, '"'));
        }
        ION_PUT(poutput, '\"');

        IONCHECK(_ion_writer_text_close_value(pwriter));
    }

    iRETURN;
}

iERR _ion_writer_text_write_clob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);
    if (length < 0) FAILWITH(IERR_INVALID_ARG);

    if (!p_buf) {
        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_CLOB));
    }
    else {
        IONCHECK(_ion_writer_text_start_value(pwriter));

        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "{{\""));
        IONCHECK(_ion_writer_text_append_clob_contents(pwriter, p_buf, length));
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "\"}}"));
        
        IONCHECK(_ion_writer_text_close_value(pwriter));
    }
    iRETURN;
}

iERR _ion_writer_text_append_clob_contents(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    int ii;
    char c, *image;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);
    if (!p_buf) FAILWITH(IERR_INVALID_ARG);     // this is append - don't call it will a null buffer
    if (length < 0) FAILWITH(IERR_INVALID_ARG);

     for (ii=0; ii<length; ii++) {
        c = p_buf[ii];
        if (ION_WRITER_NEEDS_ESCAPE_ASCII(c)) {
            image = _ion_writer_get_control_escape_string(c);
            IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));
        }
        else if (c == '"') {
            ION_PUT(pwriter->output, '\\');
            ION_PUT(pwriter->output, c);
        }
        else {
            ION_PUT(pwriter->output, c);
        }
    }

    iRETURN;
}

iERR _ion_writer_text_write_blob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);
    if (length < 0) FAILWITH(IERR_INVALID_ARG);

    if (!p_buf) {
        IONCHECK(_ion_writer_text_write_typed_null(pwriter, tid_BLOB));
    }
    else {

        IONCHECK(_ion_writer_text_start_value(pwriter));

        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "{{"));
        IONCHECK(_ion_writer_text_append_blob_contents(pwriter, p_buf, length));
        IONCHECK(_ion_writer_text_close_blob_contents(pwriter));
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, "}}"));

        IONCHECK(_ion_writer_text_close_value(pwriter));
    }
    iRETURN;
}

iERR _ion_writer_text_append_blob_contents(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    char image[5];
    int  triple;

    ASSERT(pwriter);
    ASSERT(p_buf);
    ASSERT(length >= 0);

    // we may have some blob contents waiting to close out since
    // the caller shouldn't really be obliged to pass in buffers
    // in sizes that are even multiples 3 bytes <sigh>

    // blob are base 64 encoded - yuck 
    // that's 6 bits in each output character
    //        8 bits from each original byte
    // 2*3, 2*2*2, 4*6 = 3*8 or ... 4 output chars for every 3 input bytes

    // The Input: leasure.   Encodes to bGVhc3VyZS4=
    // The Input: easure.    Encodes to ZWFzdXJlLg==
    // The Input: asure.     Encodes to YXN1cmUu
    // The Input: sure.      Encodes to c3VyZS4=

    // The Input: lea sur e.   bGVh c3Vy ZS4=
    // The Input: eas ure .    ZWFz dXJl Lg==
    // The Input: asu re.      YXN1 cmUu
    // The Input: sur e.       c3Vy ZS4=

    // append new bytes to any pending bytes
    if (TEXTWRITER(pwriter)->_pending_blob_bytes > 0) {
        triple = TEXTWRITER(pwriter)->_pending_triple;
        while (TEXTWRITER(pwriter)->_pending_blob_bytes < 3 && length > 0) {
            triple <<= 8;
            triple |= *p_buf++;
            length--;
            TEXTWRITER(pwriter)->_pending_blob_bytes++;
        }
        if (TEXTWRITER(pwriter)->_pending_blob_bytes < 3) {
            // if we still didn't get up to 3 bytes stored
            // we'll just have to hope the user calls us
            // with some more data in due course
            SUCCEED();
        }
        // but it managed to fill out the pending triple, let's write it out
        _ion_writer_text_write_blob_make_base64_image(triple, image);
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));
        TEXTWRITER(pwriter)->_pending_blob_bytes = 0; // and, for the moment, nothings pending
    }

    // output any whole triplets we can
    while (length > 2) {
        triple = *p_buf++;
        triple <<= 8;
        triple |= *p_buf++;
        triple <<= 8;
        triple |= *p_buf++;
        _ion_writer_text_write_blob_make_base64_image(triple, image);
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));
        length -= 3;
    }

    // remember the tail, whatever that turns out to be - someone
    // will have to call _ion_writer_text_close_blob_contents to
    // write the tail out (if there is any)
    TEXTWRITER(pwriter)->_pending_blob_bytes = length;
    switch(length) {
    case 0:
        triple = 0;
        break;
    case 1:
        // The Input: eas ure .    ZWFz dXJl Lg==
        triple = *p_buf++;
        break;
    case 2:
        // The Input: lea sur e.   bGVh c3Vy ZS4=
        triple = *p_buf++;
        triple <<= 8;
        triple |= *p_buf++;
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);  // wtf? never is the answer ... but just in case
    }
    TEXTWRITER(pwriter)->_pending_triple = triple;

    iRETURN;
}

iERR _ion_writer_text_close_blob_contents(ION_WRITER *pwriter)
{
    iENTER;
    char image[5];
    int  triple, length;

    ASSERT(pwriter);

    length = TEXTWRITER(pwriter)->_pending_blob_bytes;
    triple = TEXTWRITER(pwriter)->_pending_triple;
    switch(length) {
    case 0:
        break;
    case 1:
        // The Input: eas ure .    ZWFz dXJl Lg==
        triple <<= 16;        // we're pretending we're null padded
        _ion_writer_text_write_blob_make_base64_image(triple, image);
        image[2] = ION_BASE64_TRAILING_CHAR;
        image[3] = ION_BASE64_TRAILING_CHAR;
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));
        break;
    case 2:
        // The Input: lea sur e.   bGVh c3Vy ZS4=
        triple <<= 8;
        _ion_writer_text_write_blob_make_base64_image(triple, image);
        image[3] = ION_BASE64_TRAILING_CHAR;
        IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, image));
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }

    TEXTWRITER(pwriter)->_pending_blob_bytes = 0; // we cleared them out, so remember that

    iRETURN;
}

iERR _ion_writer_text_start_lob(ION_WRITER *pwriter, ION_TYPE lob_type)
{
    iENTER;
    char *open_str = NULL;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    switch ((intptr_t)lob_type) {
    case (intptr_t)tid_BLOB:  open_str = "{{";    break;
    case (intptr_t)tid_CLOB:  open_str = "{{\"";  break;
    default:        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_writer_text_start_value(pwriter));
    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, open_str));
    IONCHECK(_ion_writer_text_push(pwriter, lob_type));

    iRETURN;
}

iERR _ion_writer_text_append_lob(ION_WRITER *pwriter, BYTE *p_buf, SIZE length)
{
    iENTER;
    ION_TYPE lob_type;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    IONCHECK(_ion_writer_text_top_type(pwriter, &lob_type));

    switch((intptr_t)lob_type) {
    case (intptr_t)tid_BLOB:
        IONCHECK(_ion_writer_text_append_blob_contents(pwriter, p_buf, length));
        break;
    case (intptr_t)tid_CLOB:
        IONCHECK(_ion_writer_text_append_clob_contents(pwriter, p_buf, length));
        break;
    default:
        FAILWITH(IERR_INVALID_STATE);
    }


    iRETURN;
}

iERR _ion_writer_text_finish_lob(ION_WRITER *pwriter)
{
    iENTER;
    ION_TYPE lob_type;
    char *close_str = NULL;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    IONCHECK(_ion_writer_text_pop(pwriter, &lob_type));

    switch ((intptr_t)lob_type) {
    case (intptr_t)tid_BLOB:  close_str = "}}";   break;
    case (intptr_t)tid_CLOB:  close_str = "\"}}"; break;
    default:        FAILWITH(IERR_INVALID_ARG);
    }

    switch ((intptr_t)lob_type) {
    case (intptr_t)tid_CLOB:
        // CLOBs are neatly self contained, so there's not "dangling" hassle here
        break;
    case (intptr_t)tid_BLOB:
        // BLOBs ... well not so much ... clear out any partial triple
        // output the tail, whatever that turns out to be
        IONCHECK(_ion_writer_text_close_blob_contents(pwriter));
        break;    
    default:
        FAILWITH(IERR_INVALID_ARG);
    }

    IONCHECK(_ion_writer_text_append_ascii_cstr(pwriter->output, close_str));
    IONCHECK(_ion_writer_text_close_value(pwriter));

    iRETURN;
}

iERR _ion_writer_text_start_container(ION_WRITER *pwriter, ION_TYPE container_type)
{
    iENTER;
    int open_char;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    IONCHECK(_ion_writer_text_start_value(pwriter));
    pwriter->_in_struct = (container_type == tid_STRUCT);
    IONCHECK(_ion_writer_text_push(pwriter, container_type));
    switch ((intptr_t)container_type) {
    case (intptr_t)tid_STRUCT:    open_char = '{';    break;
    case (intptr_t)tid_LIST:      open_char = '[';    break;
    case (intptr_t)tid_SEXP:      open_char = '(';    break;
    default:                      FAILWITH(IERR_INVALID_ARG);
    }
    ION_TEXT_WRITER_APPEND_CHAR(open_char);
    TEXTWRITER(pwriter)->_pending_separator = FALSE;

    if (pwriter->options.flush_every_value) {
        IONCHECK(ion_stream_flush(pwriter->output));
    }

    iRETURN;
}

iERR _ion_writer_text_finish_container(ION_WRITER *pwriter)
{
    iENTER;
    ION_TYPE container_type;
    int close_char;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    TEXTWRITER(pwriter)->_pending_separator = ION_TEXT_WRITER_TOP_PENDING_COMMA();
    IONCHECK(_ion_writer_text_pop(pwriter, &container_type));

    switch ((intptr_t)container_type) {
    case (intptr_t)tid_STRUCT:    close_char = '}';   break;
    case (intptr_t)tid_LIST:      close_char = ']';   break;
    case (intptr_t)tid_SEXP:      close_char = ')';   break;
    default:                      FAILWITH(IERR_INVALID_ARG);
    }
    IONCHECK(_ion_writer_text_close_collection(pwriter, close_char));
    IONCHECK(_ion_writer_text_close_value(pwriter));
    pwriter->_in_struct = ION_TEXT_WRITER_TOP_IN_STRUCT();

    iRETURN;
}

iERR _ion_writer_text_close(ION_WRITER *pwriter)
{
    iENTER;

    if (!pwriter) FAILWITH(IERR_BAD_HANDLE);

    if (pwriter->options.pretty_print) {
        ION_PUT(pwriter->output, '\n');
    }

    IONCHECK(ion_stream_flush(pwriter->output));

    iRETURN;
}

iERR _ion_writer_text_append_symbol_string(ION_STREAM *poutput, ION_STRING *p_str, BOOL as_ascii)
{
    iENTER;
    SIZE written;

    if (!poutput) FAILWITH(IERR_BAD_HANDLE);
    if (!p_str) FAILWITH(IERR_INVALID_ARG);
    if (p_str->length < 0) FAILWITH(IERR_INVALID_ARG);

    if (_ion_symbol_needs_quotes_string(p_str)) {
        ION_PUT(poutput, '\'');
        if (as_ascii) {
            IONCHECK(_ion_writer_text_append_escaped_string(poutput, p_str, '\''));
        }
        else {
            IONCHECK(_ion_writer_text_append_escaped_string_utf8(poutput, p_str, '\''));
        }
        ION_PUT(poutput, '\'');
    }
    else {
        IONCHECK(ion_stream_write(poutput, p_str->value, p_str->length, &written ));
        if (written != p_str->length) FAILWITH(IERR_WRITE_ERROR);
    }

    iRETURN;
}

iERR _ion_writer_text_append_ascii_cstr(ION_STREAM *poutput, char *cp)
{
    iENTER;

    if (!poutput) FAILWITH(IERR_BAD_HANDLE);
    if (!cp) SUCCEED();

    while (*cp) {
        if (*cp > 127) FAILWITH(IERR_INVALID_ARG);
        ION_PUT(poutput, *cp);
        cp++;
    }

    iRETURN;
}

iERR _ion_writer_text_append_escape_sequence_string(ION_STREAM *poutput, BYTE *cp, BYTE *limit, BYTE **p_next)
{
    iENTER;
    char   unicode_buffer[4];  // unicode byte sequences are less than 4 bytes long
    char  *image;
    SIZE len, ii;
    int    c, unicode_scalar, ilen;

    c = *cp;
    if (c < 32 || c == '\\' || c == '"') {
        image = _ion_writer_get_control_escape_string(c);
        IONCHECK(_ion_writer_text_append_ascii_cstr(poutput, image));
        cp++;
    }
    else {
        len = (SIZE)(limit - cp);
        if (len > 4) len = 4;
        for (ii=0; ii<len; ii++) {
            unicode_buffer[ii] = cp[ii];
        }
        IONCHECK(_ion_writer_text_read_unicode_scalar(unicode_buffer, &ilen, &unicode_scalar));
        len = ilen;
        IONCHECK(_ion_writer_text_append_unicode_scalar(poutput, unicode_scalar));
        cp += len;
    }

    // update the pointer so the caller can read the next character
    *p_next = cp;
    iRETURN;
}

iERR _ion_writer_text_append_escape_sequence_cstr_limit(ION_STREAM *poutput, char *cp, char *limit, char **p_next)
{
    iENTER;
    char  *image;
    char   temp_buffer[4];
    int    unicode_scalar, ilen;
    SIZE len;

    if (*cp < 32) {
        image = _ion_writer_get_control_escape_string(*cp);
        IONCHECK(_ion_writer_text_append_ascii_cstr(poutput, image));
        cp++;
    }
    else {
        len = (SIZE)(limit - cp);
        if (len > 4) len = 4;
        strncpy(temp_buffer, cp, len);
        IONCHECK(_ion_writer_text_read_unicode_scalar(temp_buffer, &ilen, &unicode_scalar));
        len = ilen;
        IONCHECK(_ion_writer_text_append_unicode_scalar(poutput, unicode_scalar));
        cp += len;
    }

    // update the pointer so the caller can read the next character
    *p_next = cp;
    iRETURN;
}

iERR _ion_writer_text_append_escape_sequence_cstr(ION_STREAM *poutput, char *cp, char **p_next)
{
    iENTER;
    char *image;
    int   unicode_scalar, len;

    if (*cp < 32) {
        image = _ion_writer_get_control_escape_string(*cp);
        IONCHECK(_ion_writer_text_append_ascii_cstr(poutput, image));
        cp++;
    }
    else {
        IONCHECK(_ion_writer_text_read_unicode_scalar(cp, &len, &unicode_scalar));
        IONCHECK(_ion_writer_text_append_unicode_scalar(poutput, unicode_scalar));
        cp += len;
    }

    // update the pointer so the caller can read the next character
    *p_next = cp;
    iRETURN;
}

iERR _ion_writer_text_append_escaped_string_utf8(ION_STREAM *poutput, ION_STRING *p_str, char quote_char)
{
    iENTER;
    BYTE *cp, *limit;

    if (!poutput) FAILWITH(IERR_BAD_HANDLE);
    if (!p_str) FAILWITH(IERR_INVALID_ARG);
    if (p_str->length < 0) FAILWITH(IERR_INVALID_ARG);
    if (p_str->length == 0) SUCCEED();

    cp = p_str->value;
    limit = cp + p_str->length;

    while (cp < limit) {
        // this only escapes chars < 32 or slash or quote character (single or double)
        // utf8 sequences have the high bit set and will simply be treated
        // as normal characters and pass through - at this point we don't
        // validate that the sequences are valid
        if (ION_WRITER_NEEDS_ESCAPE_UTF8(*cp) || (*cp == quote_char)) {
            IONCHECK(_ion_writer_text_append_escape_sequence_string(poutput, cp, limit, &cp));
        }
        else {
            ION_PUT(poutput, *cp);
            cp++;
        }
    }

    iRETURN;
}

iERR _ion_writer_text_append_escaped_string(ION_STREAM *poutput, ION_STRING *p_str, char quote_char)
{
    iENTER;
    BYTE *cp, *limit;

    if (!poutput) FAILWITH(IERR_BAD_HANDLE);
    if (!p_str) FAILWITH(IERR_INVALID_ARG);
    if (p_str->length < 0) FAILWITH(IERR_INVALID_ARG);
    if (p_str->length == 0) SUCCEED();

    cp = p_str->value;
    limit = cp + p_str->length;

    while (cp < limit) {
        // this escapes <32, slash, double quotes AND utf8 sequences
        if (ION_WRITER_NEEDS_ESCAPE_ASCII(*cp) || *cp == quote_char) {
            IONCHECK(_ion_writer_text_append_escape_sequence_string(poutput, cp, limit, &cp));
        }
        else {
            ION_PUT(poutput, *cp);
            cp++;
        }
    }

    iRETURN;
}

iERR _ion_writer_text_append_unicode_scalar(ION_STREAM *poutput, int unicode_scalar)
{
    iENTER;

    if (unicode_scalar < 0) {
        FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
    }
    else if (unicode_scalar < 128) {
        ION_PUT(poutput, (BYTE)unicode_scalar);
    }
    else if (unicode_scalar < 0x256) {\
        // handle with \xXX
        ION_PUT(poutput, '\\');
        ION_PUT(poutput, 'x');
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >>  4) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[( unicode_scalar        & 0xF)]);
    }
    else if (unicode_scalar < 0x10000) {
        // handle with \uXXXX
        ION_PUT(poutput, '\\');
        ION_PUT(poutput, 'u');
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >> 12) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >>  8) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >>  4) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[( unicode_scalar        & 0xF)]);
    }
    else if (unicode_scalar <= 0x10FFFF) {
        ION_PUT(poutput, '\\');
        ION_PUT(poutput, 'U');
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >> 28) & 0xF)]);  // should be 0
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >> 24) & 0xF)]);  // also should be 0
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >> 20) & 0xF)]);  // either 0 or 1
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >> 16) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >> 12) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >>  8) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[((unicode_scalar >>  4) & 0xF)]);
        ION_PUT(poutput, _ion_hex_chars[( unicode_scalar        & 0xF)]);
    }
    else {
        FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
    }

    iRETURN;
}

iERR _ion_writer_text_read_unicode_scalar(char *cp, int *p_chars_read, int *p_unicode_scalar)
{
    iENTER;
    int32_t c = 0;
    int     b = CHAR2INT(*cp++);  // makes porting the java easier

    ASSERT( cp != NULL && p_chars_read != NULL && p_unicode_scalar != NULL );
    *p_chars_read = -1;
    *p_unicode_scalar = -1;

    // ascii is all good, even -1 (eof)
    if (b < 0x80) {
        c = b;
        *p_chars_read = 1;
    }
    // now we start gluing the multi-byte value together
    else if ((b & 0xe0) == 0xc0) {
        // for values from 0x80 to 0x7FF (all legal)
        c = (b & ~0xe0);
        b = CHAR2INT(*cp++);
        if ((b & 0xc0) != 0x80) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        c <<= 6;
        c |= (b & ~0x80);
        *p_chars_read = 2;
    }
    else if ((b & 0xf0) == 0xe0) {
        // for values from 0x800 to 0xFFFFF (NOT all legal)
        c = (b & ~0xf0);
        b = CHAR2INT(*cp++);
        if ((b & 0xc0) != 0x80) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        c <<= 6;
        c |= (b & ~0x80);
        b = CHAR2INT(*cp++);
        if ((b & 0xc0) != 0x80) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        c <<= 6;
        c |= (b & ~0x80);
        if (c > 0x00D7FF && c < 0x00E000) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        *p_chars_read = 3;
    }
    else if ((b & 0xf8) == 0xf0) {
        // for values from 0x010000 to 0x1FFFFF (NOT all legal)
        c = (b & ~0xf8);
        b = CHAR2INT(*cp++);
        if ((b & 0xc0) != 0x80) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        c <<= 6;
        c |= (b & ~0x80);
        b = CHAR2INT(*cp++);
        if ((b & 0xc0) != 0x80) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        c <<= 6;
        c |= (b & ~0x80);
        b = CHAR2INT(*cp++);
        if ((b & 0xc0) != 0x80) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        c <<= 6;
        c |= (b & ~0x80);
        if (c > 0x10FFFF) FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);
        *p_chars_read = 4;        
    }
    else {
        FAILWITH(IERR_INVALID_UNICODE_SEQUENCE);;
    }

    // we've got a good character here
    *p_unicode_scalar = c;

    iRETURN;
}
