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

/**@file */


#ifdef ERROR_CODE

/*  IERR_EOF is the short form of IERR_EOF */

    ERROR_CODE( IERR_OK,                         0 )
    ERROR_CODE( IERR_BAD_HANDLE,                 1 )
    ERROR_CODE( IERR_INVALID_ARG,                2 )
    ERROR_CODE( IERR_NO_MEMORY,                  3 )
    /** Unexpected end of stream.
     * E.g. reader reached EOF before closing struct }
     */
    ERROR_CODE( IERR_EOF,                        4 )

    /** Usually caused by invalid application calling sequence.
     * E.g. read_int when get_type returns string.
     */
    ERROR_CODE( IERR_INVALID_STATE,              5 )
    ERROR_CODE( IERR_TOO_MANY_ANNOTATIONS,       6 )
    ERROR_CODE( IERR_UNRECOGNIZED_FLOAT,         7 )

    /** Usually caused by invalid application calling sequence.
     * E.g. read_int when is_null is true.
     */
    ERROR_CODE( IERR_NULL_VALUE,                 8 )
    ERROR_CODE( IERR_BUFFER_TOO_SMALL,           9 )
    ERROR_CODE( IERR_INVALID_TIMESTAMP,         10 )
    ERROR_CODE( IERR_INVALID_UNICODE_SEQUENCE,  12 )
    ERROR_CODE( IERR_UNREAD_LIMIT_EXCEEDED,     13 )
    ERROR_CODE( IERR_INVALID_TOKEN,             14 )
    ERROR_CODE( IERR_INVALID_UTF8,              15 )
    ERROR_CODE( IERR_LOOKAHEAD_OVERFLOW,        16 )
    ERROR_CODE( IERR_BAD_BASE64_BLOB,           17 )
    ERROR_CODE( IERR_TOKEN_TOO_LONG,            18 )
    ERROR_CODE( IERR_INVALID_UTF8_CHAR,         19 )
    ERROR_CODE( IERR_UNEXPECTED_EOF,            20 )
    ERROR_CODE( IERR_INVALID_ESCAPE_SEQUENCE,   21 )

    /** Invalid Ion syntax during parsing Ion text input.
     *
     */
    ERROR_CODE( IERR_INVALID_SYNTAX,            22 )
    ERROR_CODE( IERR_INVALID_TOKEN_CHAR,        23 )
    ERROR_CODE( IERR_INVALID_SYMBOL,            24 )
    ERROR_CODE( IERR_STACK_UNDERFLOW,           25 )
    ERROR_CODE( IERR_INVALID_SYMBOL_LIST,       26 )
    ERROR_CODE( IERR_PARSER_INTERNAL,           27 )
    ERROR_CODE( IERR_INVALID_SYMBOL_TABLE,      28 )
    ERROR_CODE( IERR_IS_IMMUTABLE,              29 )
    ERROR_CODE( IERR_DUPLICATE_SYMBOL,          30 )
    ERROR_CODE( IERR_DUPLICATE_SYMBOL_ID,       31 )
    ERROR_CODE( IERR_NO_SUCH_ELEMENT,           32 )

    ERROR_CODE( IERR_INVALID_FIELDNAME,         33 )

    /** Corrupted binary data (not comforms to Ion spec. */
    ERROR_CODE( IERR_INVALID_BINARY,            34 )
    ERROR_CODE( IERR_IMPORT_NOT_FOUND,          35 )
    ERROR_CODE( IERR_NUMERIC_OVERFLOW,          36 )
    ERROR_CODE( IERR_INVALID_ION_VERSION,       37 )
    ERROR_CODE( IERR_ENTRY_NOT_FOUND,           38 )
    ERROR_CODE( IERR_CANT_FIND_FILE,            39 )
    ERROR_CODE( IERR_STREAM_FAILED,             40 )
    ERROR_CODE( IERR_KEY_ALREADY_EXISTS,        41 )
    ERROR_CODE( IERR_KEY_NOT_FOUND,             42 )
    ERROR_CODE( IERR_KEY_ADDED,                 43 )
    ERROR_CODE( IERR_HAS_LOCAL_SYMBOLS,         44 )
    ERROR_CODE( IERR_NOT_A_SYMBOL_TABLE,        45 )
    ERROR_CODE( IERR_MARK_NOT_SET,              46 )
    ERROR_CODE( IERR_WRITE_ERROR,               47 )
    ERROR_CODE( IERR_SEEK_ERROR,                48 )
    ERROR_CODE( IERR_READ_ERROR,                49 )
    ERROR_CODE( IERR_INTERNAL_ERROR,            50 )

    ERROR_CODE( IERR_NEW_LINE_IN_STRING,        51 )
    ERROR_CODE( IERR_INVALID_LEADING_ZEROS,     52 )
    ERROR_CODE( IERR_INVALID_LOB_TERMINATOR,    53 )


// if it was defined we undefine it now
#undef ERROR_CODE

#endif
