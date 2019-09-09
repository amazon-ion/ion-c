/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/*
 * updated versions of the file stream handling support for
 * the IonC readers and writers.
 * 
 *  fn_fill -  updates the streams buffer, curr, limit and max
 *             fields.  Should return -1 on eof or bytes read
 *             or error.  For a writable stream this may need to
 *             append an empty buffer.
 * 
 *  fn_unread- unreads a byte when there is no room in the current
 *             buffer to hold the unread byte. This will not get
 *             called with an EOF value. Normal behavior is to create 
 *             an empty buffer that "preceeds" the current buffer and
 *             write the byte there.  It may use the the actual previous
 *             buffer.  Or it may fail.
 * 
 *  fn_flush - writes any dirty data out to the output stream.
 * 
 *  fn_close - releases any resources as needed.
 *
 
 
 MISSING: skip, fixes for block read/unread, collapse some functions
 
 
 use cases:
 
 read only stream - (aka (can write == false)
  --next char
      if not end of buffer - done
      if user buffer - fail
      find next page
      if not found
        if mark allocate new page
        fill page from last read position
  --unread
      if beginning of page
        find previous page
        else (not found)
          create temp page
          mark as fake
        save current page
        make new (found or temp) page current @ end of page
      back up in current page
  --write - fail
  --seek - fail
  --skip N
      next char N times
 
  write only stream - (aka (can read == false)
    --next char - fail 
    --unread - fail
    --write
      if no room
        write current page to disk
        release current page
      get new page
      fill page
      make page current @ beginning of buf
      update dirty pointers
    --seek - fail
    --skip - fail
    
----- read only and write only streams are not seekable

  user buffer
    --to initialize allocate stream and 1 page set up page to point to user buffer
    --next char
      if end of buffer fail with EOF
    --unread
      if beginning of buffer - fail
      back up in current buffer
    --write
      if write past buffer length - fail
      write at current position
      update dirty pointers
    --seek
      if position outside buffer - fail
      set position
    --skip
      compute new position
      seek to new position

  seekable (read/write)
    --next char - fail
    --unread - fail
    --write
      if no room in current
        find next page
        if not found
          make page
          seek to page
          fill page
      write data at page
      update dirty pointers
    --seek
      find page
      if not found
        make page
        seek in file to page start
        fill page
      make page current
      set position @ position in page
    --skip
      compute new position
      seek to new position

  ----make page
        when not buffering
          flush current page
          release current page
        allocate page

  ----make page current
        if page != current page
          flush current page
          if not buffering release current page
        set up stream members to new page
        clear dirty pointers
 *
 */

#ifndef ION_STREAM_H_
#define ION_STREAM_H_

#include "ion_types.h"
#include "ion_platform_config.h"
#include "ion_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ION_STREAM_DECL
#define ION_STREAM_DECL

// needed when the stream is used outside the context of the
// general Ion library. Otherwise this must be defined in the
// Ion type header (ion_types.h).
typedef struct _ion_stream        ION_STREAM;

// decl's for user managed stream
typedef iERR (*ION_STREAM_HANDLER)(struct _ion_user_stream *pstream);
struct _ion_user_stream
{
    BYTE *curr;
    BYTE *limit;
    void *handler_state;
    ION_STREAM_HANDLER handler;
};

#endif

typedef struct _ion_stream_user_paged ION_STREAM_USER_PAGED;
typedef struct _ion_stream_paged  ION_STREAM_PAGED;
typedef struct _ion_page          ION_PAGE;
typedef int32_t                   PAGE_ID;
typedef int64_t                   POSITION;

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//                     public constructors

//////////////////////////////////////////////////////////////////////////////////////////////////////////


ION_API_EXPORT iERR ion_stream_open_buffer(BYTE *buffer       // pointer to memory to stream over
                           , SIZE buf_length    // length of user buffer, filled or not
                           , SIZE buf_filled    // length of user filled data (0 or more bytes)
                           , BOOL read_only     // if read_only is true write is disallowed (read is always valid)
                           , ION_STREAM **pp_stream
);
ION_API_EXPORT iERR ion_stream_open_memory_only(ION_STREAM **pp_stream);

ION_API_EXPORT iERR ion_stream_open_stdin(ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_stdout(ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_stderr(ION_STREAM **pp_stream);

ION_API_EXPORT iERR ion_stream_open_file_in(FILE *in, ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_file_out(FILE *out, ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_file_rw(FILE *fp, BOOL cache_all, ION_STREAM **pp_stream);

ION_API_EXPORT iERR ion_stream_open_handler_in(ION_STREAM_HANDLER fn_input_handler, void *handler_state, ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_handler_out(ION_STREAM_HANDLER fn_output_handler, void *handler_state, ION_STREAM **pp_stream);

ION_API_EXPORT iERR ion_stream_open_fd_in(int fd_in, ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_fd_out(int fd_out, ION_STREAM **pp_stream);
ION_API_EXPORT iERR ion_stream_open_fd_rw(int fd, BOOL cache_all, ION_STREAM **pp_stream);

ION_API_EXPORT iERR ion_stream_flush(ION_STREAM *stream);
ION_API_EXPORT iERR ion_stream_close(ION_STREAM *stream);

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//             informational routines (aka getters)

//////////////////////////////////////////////////////////////////////////////////////////////////////////

ION_API_EXPORT BOOL      ion_stream_can_read          (ION_STREAM *stream);
ION_API_EXPORT BOOL      ion_stream_can_write         (ION_STREAM *stream);
ION_API_EXPORT BOOL      ion_stream_can_seek          (ION_STREAM *stream);
ION_API_EXPORT BOOL      ion_stream_can_mark          (ION_STREAM *stream);
ION_API_EXPORT BOOL      ion_stream_is_dirty          (ION_STREAM *stream);
ION_API_EXPORT BOOL      ion_stream_is_mark_open      (ION_STREAM *stream);
ION_API_EXPORT POSITION  ion_stream_get_position      (ION_STREAM *stream);
ION_API_EXPORT FILE     *ion_stream_get_file_stream   (ION_STREAM *stream);
ION_API_EXPORT POSITION  ion_stream_get_mark_start    (ION_STREAM *stream);
ION_API_EXPORT POSITION  ion_stream_get_marked_length (ION_STREAM *stream);

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//            public instance methods

//////////////////////////////////////////////////////////////////////////////////////////////////////////

ION_API_EXPORT iERR ion_stream_read_byte           (ION_STREAM *stream, int *p_c);
ION_API_EXPORT iERR ion_stream_read                (ION_STREAM *stream, BYTE *buf, SIZE len, SIZE *p_bytes_read);
ION_API_EXPORT iERR ion_stream_unread_byte         (ION_STREAM *stream, int c);
ION_API_EXPORT iERR ion_stream_write               (ION_STREAM *stream, BYTE *buf, SIZE len, SIZE *p_bytes_written);
ION_API_EXPORT iERR ion_stream_write_byte          (ION_STREAM *stream, int byte);
ION_API_EXPORT iERR ion_stream_write_byte_no_checks(ION_STREAM *stream, int byte);
ION_API_EXPORT iERR ion_stream_write_stream        (ION_STREAM *stream, ION_STREAM *stream_input, SIZE len, SIZE *p_written);
ION_API_EXPORT iERR ion_stream_seek                (ION_STREAM *stream, POSITION position);
ION_API_EXPORT iERR ion_stream_truncate            (ION_STREAM *stream);
ION_API_EXPORT iERR ion_stream_skip                (ION_STREAM *stream, SIZE distance, SIZE *p_skipped);
ION_API_EXPORT iERR ion_stream_mark                (ION_STREAM *stream);
ION_API_EXPORT iERR ion_stream_mark_remark         (ION_STREAM *stream, POSITION position);
ION_API_EXPORT iERR ion_stream_mark_rewind         (ION_STREAM *stream);
ION_API_EXPORT iERR ion_stream_mark_clear          (ION_STREAM *stream);

#ifdef __cplusplus
}
#endif

#endif /* ION_STREAM_H_ */
