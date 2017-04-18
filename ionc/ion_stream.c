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

/*
 * updated versions of the file stream handling support for
 * the IonC readers and writers.
 *
 * see ion_stream.h for "doc's"
 *  
 *  use cases:
 * 
 *  read only stream - (aka (can write == false)
 *   --next char
 *       if not end of buffer - done
 *       if user buffer - fail
 *       find next page
 *       if not found
 *         if mark allocate new page
 *         fill page from last read position
 *   --unread
 *       if beginning of page
 *         find previous page
 *         else (not found)
 *           create temp page
 *           mark as fake
 *         save current page
 *         make new (found or temp) page current @ end of page
 *       back up in current page
 *   --write - fail
 *   --seek - fail
 *   --skip N
 *       next char N times
 *  
 *   write only stream - (aka (can read == false)
 *     --next char - fail 
 *     --unread - fail
 *     --write
 *       if no room
 *         write current page to disk
 *         release current page
 *       get new page
 *       fill page
 *       make page current @ beginning of buf
 *       update dirty pointers
 *     --seek - fail
 *     --skip - fail
 *     
 * ----- read only and write only streams are not seekable
 * 
 *   user buffer
 *     --to initialize allocate stream and 1 page set up page to point to user buffer
 *     --next char
 *       if end of buffer fail with EOF
 *     --unread
 *       if beginning of buffer - fail
 *       back up in current buffer
 *     --write
 *       if write past buffer length - fail
 *       write at current position
 *       update dirty pointers
 *     --seek
 *       if position outside buffer - fail
 *       set position
 *     --skip
 *       compute new position
 *       seek to new position
 * 
 *   seekable (read/write)
 *     --next char - fail
 *     --unread - fail
 *     --write
 *       if no room in current
 *         find next page
 *         if not found
 *           make page
 *           seek to page
 *           fill page
 *       write data at page
 *       update dirty pointers
 *     --seek
 *       find page
 *       if not found
 *         make page
 *         seek in file to page start
 *         fill page
 *       make page current
 *       set position @ position in page
 *     --skip
 *       compute new position
 *       seek to new position
 * 
 *   ----make page
 *         when not buffering
 *           flush current page
 *           release current page
 *         allocate page
 * 
 *   ----make page current
 *         if page != current page
 *           flush current page
 *           if not buffering release current page
 *         set up stream members to new page
 *         clear dirty pointers
 *
 */

#include "ion_internal.h"

#include <fcntl.h> 

#ifdef ION_PLATFORM_WINDOWS
  #include <io.h>
  // deal with Win32/64 lameness with respect to setting modes
  #define SET_MODE_BINARY(x) (_setmode(_fileno(x),_O_BINARY))
  #define FSEEK _fseeki64
  // in windows we'll let the is tty fn handle this otherwise a rw file isn't likely to be a tty
  #define FD_IS_TTY(fd)           _isatty(fd) /* TODO */
#else
  #define SET_MODE_BINARY(x) 1
  // We use the fseeko incase of MAC or iOS to support file size >2GB
  #define FSEEK fseeko
  #define FD_IS_TTY(fd)           FALSE /* TODO */
#endif

// We use fd based stream in android to support file size > 2GB
// The lseek should be lseek64 in that case
#if defined(ION_PLATFORM_ANDROID)
  #include <unistd.h>
  #define LSEEK lseek64
#elif defined(ION_PLATFORM_WINDOWS)
  #define LSEEK _lseek
#else
  #include <unistd.h>
  #define LSEEK lseek
#endif

#ifdef ION_PLATFORM_WINDOWS
  #define WRITE _write
#else
  #include <unistd.h>
  #define WRITE write
#endif

#ifdef ION_PLATFORM_WINDOWS
  #define READ _read
#else
  #include <unistd.h>
  #define READ read
#endif



//////////////////////////////////////////////////////////////////////////////////////////////////////////

//                     public constructors

//////////////////////////////////////////////////////////////////////////////////////////////////////////

iERR ion_stream_open_buffer( BYTE *buffer       // pointer to memory to stream over
                           , SIZE buf_length    // length of user buffer, filled or not
                           , SIZE buf_filled    // length of user filled data (0 or more bytes)
                           , BOOL read_only     // if read_only is true write is disallowed (read is always valid)
                           , ION_STREAM **pp_stream
) {
  iENTER;
  ION_STREAM_FLAG  flags;
  ION_STREAM *stream;
 
  
  if (!buffer)                  FAILWITH(IERR_INVALID_ARG);
  if (buf_filled < 0)           FAILWITH(IERR_INVALID_ARG);
  if (buf_length < buf_filled)  FAILWITH(IERR_INVALID_ARG);
  if (!pp_stream)               FAILWITH(IERR_INVALID_ARG);

  flags = ION_STREAM_USER_BUF; //  = (FLAG_CAN_READ  | FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS | FLAG_BUFFER_ALL | FLAG_IS_USER_BUFFER), 

  if (read_only) {
    SET_FLAG_OFF(flags, FLAG_CAN_WRITE);
  }
 
  
  IONCHECK(_ion_stream_open_helper(flags, buf_length, &stream));

  // here we manualy set up the state to mimic a paged stream
  // see _ion_stream_page_make_current
  stream->_buffer = buffer;
  stream->_offset = 0;
  stream->_limit  = buffer + buf_filled;
  stream->_curr   = buffer;
 
  
  *pp_stream = stream;
  SUCCEED();    
 
  
  iRETURN;
}

iERR ion_stream_open_stdin( ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_STDIN;
  
  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));

  SET_MODE_BINARY(stdin);
  stream->_fp = stdin;

  err = _ion_stream_fetch_position(stream, 0);
  if (err != IERR_OK && err != IERR_EOF) {
      // we might enounter an eof if the input stream is completely empty
      FAILWITH(err);
  }

  *pp_stream = stream;
  SUCCEED();    

  iRETURN;
}

iERR ion_stream_open_stdout( ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_STDOUT;

  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));
  SET_MODE_BINARY(stdout);
  stream->_fp = stdout;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
  
  *pp_stream = stream;
  SUCCEED();
  
  iRETURN;
}

iERR ion_stream_open_file_in( FILE *in, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_FILE_IN;

  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);
  if (!in) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));

  stream->_fp = in;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
  
  *pp_stream = stream;
  SUCCEED();
  
  iRETURN;
}

iERR ion_stream_open_file_out( FILE *out, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_FILE_OUT;
   
  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);
  if (!out) FAILWITH(IERR_INVALID_ARG);
   
  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));
  stream->_fp = out;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
   
  *pp_stream = stream;
  SUCCEED();    
   
  iRETURN;
}

iERR ion_stream_open_file_rw( FILE *fp, BOOL cache_all, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_FILE_RW;
   
  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);
  if (!fp) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));

  stream->_fp = fp;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
 
  *pp_stream = stream;
  SUCCEED();    
   
  iRETURN;
}

iERR ion_stream_open_fd_in( int fd_in, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_FD_IN;  // was: ION_STREAM_FD_RW, hmmm?
  
  if (!pp_stream)  FAILWITH(IERR_INVALID_ARG);
  if (fd_in == -1) FAILWITH(IERR_INVALID_ARG);

  if ( FD_IS_TTY(fd_in) ) {
	  flags |= FLAG_IS_TTY;  // or should we throw an error ?
  }

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));
  stream->_fp = (FILE *)fd_in;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
  
  *pp_stream = stream;
  SUCCEED();    
  
  iRETURN;
}

iERR ion_stream_open_fd_out( int fd_out, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_FD_OUT;
  
  if (!pp_stream)   FAILWITH(IERR_INVALID_ARG);
  if (fd_out == -1) FAILWITH(IERR_INVALID_ARG);

  if ( FD_IS_TTY(fd_out) ) {
	  flags |= FLAG_IS_TTY;
  }

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));
  stream->_fp = (FILE *)fd_out;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
  
  *pp_stream = stream;
  SUCCEED();    
  
  iRETURN;
}

iERR ion_stream_open_fd_rw( int fd, BOOL cache_all, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_FLAG   flags = ION_STREAM_FD_RW;
  
  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);
  if (fd == -1)   FAILWITH(IERR_INVALID_ARG);

  if ( FD_IS_TTY(fd) ) {
	  flags |= FLAG_IS_TTY;  // or should we throw an error ?
  }

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));
  stream->_fp = (FILE *)fd;
  IONCHECK(_ion_stream_fetch_position(stream, 0));
  
  *pp_stream = stream;
  SUCCEED();    
  
  iRETURN;
}

iERR ion_stream_open_memory_only( ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM       *stream;
  ION_STREAM_PAGED *paged;
  ION_PAGE         *page = NULL;
  ION_STREAM_FLAG   flags = ION_STREAM_MEMORY_ONLY;

  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));

  // for the all in memory case 
  paged = PAGED_STREAM( stream );
  IONCHECK(_ion_stream_page_allocate(paged, 0, &page));
  IONCHECK(_ion_stream_page_register(paged, page));
  IONCHECK(_ion_stream_page_make_current(paged, page));

  *pp_stream = stream;
  SUCCEED();    

  iRETURN;
}

iERR ion_stream_open_handler_in( ION_STREAM_HANDLER fn_input_handler, void *handler_state, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM              *stream = NULL;
  struct _ion_user_stream *user_stream = NULL;
  ION_STREAM_FLAG          flags = ION_STREAM_USER_IN;

  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);
  if (!fn_input_handler) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));

  user_stream = &(((ION_STREAM_USER_PAGED *)stream)->_user_stream);

  user_stream->handler_state = handler_state;
  user_stream->handler = fn_input_handler;

  IONCHECK(_ion_stream_fetch_position(stream, 0));

  *pp_stream = stream;

  iRETURN;
}

iERR ion_stream_open_handler_out( ION_STREAM_HANDLER fn_output_handler, void *handler_state, ION_STREAM **pp_stream )
{
  iENTER;
  ION_STREAM              *stream = NULL;
  struct _ion_user_stream *user_stream = NULL;
  ION_STREAM_FLAG          flags = ION_STREAM_USER_OUT;

  if (!pp_stream) FAILWITH(IERR_INVALID_ARG);
  if (!fn_output_handler) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_open_helper(flags, g_Ion_Stream_Default_Page_Size, &stream));

  user_stream = &(((ION_STREAM_USER_PAGED *)stream)->_user_stream);
  user_stream->handler_state = handler_state;
  user_stream->handler = fn_output_handler;

  IONCHECK(_ion_stream_fetch_position(stream, 0));

  *pp_stream = stream;

  iRETURN;
}


iERR ion_stream_flush(ION_STREAM *stream)
{
  iENTER;
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (_ion_stream_can_write(stream) != TRUE) FAILWITH(IERR_INVALID_ARG);

  IONCHECK(_ion_stream_flush_helper(stream));  
  SUCCEED();

  iRETURN;
}

iERR ion_stream_close(ION_STREAM *stream)
{
  iENTER;

  if (!stream) FAILWITH(IERR_INVALID_ARG);  
  
  if (_ion_stream_can_write(stream) == TRUE) {
    IONCHECK(_ion_stream_flush_helper(stream));
  }

  // clear the stream out so that it is invalid in case
  // someone tries to use it after they have freed it
  stream->_buffer = NULL;
  stream->_buffer_size = 0;
  stream->_fp = NULL;
  stream->_flags = FLAG_IS_CLOSED;
  ion_free_owner(stream);
  SUCCEED();
 
  iRETURN;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//             informational routines (aka getters)

//////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOL ion_stream_can_read( ION_STREAM *stream )
{
  BOOL can_read = FALSE;
  if (stream) {
    can_read = _ion_stream_can_read(stream);
  }
  return can_read;
}

BOOL ion_stream_can_write( ION_STREAM *stream )
{
  BOOL can_write = FALSE;
  if (stream) {
    can_write = _ion_stream_can_write(stream);
  }
  return can_write;
}

BOOL ion_stream_can_seek( ION_STREAM *stream )
{
  BOOL can_seek = FALSE;
  if (stream) {
    can_seek = _ion_stream_can_random_seek(stream);
  }
  return can_seek;
}

BOOL ion_stream_is_dirty( ION_STREAM *stream )
{
  BOOL is_dirty = FALSE;
  if (stream) {
    is_dirty = _ion_stream_is_dirty(stream);
  }
  return is_dirty;
}

BOOL ion_stream_is_mark_open( ION_STREAM *stream )
{
  BOOL is_mark_open = FALSE;
  if (stream) {
    is_mark_open = _ion_stream_is_mark_open(stream);
  }
  return is_mark_open;
}

POSITION ion_stream_get_position( ION_STREAM *stream )
{
  POSITION pos = -1;
  if (stream) {
    pos = _ion_stream_position(stream);
  }
  return pos;
}

FILE *ion_stream_get_file_stream( ION_STREAM *stream )
{
  FILE *fp = NULL;
  if (stream) {
    fp = _ion_stream_get_file_stream(stream);
  }
  return fp;
}

POSITION ion_stream_get_mark_start( ION_STREAM *stream )
{
  POSITION mark_start = -1;
  if (stream) {
    mark_start = _ion_stream_get_mark_start(stream);
  }
  return mark_start;
}

POSITION ion_stream_get_marked_length( ION_STREAM *stream )
{
  POSITION marked_length = -1;
  if (stream) {
    marked_length = _ion_stream_get_marked_length(stream);
  }
  return marked_length;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////

//            public instance methods

//////////////////////////////////////////////////////////////////////////////////////////////////////////


// read 1 byte, return -1 (EOF) if not data is available or the stream can't be read
iERR ion_stream_read_byte(ION_STREAM *stream, int *p_c)
{
  iENTER;
  int               c;
  POSITION          position;
 
 
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (!_ion_stream_can_read(stream)) FAILWITH(IERR_INVALID_ARG);
  if (!p_c) FAILWITH(IERR_INVALID_ARG);

  if (stream->_curr >= stream->_limit) {
    if (_ion_stream_is_paged(stream)) {
		position = _ion_stream_position(stream);

		// note that position is the next (unavailable) byte
		// since it is past the limit of this page
		err = _ion_stream_fetch_position( stream, position );
    }
    else {
        // if we hit the end of buffer on any unpaged stream, we're at EOF
        err = IERR_EOF;
    }
    if (err != IERR_OK) {
        if (err != IERR_EOF) FAILWITH(err);
        err = IERR_OK;
        c = EOF;
    }
  }

  if (stream->_curr < stream->_limit) {
    c = *stream->_curr++;
  }
  else {
    c = EOF;
  }
  *p_c = c;

  iRETURN;
}

iERR ion_stream_read(ION_STREAM *stream, BYTE *buf, SIZE len, SIZE *p_bytes_read)
{
  iENTER;
  SIZE     available, needed = len;
  POSITION position;
  BYTE    *dst = buf;
  
  if (!stream)       FAILWITH(IERR_INVALID_ARG);
  if (len < 0)       FAILWITH(IERR_INVALID_ARG);
  if (!buf)          FAILWITH(IERR_INVALID_ARG);
  if (!p_bytes_read) FAILWITH(IERR_INVALID_ARG);
  if (_ion_stream_can_read(stream) == FALSE) FAILWITH(IERR_INVALID_ARG);

  while (needed > 0) {
    available = (SIZE)(stream->_limit - stream->_curr);
    if (available < 1) {
      // note that position is the next (unavailable) byte
      // since it is past the limit of this page
      position = _ion_stream_position(stream);
      err = _ion_stream_fetch_position(stream, position);
      if (err && err != IERR_EOF) FAILWITH(err); // on EOF we'll break out just below
      
      available = (SIZE)(stream->_limit - stream->_curr);
      if (available < 1) {
        break;
      }
    }
    if (available > needed) {
        available = needed;
    }
    memcpy(dst, stream->_curr, available);
    stream->_curr += available;
    dst += available;
    needed -= available;
  }
  *p_bytes_read = len - needed;
  if (err == IERR_EOF) FAILWITH(err);
  SUCCEED();

  iRETURN;
}

// unreads last read byte, you can only unread what was actually read
iERR ion_stream_unread_byte(ION_STREAM *stream, int c)
{
  iENTER;
  ION_STREAM_PAGED *paged;
  POSITION          position;
  PAGE_ID           target_page_id;
  ION_PAGE         *page = NULL;
  
  if (!stream)           FAILWITH(IERR_INVALID_ARG);
  if (c < 0 && c != EOF) FAILWITH(IERR_INVALID_ARG);
  if (_ion_stream_can_read(stream) == FALSE) FAILWITH(IERR_INVALID_ARG);

  // do we need to fetch a previous page?
  if (stream->_curr <= stream->_buffer) 
  {
    if (stream->_offset == 0) {
      // you can unread past the beginning of the file
      if (c != EOF) {
          // but only when we're unreading an EOF
          FAILWITH(IERR_UNEXPECTED_EOF);
      }
      SUCCEED();
    }

    // note that if the offset is not 0 then this stream has to be paged
    ASSERT(_ion_stream_is_paged(stream));
    paged = PAGED_STREAM(stream);

    position = _ion_stream_position(stream) - 1;  // -1 because we're backing up to unread onto the previous read char and position is the next-to-read char
    target_page_id = _ion_stream_page_id_from_offset(stream, position);
    IONCHECK(_ion_stream_page_find(paged, target_page_id, &page));
    if (!page) {
      if (_ion_stream_can_seek_to(stream, position)) {
        IONCHECK(_ion_stream_fetch_position(stream, position));
        page = paged->_curr_page;
        ASSERT(page);
        ASSERT(target_page_id == page->_page_id);
      }
      if (!page) {
        // we can't back up into saved pages so we have to make a fake page
        IONCHECK(_ion_stream_page_allocate(paged, target_page_id, &page));
        page->_page_start = stream->_buffer_size;
      }
    }
    IONCHECK(_ion_stream_page_make_current(paged, page));
    ASSERT((position - stream->_offset) < (POSITION)stream->_buffer_size); // check for overflow
    stream->_curr = IH_CURR_OF(position) + 1; // +1 since we offset the position due to backing up
  }
  
  // at this point we have to have room to back up
  if (c != EOF) {
      ASSERT(stream->_curr > stream->_buffer);
      stream->_curr--;
      if (page != NULL && page->_page_start > 0) {
        *stream->_curr = IH_MAKE_BYTE(c);
        page->_page_start--;
        page->_page_limit++;
      }
      if (*(stream->_curr) != c) {
        FAILWITH(IERR_INVALID_ARG);
      }
  }
  SUCCEED();
  iRETURN;
}

// write len bytes from buf onto output, returns bytes actually written which should be len
iERR ion_stream_write(ION_STREAM *stream, BYTE *buf, SIZE len, SIZE *p_bytes_written)
{
  iENTER;
  SIZE to_write, src_remaining = len;
  BYTE *dst, *src = buf;
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (!buf) FAILWITH(IERR_INVALID_ARG);
  if (len < 0) FAILWITH(IERR_INVALID_ARG);
  if (!p_bytes_written) FAILWITH(IERR_INVALID_ARG);
  if (_ion_stream_can_write(stream) == FALSE) FAILWITH(IERR_INVALID_ARG);
  
  // we just copy as many bytes as can fit into the current buffer
  // and if we have more to write we seek to fetch the next page
  // which will flush the current page to the output stream
  while (src_remaining > 0) {
    to_write = stream->_buffer_size - (SIZE)(stream->_curr - stream->_buffer); // should be limited by page size
    if (to_write < 1) {
      // if there's no room get the next page
      IONCHECK(_ion_stream_fetch_position( stream, _ion_stream_position(stream) ));
      to_write = stream->_buffer_size - (SIZE)(stream->_curr - stream->_buffer); // limited by page size
    }
    dst = stream->_curr;
    if (to_write > src_remaining) {
      to_write = src_remaining;
    }
    memcpy(dst, src, to_write);
    if (stream->_dirty_start == NULL) {
      stream->_dirty_start = stream->_curr;
    }
    src += to_write;
    stream->_dirty_length += to_write;
    stream->_curr += to_write;
    if (stream->_curr > stream->_limit) {
      stream->_limit = stream->_curr;
    }
    src_remaining -= to_write;
  }

  *p_bytes_written = len - src_remaining;
  SUCCEED();
  
  iRETURN;
}

// write byte out, this is treated as an unsigned 8 bit int
iERR ion_stream_write_byte(ION_STREAM *stream, int byte)
{
  iENTER;
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (IH_IS_BYTE(byte) == FALSE) FAILWITH(IERR_INVALID_ARG);
  if (_ion_stream_can_write(stream) == FALSE) FAILWITH(IERR_INVALID_ARG);
  
  if (stream->_curr >= (stream->_buffer + stream->_buffer_size) ) {
    // if there's no room get the next page
    IONCHECK(_ion_stream_fetch_position( stream, _ion_stream_position(stream)+1));
  }
  
  *stream->_curr = IH_MAKE_BYTE(byte);
  
  // now update our stream state
  if (stream->_dirty_start == NULL) {
    stream->_dirty_start = stream->_curr;
  }
  stream->_dirty_length++;
  stream->_curr++;          // this has to be done _after_ dirty start is updated
  if (stream->_curr > stream->_limit) {
    stream->_limit = stream->_curr;
  }
  SUCCEED();
  
  iRETURN;
}

// write byte out, this is treated as an unsigned 8 bit int
iERR ion_stream_write_byte_no_checks(ION_STREAM *stream, int byte)
{
  iENTER;
   
  if (stream->_curr >= (stream->_buffer + stream->_buffer_size) ) {
    // if there's no room get the next page
    IONCHECK(_ion_stream_fetch_position( stream, _ion_stream_position(stream)));
  }
   
  *stream->_curr = IH_MAKE_BYTE(byte);
  
  // now update our stream state
  if (stream->_dirty_start == NULL) {
    stream->_dirty_start = stream->_curr;
  }
  stream->_dirty_length++;
  stream->_curr++;          // this has to be done _after_ dirty start is updated
  if (stream->_curr > stream->_limit) {
    stream->_limit = stream->_curr;
  }
  SUCCEED();
  
  iRETURN;
}


#define TEMP_BUFFER_LEN (8096)

// this writes some number of bytes from an input stream
// to this (target) output stream
iERR ion_stream_write_stream( ION_STREAM *stream, ION_STREAM *stream_input, SIZE len, SIZE *p_written )
{
  iENTER;
  SIZE written = 0, planned_bytes, actual_bytes, remaining;
  BYTE temp_buffer[TEMP_BUFFER_LEN];

  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (!stream_input) FAILWITH(IERR_INVALID_ARG);
  if (!len < 0) FAILWITH(IERR_INVALID_ARG);
  if (!p_written) FAILWITH(IERR_INVALID_ARG);
  if (!_ion_stream_can_write(stream)) FAILWITH(IERR_INVALID_ARG);
  if (!_ion_stream_can_read(stream_input)) FAILWITH(IERR_INVALID_ARG);

  for (remaining = len; remaining > 0; written += actual_bytes, remaining -= actual_bytes) {
    if (remaining < TEMP_BUFFER_LEN) {
        planned_bytes = remaining;
    }
    else {
        planned_bytes = TEMP_BUFFER_LEN;
    }
    IONCHECK(ion_stream_read(stream_input, temp_buffer, planned_bytes, &actual_bytes));
    if (planned_bytes != actual_bytes) FAILWITH(IERR_READ_ERROR);

    IONCHECK(ion_stream_write(stream, temp_buffer, planned_bytes, &actual_bytes));
    if (planned_bytes != actual_bytes) FAILWITH(IERR_READ_ERROR);
  }
  *p_written = written;
  SUCCEED();
  
  iRETURN;
}

iERR ion_stream_truncate( ION_STREAM *stream )
{
    iENTER;
    POSITION          end_mark;
    ION_STREAM_PAGED *paged;
    PAGE_ID           idx, end_idx;
    ION_PAGE         *page;

    if (!stream) FAILWITH(IERR_INVALID_ARG);

    if (_ion_stream_is_mark_open(stream)) {
        end_mark = _ion_stream_get_mark_start( stream ) + _ion_stream_get_marked_length( stream );
        if (end_mark > _ion_stream_position(stream)) {
             FAILWITH(IERR_INVALID_STATE);
        }
    }

    if (_ion_stream_is_paged(stream)) {
        paged = PAGED_STREAM(stream);
        idx = paged->_curr_page->_page_id;
        end_idx = paged->_last_page->_page_id;
        while (idx < end_idx) {
            idx++;
            IONCHECK(_ion_stream_page_find(paged, idx, &page));
            if (page) {
                _ion_stream_page_release(paged, page);
            }
        }
        paged->_last_page = paged->_curr_page;
    }
    stream->_limit = IH_CURR_OF(_ion_stream_position(stream));
    SUCCEED();
	
    iRETURN;
}

 // seek to position - move the read/write head to the user specified position in the file
 // TODO: should we include direction (ala C std lib)?
 // that would eliminate the need for _skip
iERR ion_stream_seek( ION_STREAM *stream, POSITION target_pos)
{
  iENTER;
  POSITION actual_pos;
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (target_pos < 0) FAILWITH(IERR_INVALID_ARG);

  // we can also see with there's mark - that's checked in seek helper - if (_ion_stream_can_seek(stream) == FALSE) FAILWITH(IERR_INVALID_ARG);

  if (_ion_stream_current_page_contains_position( stream, target_pos )) {
      stream->_curr = IH_CURR_OF( target_pos );
  } 
  else {
	if (_ion_stream_is_paged(stream) == FALSE) {
		if (target_pos != _ion_stream_position(stream)) {
			FAILWITH(IERR_SEEK_ERROR);
		}
    }
    else {
		IONCHECK(_ion_stream_fetch_position(stream, target_pos));
	}
  }

  actual_pos = _ion_stream_position(stream);
  if (actual_pos != target_pos) {
      FAILWITH(IERR_SEEK_ERROR);
  }
  
  iRETURN;
}

// skip <distance> from current position, return actual distance skipped in case it hits an edge first
iERR ion_stream_skip( ION_STREAM *stream, SIZE distance, SIZE *p_skipped)
{
  iENTER;
  POSITION original_pos, target_pos, actual_pos;
  SIZE     skipped;  
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (!p_skipped) FAILWITH(IERR_INVALID_ARG);

  original_pos = target_pos = _ion_stream_position(stream);
  target_pos += distance;
  IONCHECK(_ion_stream_fetch_position(stream, target_pos));
  actual_pos = _ion_stream_position(stream);
   
  ASSERT((actual_pos - original_pos) <= (POSITION)distance); // we should never overshoot
  skipped = (SIZE)(actual_pos - original_pos);
  *p_skipped = skipped;
  
  iRETURN;
}

iERR ion_stream_mark( ION_STREAM *stream )
{
  iENTER;
  
  if (!stream) FAILWITH(IERR_INVALID_ARG); 
 
  if (stream->_mark == -1) {
    stream->_mark = _ion_stream_position(stream);
  }
   
  SUCCEED();
  
  iRETURN;
}

// move the current mark position from whereever it
// was to the current position (generally used when
// moving forward)
iERR ion_stream_mark_remark( ION_STREAM *stream, POSITION position )
{
  iENTER;
  ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (!_ion_stream_is_mark_open(stream)) FAILWITH(IERR_INVALID_ARG);

  if (stream->_mark < position) {
    // if we have pages, and we're not caching all of them, free up pages we 
    // have already gone past. That is starting from the mark to the page 
    // just before the current position (before because we haven't "stepped"
    // onto that byte just yet.
    if (_ion_stream_is_paged(stream) && !_ion_stream_is_fully_buffered(stream)) {
      IONCHECK(_ion_stream_mark_clear_helper( paged, position ));
    }
  }
  // now make out current position the new mark
  stream->_mark = position;
  SUCCEED();
  
  iRETURN;
}

iERR ion_stream_mark_rewind( ION_STREAM *stream )
{
  iENTER;
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (stream->_mark == -1) {
      FAILWITH(IERR_MARK_NOT_SET);
  }

  if (_ion_stream_current_page_contains_position( stream, stream->_mark )) {
      stream->_curr = IH_CURR_OF( stream->_mark );
  } 
  else {
      if (_ion_stream_is_paged(stream) == FALSE) {
          FAILWITH(IERR_SEEK_ERROR);
      }
      IONCHECK(_ion_stream_fetch_position(stream, stream->_mark));
  }
  SUCCEED();
  
  iRETURN;
}

iERR ion_stream_mark_clear( ION_STREAM *stream )
{
  iENTER;
  ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
  
  if (!stream) FAILWITH(IERR_INVALID_ARG);
  if (!_ion_stream_is_mark_open(stream)) SUCCEED();

  // if we have pages, and we're not caching all of them, free up pages we 
  // have already gone past. That is starting from the mark to the page 
  // just before the current position (before because we haven't "stepped"
  // onto that byte just yet.
  if (_ion_stream_is_paged(stream) && !_ion_stream_is_fully_buffered(stream)) {
      IONCHECK(_ion_stream_mark_clear_helper( paged, _ion_stream_position(stream) ));
  }

  // and now we don't have a mark
  stream->_mark = -1;
  SUCCEED();
   
  iRETURN;
}

iERR _ion_stream_mark_clear_helper( ION_STREAM_PAGED *paged, POSITION position )
{
  iENTER;
  ION_STREAM *stream = UNPAGED_STREAM(paged);
  POSITION    mark_position; // _ion_stream_position(stream)
  PAGE_ID     page_id, current_page;
  ION_PAGE   *page;
   
  ASSERT(stream);
  ASSERT(_ion_stream_is_mark_open(stream));
  ASSERT(_ion_stream_is_paged(stream));
  ASSERT(!_ion_stream_is_fully_buffered(stream));
 
  
  // if we have pages, and we're not caching all of them, free up pages we 
  // have already gone past. That is starting from the mark to the page 
  // just before the current position (before because we haven't "stepped"
  // onto that byte just yet.
  mark_position =  stream->_mark;
  page_id = _ion_stream_page_id_from_offset(stream, mark_position);

// WAS:: !!!!  position = _ion_stream_position(stream);
  // when position is less than buffer_size we can only be on the first page 
  // (so there can be nothing to free and there is a boundary condition at 0 
  // where position - 1 is negative, i.e. invalid, we need to avoid)
  if (position > stream->_buffer_size) { 
      // -1 on pos since it's the next char and we need to keep the next char in place
      current_page = _ion_stream_page_id_from_offset(stream, (position - 1)); 
      while (page_id < current_page) {
          IONCHECK(_ion_stream_page_find(paged, page_id, &page));
          _ion_stream_page_release(paged, page);
          page_id++;
      }
  }

  SUCCEED();
   
  iRETURN;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////

//                     internal constructor helpers

//////////////////////////////////////////////////////////////////////////////////////////////////////////

iERR _ion_stream_open_helper(ION_STREAM_FLAG flags, SIZE page_size, ION_STREAM **pp_stream)
{
  iENTER;
  BOOL              user_buffer, user_managed;
  SIZE              len;
  ION_STREAM       *stream = NULL;
  ION_STREAM_PAGED *paged;
  ION_INDEX_OPTIONS index_options;
  
  ASSERT(pp_stream);
  //ASSERT(page_size > 0);

  user_buffer = IS_FLAG_ON(flags, FLAG_IS_USER_BUFFER);
  if (user_buffer) {
    len = sizeof(ION_STREAM);
  }
  else {
    user_managed = IS_FLAG_ON(flags, FLAG_USER_HANDLING);
    if (user_managed) {
        len = sizeof(ION_STREAM_USER_PAGED);
    }
    else {
		len = sizeof(ION_STREAM_PAGED);
	}
  }

  stream = ion_alloc_owner(len);
  if (!stream) FAILWITH(IERR_NO_MEMORY);
   
  memset(stream, 0, len);

  stream->_flags = flags;
  stream->_mark =  -1;
  stream->_dirty_start = NULL;
  stream->_dirty_length = 0;
  stream->_buffer_size = page_size;

  if (!user_buffer) {
    paged = PAGED_STREAM(stream);
    paged->_curr_page = NULL;
    paged->_last_page = NULL;
    paged->_page_size = page_size;

    index_options._memory_owner           = stream;
    index_options._compare_fn             = _ion_stream_page_compare_page_ids;
    index_options._hash_fn                = _ion_stream_page_hash_page_id;
    index_options._initial_size           = 0; /* let index pick a default number of actual keys */
    index_options._density_target_percent = 0; /* let index pick a default for table size increases */
    
    IONCHECK(_ion_index_initialize((ION_INDEX *)(&(paged->_index)), &index_options));

    if (user_managed) {
        // 0 fill is fine
    }
  }

  *pp_stream = stream;
  SUCCEED();
 
 iRETURN;
}

iERR _ion_stream_flush_helper(ION_STREAM *stream)
{
  iENTER;
  POSITION position;
  SIZE     written, available;
  struct _ion_user_stream  *user_stream;

  ASSERT(stream);
  ASSERT(_ion_stream_can_write(stream));
 
  
  if (_ion_stream_is_dirty(stream)) {
    if (_ion_stream_is_file_backed(stream)) {
      // now we either write through the user handler, or directly to the file
      if (_ion_stream_is_user_controlled(stream)) {
        user_stream = &(((ION_STREAM_USER_PAGED *)stream)->_user_stream);
        while (stream->_dirty_length > 0) {
            available = (SIZE)(user_stream->limit - user_stream->curr);
            if (available > stream->_dirty_length) {
                available = stream->_dirty_length;
            }
            memcpy(user_stream->curr, stream->_dirty_start, available);
            user_stream->curr += available;
            IONCHECK((*(user_stream->handler))(user_stream));
            stream->_dirty_length -= available;
            stream->_dirty_start += available;
        }
      }
      else if (_ion_stream_is_fd_backed(stream)) {
        written = (SIZE)WRITE( (int)stream->_fp, stream->_dirty_start, stream->_dirty_length );
        if (written != stream->_dirty_length) {
          FAILWITH( IERR_WRITE_ERROR );
        }
      }
	  else {
		ASSERT(_ion_stream_is_file_backed(stream));
		written = (SIZE)fwrite( stream->_dirty_start, sizeof(BYTE), stream->_dirty_length, stream->_fp );

		if (written != stream->_dirty_length) {

			FAILWITH( IERR_WRITE_ERROR );

		}

	  }
    }
    stream->_dirty_start = NULL;
    stream->_dirty_length = 0;
  }  
  SUCCEED();
 
  iRETURN;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//  internal getters and other informational functions
//  (these are expected to be inlined by the compiler)
   
//////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOL _ion_stream_is_mark_open(ION_STREAM *stream)
{
  BOOL   mark_in_progress = (stream->_mark >= 0);
  return mark_in_progress;
}

BOOL _ion_stream_can_random_seek(ION_STREAM *stream)
{
  BOOL   can_seek = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_RANDOM_ACCESS);
  return can_seek;
}

BOOL _ion_stream_can_seek_to(ION_STREAM *stream, POSITION pos)
{
  BOOL can_seek;

  if (IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_RANDOM_ACCESS)) {
      // we can random access the stream we can always seek
      can_seek = TRUE;
  }
  else if (pos >= stream->_offset) {
      // if the position is after the current page start
      // we can seek, by reading if no other option is available
      can_seek = TRUE;
  }
  else if (stream->_mark >= 0 && pos >= stream->_mark) {
      // if the position is after the current mark AND there is a mark it is cached
      can_seek = TRUE;
  }
  else {
      // we can't seek in the file, we can't read to the position 
      // since it's in front of the current page, and it's not
      // cached - we're out of option - no finding, no seeking
      can_seek = FALSE;
  }

  return can_seek;
}

BOOL _ion_stream_can_read(ION_STREAM *stream)
{
  BOOL   can_read = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_CAN_READ);
  return can_read;
}
BOOL _ion_stream_can_write(ION_STREAM *stream)
{
  BOOL   can_write = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_CAN_WRITE);
  return can_write;
}
BOOL _ion_stream_is_dirty(ION_STREAM *stream)
{
  BOOL   is_dirty = (stream->_dirty_start != NULL);
  return is_dirty;
}
BOOL _ion_stream_is_file_backed(ION_STREAM *stream)
{
  BOOL   is_file_backed = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_IS_FILE_BACKED);
  return is_file_backed;
}
BOOL _ion_stream_is_fd_backed(ION_STREAM *stream)
{
  BOOL   fd_backed = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_IS_FD_BACKED);
  return fd_backed;
}
BOOL _ion_stream_is_tty(ION_STREAM *stream)
{
  BOOL   is_tty = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_IS_TTY);
  return is_tty;
}
BOOL _ion_stream_is_user_controlled(ION_STREAM *stream)
{
  BOOL   is_user_controlled = IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_USER_HANDLING);
  return is_user_controlled;
}
BOOL _ion_stream_is_paged( ION_STREAM *stream)
{
  BOOL   is_paged = (IS_FLAG_ON(STREAM_FLAGS(stream), FLAG_IS_USER_BUFFER) == FALSE);
  return is_paged;
}
BOOL _ion_stream_is_fully_buffered(ION_STREAM *stream)
{
  BOOL   is_fully_buffered = IS_FLAG_ON(STREAM_FLAGS(stream),FLAG_BUFFER_ALL);
  return is_fully_buffered;
}
BOOL _ion_stream_is_caching( ION_STREAM *stream)
{
  BOOL   is_caching = _ion_stream_is_mark_open(stream) || _ion_stream_is_fully_buffered(stream);
  return is_caching;
}
FILE *_ion_stream_get_file_stream( ION_STREAM *stream )
{
  FILE *fp;
  ASSERT(stream);
  fp = stream->_fp;
  return fp;
}
POSITION _ion_stream_get_mark_start( ION_STREAM *stream )
{
  POSITION mark_start;
  ASSERT(stream);
  mark_start = stream->_mark;
  return mark_start;
}
POSITION _ion_stream_get_marked_length( ION_STREAM *stream )
{
  POSITION marked_length = -1;
  ASSERT(stream);
  if (_ion_stream_is_mark_open(stream)) {
       marked_length = _ion_stream_position(stream) - stream->_mark;
  }
  return marked_length;
}

// page_id - get the page id for the page containing this file offset
PAGE_ID _ion_stream_page_id_from_offset( ION_STREAM *stream, POSITION file_offset )
{
  PAGE_ID page_id;
  
  ASSERT(stream);
  ASSERT(file_offset >= 0);

  if (stream->_buffer_size > 0) {
      page_id = (SIZE)(file_offset / ((POSITION)stream->_buffer_size));
  }
  else {
      page_id = 0;
  }
   
  return page_id;
}

// get the file offset of the first byte in the page page_id
POSITION _ion_stream_offset_from_page_id( ION_STREAM *stream, PAGE_ID page_id )
{
  POSITION pos;

  ASSERT(stream);
  ASSERT(page_id >= 0);
   
  pos = page_id * ((POSITION)stream->_buffer_size);
  
  return pos;
}

// target offset = start of page offset ( new offset ) // modulus??
POSITION _ion_stream_page_start_offset( ION_STREAM *stream, POSITION file_offset )
{
  PAGE_ID  page_id;
  POSITION pos;

  ASSERT(stream);
  ASSERT(file_offset >= 0);
   
  page_id = _ion_stream_page_id_from_offset(stream, file_offset);
  pos     = _ion_stream_offset_from_page_id(stream, page_id);

  return pos;
}

// return the streams current logical position
POSITION _ion_stream_position( ION_STREAM *stream )
{
  POSITION pos;

  ASSERT(stream);
 
  pos = IH_POSITION_OF( stream->_curr );

  return pos;
}

BOOL _ion_stream_current_page_contains_position( ION_STREAM *stream, POSITION position )
{
    ASSERT(stream);
    ASSERT(position >= 0);

    if (position < stream->_offset) {
        return FALSE;
    }
    if (position >= IH_POSITION_OF(stream->_limit)) {
        return FALSE;
    }
    return TRUE;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//            internal instance functions

//////////////////////////////////////////////////////////////////////////////////////////////////////////

iERR _ion_stream_fetch_position( ION_STREAM *stream, POSITION target_position )
{
    iENTER;
    ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
    PAGE_ID           current_page_id, target_page_id;
    ION_PAGE         *page;
    POSITION          page_end;
    BOOL              new_page = FALSE;

    ASSERT(stream);
    ASSERT(target_position >= 0);

    if (!_ion_stream_is_paged(stream) && _ion_stream_is_fully_buffered(stream)) {
        // if we have a user buffer, this is one large page and thus we can position within it
        page_end = IH_POSITION_OF(stream->_limit);
        if (target_position >= page_end) {
            FAILWITH(IERR_EOF);
        }
        SUCCEED();
    }

    ASSERT(_ion_stream_is_paged(stream)); // we should never get here unless we're in a paged stream
    
    // never mind, it's ok to call this even if the new position is on the current page: ASSERT(IH_CURR_OF( target_position ) < stream->_buffer || IH_CURR_OF( target_position ) >= stream->_limit);

    // where are we? and where do we want to go?
    page = paged->_curr_page;
    current_page_id = page ? page->_page_id : -1; // if we don't have a current page pick an invalid page id
    target_page_id = _ion_stream_page_id_from_offset( stream, target_position );

    // if we are switching pages see if we have the page cached already
    if ( current_page_id != target_page_id ) {
        IONCHECK( _ion_stream_page_find( paged, target_page_id, &page ) );
        if (page == NULL) {            // we don't have the page we want. So we have to create the target page so
            // we can fill this page shortly
            IONCHECK( _ion_stream_page_allocate( paged, target_page_id, &page ) );
            new_page = TRUE;
        }
    }

    ASSERT( page );
    ASSERT( page->_page_id == target_page_id );

    // if the page hasn't been filled, then we need to fill it
    page_end  = _ion_stream_offset_from_page_id( stream, page->_page_id );
    page_end +=  page->_page_start;
    page_end +=  page->_page_limit;

    if ( page_end <= target_position ) {
        //
        //  This calls fetch and fill page to do the actual read
        //
        err = _ion_stream_fetch_fill_page( stream, page, target_position );
        if (err != IERR_OK) {
            // if we were not able to read the needed bytes, we *may* need to
            // free this page (to avoid leaking pages at EOF, for example)
            // so - if there are no bytes on page, and it's not the current page ...
            if (page->_page_limit < 1 && page != paged->_curr_page) {
                _ion_stream_page_release(paged, page);
            }
            if (err == IERR_EOF) {
                DONTFAILWITH( err );
            }
            else {
                FAILWITH(err);
            }
        }
        // in leiu of the "page make current" (below) which sets stream->_limit
        if ( current_page_id == target_page_id ) {
            stream->_limit = page->_buf + page->_page_limit;
        }
    }
    else {
        ASSERT(page->_page_start == 0); // TODO: BUGBUG: What is we backed up onto this page through "unread" and the choose to seek back to it ??
    }
    ASSERT( page );
    ASSERT( page->_page_id == target_page_id );
    
    // page is filled and allocated - make it current (if it isn't already)
    if (new_page) {
        IONCHECK( _ion_stream_page_register( paged, page ) );
    }
    if ( current_page_id != target_page_id ) {
        IONCHECK( _ion_stream_page_make_current( paged, page ) );
    }
    stream->_curr = IH_CURR_OF( target_position );
    ASSERT(
             (_ion_stream_current_page_contains_position( stream, target_position ) == TRUE)
           ||   
             ( target_position ==(page_end = IH_POSITION_OF( stream->_curr )) )
          );
    SUCCEED();

    iRETURN;
}

iERR _ion_stream_fetch_fill_page( ION_STREAM *stream, ION_PAGE *page, POSITION target_position )
{
    iENTER;
    ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
    POSITION          page_read_position;
    BYTE             *dst, *end;
    SIZE              end_buf_offset, bytes_needed_user, bytes_needed_buffer, local_bytes_read;
    
    ASSERT(stream);
    ASSERT(_ion_stream_is_paged(stream));
    ASSERT(target_position >= 0);
    ASSERT(page);

    end_buf_offset = page->_page_start + page->_page_limit;

    page_read_position = _ion_stream_offset_from_page_id(stream, page->_page_id);
    page_read_position += end_buf_offset;

    ASSERT((target_position - page_read_position) < MAX_SIZE);

    bytes_needed_user = bytes_needed_buffer = (SIZE)(target_position - page_read_position) + 1;
    if (bytes_needed_user < (stream->_buffer_size - end_buf_offset)) {
        bytes_needed_buffer = (stream->_buffer_size - end_buf_offset);
    }

    if (_ion_stream_is_file_backed(stream) && _ion_stream_can_read(stream)) {

        // first position ourselves for the read
        IONCHECK( _ion_stream_fseek( stream, page_read_position ) );

        // we will read directly into the page buffer between these two pointers
        dst = &(page->_buf[end_buf_offset]);
        end = dst + bytes_needed_buffer;
        IONCHECK(_ion_stream_fread( stream, dst, end, &local_bytes_read ));
        if (local_bytes_read < 0) {
            // the read functions return negative lengths for unusual read conditions
            if (local_bytes_read == READ_EOF_LENGTH) {
                DONTFAILWITH(IERR_EOF);
            }
            FAILWITH(IERR_READ_ERROR);
        }

        // update our page state to match the read (if we actually were able to read something)
        page->_page_limit += local_bytes_read;
    }
    else {
        // if we're not file backed there's really nothing to do
        // since it's either an in memory buffer, which is already filled,
        // or it's self contained buffer (which are simply 0)
    }
    SUCCEED();

    iRETURN;
}

// _ion_stream_fseek wraps fseek (for normal files) and
// stream only seek files

iERR _ion_stream_fseek( ION_STREAM *stream, POSITION target_position )
{
    iENTER;
    ION_STREAM_PAGED *paged = PAGED_STREAM(stream);

    ASSERT(stream);
    ASSERT(_ion_stream_is_paged(stream));
    ASSERT(_ion_stream_is_file_backed(stream));
    ASSERT(target_position >= 0);

    if (_ion_stream_can_random_seek(stream)) {
        // short cut when we have a random access file backing the stream
		if (_ion_stream_is_fd_backed(stream)) {
			// TODO : should we validate this cast to long somehow?
	        if (LSEEK((int)stream->_fp, (long)target_position, SEEK_SET)) {
		        FAILWITH(IERR_SEEK_ERROR);
			}
		}
		else {
			ASSERT(_ion_stream_is_file_backed(stream));
			if (FSEEK(stream->_fp, target_position, SEEK_SET)) {
				FAILWITH(IERR_SEEK_ERROR);
			}
		}
    }
    else {
        // position ourselves in the read only stream by simply
        // reading bytes until we get where we want to be
        IONCHECK( _ion_stream_read_for_seek( stream, target_position ) );
    }

    SUCCEED();

    iRETURN;
}

// the "stream read for seek" uses a stack variable to hold 1 fake
// page. this fake page is used when calling console read since
// console read always assumes it has a buffer to read into
// these defines specify set up the correct size for the local
// so it can hold the ion_page overhead and some bytes of input
// (console input is generally a slower operation so there's
// no optimization for this case ... yet)

#define LOCAL_BUFFER_SIZE 512
#define LOCAL_FAKE_PAGE_SIZE (sizeof(ION_PAGE) + LOCAL_BUFFER_SIZE)

iERR _ion_stream_read_for_seek( ION_STREAM *stream, POSITION target_position )
{
    iENTER;
    ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
    ION_PAGE         *page, *local_fake_page;
    BYTE              local_fake_buffer[LOCAL_FAKE_PAGE_SIZE], *dst, *end;
    PAGE_ID           current_page_id;
    POSITION          current_position;
    SIZE              bytes_read;

    ASSERT(stream);
    ASSERT(_ion_stream_is_paged(stream));
    ASSERT(target_position >= 0);

    // for a stream the last page is always where
    // we last read (if we've read anything)
    page = paged->_last_page;
    if (page == NULL) {
        // and page is null - this is the first page
        current_page_id  = 0;
        current_position = 0;
    }
    else {
        current_page_id   =  page->_page_id;
        if (page == paged->_curr_page) {
            current_position  = stream->_offset;
            page->_page_start = (SIZE)(stream->_buffer - page->_buf);
            page->_page_limit = (SIZE)(stream->_limit  - page->_buf);
        }
        else {
            current_position  = _ion_stream_offset_from_page_id(stream, current_page_id);
        }
        current_position +=  page->_page_start;
        current_position +=  page->_page_limit;
    }

    // if we aren't caching we'll need somewhere to put the
    // bytes we read - just to throw them away
    // note that since this is a stack allocated value the cost 
    // of setting up the buffer is very small (so we don't bother 
    // to bail out here even when there's nothing to read)
    if (_ion_stream_is_caching(stream) == FALSE) {
        local_fake_page = (ION_PAGE *)local_fake_buffer;
        local_fake_page->_page_start = 0;
        local_fake_page->_page_limit = 0;
        local_fake_page->_next_free = NULL;
    }

    // read input source until we get to the desired target position
    while (current_position < target_position) {
        // see if we need to set up a page to read data into
        if (!page || page->_page_id != current_page_id) {
            // we don't have the right page to put data into (either a
            // real for a fake page)
            if (_ion_stream_is_caching(stream)) {
                // if we are caching the data we need a real page
                IONCHECK( _ion_stream_page_find( paged, current_page_id, &page ) );
                if (page == NULL) {
                    // we don't have the page we want. So we have to create the target page so
                    IONCHECK( _ion_stream_page_allocate( paged, current_page_id, &page ) );
                }
            }
            else {
                // since we're not caching we will read into our local fake page so here we
                // reset to a clean local fake page (we're going to throw it out anyway)
                page = local_fake_page;
                page->_page_id = current_page_id;
                page->_page_limit = 0;
            }
        }
        // set the dst to write to - we write to the unused area of the pages buffer
        dst = page->_buf + page->_page_start + page->_page_limit;
        if (page == local_fake_page) {
            end = local_fake_page->_buf + (sizeof(local_fake_buffer) - sizeof(ION_PAGE));
        }
        else {
            end = page->_buf + paged->_page_size;
        }

        // we shorten end if our target position is inside the current page
        if ((SIZE)(end - dst) > (SIZE)(target_position - current_position)) {
            end = dst + (SIZE)(target_position - current_position);
        }

        if (dst < end) {
            // read the bytes (this will check for the type of input as necessary)
            IONCHECK(_ion_stream_fread( stream, dst, end, &bytes_read ));
            if (bytes_read < 0) {
                if (bytes_read == READ_EOF_LENGTH) {
                    FAILWITH(IERR_EOF);
                }
                FAILWITH(IERR_SEEK_ERROR);
            }

            // update our two positions (buffer and file position)
            current_position += bytes_read;
            dst += bytes_read;
        }

        // we test again since we fall through
        if (dst >= end) {
            // we'll need a new page if we have not finished
            current_page_id++;
        }
    }

    if ( target_position == current_position ) {
        // we read to the desired location successfully (without error or eof)
        SUCCEED();
    }
    else if ( target_position > current_position ) {
        FAILWITH(IERR_EOF);
    }
    else { // if ( target_position < current_position )
        // we can't seek backwards in a console stream
        FAILWITH(IERR_SEEK_ERROR);
    }

    iRETURN;
}

iERR _ion_stream_fread( ION_STREAM *stream, BYTE *dst, BYTE *end, SIZE *p_bytes_read)
{
    iENTER;
    ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
    struct _ion_user_stream *user_stream = NULL;
    SIZE              local_bytes_read, needed, bytes_read = 0;

    ASSERT(stream);
    ASSERT(_ion_stream_is_paged(stream));
    ASSERT(dst && end && end > dst && (end - dst) < MAX_SIZE);
    ASSERT(p_bytes_read);

    // read from the console or the file stream - it'll be one or the other
    if (_ion_stream_is_tty(stream)) {
        //
        // read from the console stream until the page buffer is filled
        //
        bytes_read = 0;

        while (dst < end) {
            IONCHECK(_ion_stream_console_read( stream, dst, end, &local_bytes_read ));
            if (local_bytes_read == READ_EOF_LENGTH && bytes_read > 0) {
                break;
            }
            if (local_bytes_read < 0) {
                // we always return a read error, we only return EOF if we didn't read anything
                bytes_read = local_bytes_read;
                break; // on either READ_ERRORLENGTH or READ_EOF_LENGTH
            }
            dst += local_bytes_read;
            bytes_read += local_bytes_read;
        }
    }
    else if (_ion_stream_is_user_controlled(stream)) {
        //
        // read from the user managed old style stream
        //
        user_stream = &(((ION_STREAM_USER_PAGED *)stream)->_user_stream);
        // first check to see if we have any pending bytes, from a previous read
        if (user_stream->curr == NULL 
         || user_stream->limit == NULL
         || ((bytes_read = (SIZE)(user_stream->limit - user_stream->curr)) < 1)
        ) {
          // if we didn't have anything, call the handler to get more bytes
          switch((err = (*(user_stream->handler))(user_stream))) {
          case IERR_OK:
            bytes_read = (SIZE)(user_stream->limit - user_stream->curr);
            break;
          case IERR_EOF:
            bytes_read = 0;
            break;
          default:
            bytes_read = READ_ERROR_LENGTH;
            break;
          }
        }
        // now, if we have bytes and no error, copy over what we got, or a page full
        // whichever is shorter (if we don't copy the whole page we'll get more on our next pass)
        if (bytes_read > 0) {
          needed = (SIZE)(end - dst);
          if (bytes_read > needed) {
              bytes_read = needed;
          }
          memcpy(dst, user_stream->curr, bytes_read);
          user_stream->curr += bytes_read; // move the handlers cursor forward
        }
    }
    else {
        //
        // read a page from the underlying FILE*
        //
        local_bytes_read = (SIZE)(end - dst);
		if (_ion_stream_is_fd_backed(stream)) {
	        bytes_read = (SIZE)READ((int)stream->_fp, dst, local_bytes_read);
		    if (bytes_read < 0) {
			    bytes_read = READ_ERROR_LENGTH;
			}	
		    else if (bytes_read == 0) {
			    bytes_read = READ_EOF_LENGTH;
			}	
		}
		else {
			bytes_read = (SIZE)fread(dst, sizeof(BYTE), local_bytes_read, stream->_fp);
			if (ferror(stream->_fp)) {
				bytes_read = READ_ERROR_LENGTH;
			}
		}
    }

    *p_bytes_read = bytes_read;
    SUCCEED();

    iRETURN;
}


iERR _ion_stream_console_read( ION_STREAM *stream, BYTE *buf, BYTE *end, SIZE *p_bytes_read)
{
    iENTER;
    ION_STREAM_PAGED *paged = PAGED_STREAM(stream);
    BYTE             *dst = buf;
    int               c;
    SIZE              bytes_read;
    BOOL              saw_cr = FALSE, anything_read = FALSE;

    ASSERT(stream);
    ASSERT(_ion_stream_is_paged(stream));
    ASSERT(_ion_stream_is_tty(stream));
    ASSERT(dst && end && end > dst);
    ASSERT(p_bytes_read);

    // read bytes until we either reqad enough to fill the
    // request, or we hit a new line or EOF or an error
    while (dst < end) {
        if ((c = getc(stream->_fp)) < 0) {
            if (ferror(stream->_fp)) {
                bytes_read = READ_ERROR_LENGTH;
                goto break_on_error;
            }
            if (feof(stream->_fp)) goto break_on_eof;
        }
        *dst++ = IH_MAKE_BYTE(c);
        anything_read = TRUE;
        if (c == '\n' || saw_cr) break;
        // TODO: do we really want to do this or just let the caller call again?
        if (c == '\r') saw_cr = TRUE;
    }
    // fall through to "break_on_eof", since we've read enough

break_on_eof: 
    bytes_read = (SIZE)(dst - buf);
    if (!anything_read && feof(stream->_fp)) { // was, but it seems the optimizer didn't like this: bytes_read == 0 && feof(stream->_fp)) {
        // we only treat the eof as an eof if we
        // were not able to read any bytes
        bytes_read = READ_EOF_LENGTH;
    }
    // fall through to break_on_error - we'll let the caller deal with the error
       
break_on_error:
    // the return bytes read will either be the amount read, or a negative
    // length which signals either error or eof
    *p_bytes_read = bytes_read;
    SUCCEED();

    iRETURN;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////

//            PAGE ROUTINES - these manage pages for the paged streams

//////////////////////////////////////////////////////////////////////////////////////////////////////

// perf matters here, somewhat, but for the moment I'm going for obvious
// and depend on the compiler optimization to make this reasonable (and
// it's not _that_ bad as is anyway)
int_fast8_t _ion_stream_page_compare_page_ids(void *key1, void *key2, void *context)
{
  PAGE_ID     page_id1, page_id2;
  int_fast8_t compares;
   
  ASSERT(key1);
  ASSERT(key2);
   
  page_id1 = *((PAGE_ID *)key1);
  page_id2 = *((PAGE_ID *)key2);
   
  if (page_id1 == page_id2)     compares =  0;
  else if (page_id1 > page_id2) compares =  1;
  else                          compares = -1;
  
  return compares;
}

int_fast32_t _ion_stream_page_hash_page_id(void *key, void *context)
{
  int_fast32_t hash;
   
  ASSERT(key);
   
  hash = *((int_fast32_t *)key);
  return hash;
}

iERR _ion_stream_page_allocate(ION_STREAM_PAGED *paged, PAGE_ID page_id, ION_PAGE **pp_page)
{
  iENTER;
  ION_PAGE *page = NULL;
  SIZE      size;
   
  ASSERT(paged);
  ASSERT(pp_page);
  ASSERT(page_id >= 0);
  ASSERT(_ion_stream_is_paged((ION_STREAM *)paged));
   
  page = paged->_free_pages;
  if (page) {
    // since we have a free page on the list - use it
    paged->_free_pages = page->_next_free;
  }
  else {
    // if there isn't any free page in the queue - allocate a new page
    size = paged->_page_size + sizeof(ION_PAGE); // we'll allocate the struct and it's buffer in one piece
    page = _ion_alloc_with_owner(paged, size);
    if (!page) FAILWITH(IERR_NO_MEMORY);
  }

  // initialize the page for use
  page->_next_free  = NULL;
  page->_page_id    = page_id;
  page->_page_start = 0;
  page->_page_limit = 0;
   
  *pp_page = page;
  SUCCEED();
   
  iRETURN;
}

void _ion_stream_page_release(ION_STREAM_PAGED *paged, ION_PAGE *page )
{
  PAGE_ID   page_id;
  ION_PAGE *test_page;

  ASSERT(paged);
  ASSERT(_ion_stream_is_paged((ION_STREAM *)paged));
  ASSERT(page);
  
  // add this to the index - 
  page_id = page->_page_id;
  test_page = _ion_index_find(&(paged->_index), &page_id);
  if (test_page == page) {
    _ion_index_delete(&(paged->_index), &page_id, &test_page);
    ASSERT(test_page == page);
  }

  // if we are releasing the last page we need to patch back in the previous last page
  // the cases are:
  //    page being released is earlier in the file - in which case page will not be last page
  //    page was just allocated ahead of the current page but then dropped
  //    ??
  if (page == paged->_last_page) {
    if (page_id > 0) {
      page_id--;
      test_page = _ion_index_find(&(paged->_index), &page_id);
    }
    else {
      test_page = NULL;
    }
    paged->_last_page = test_page;
  }
  _ion_stream_page_clear(page);

#ifdef MEM_DEBUG
  // do nothing - since this memory is owned by the stream anyway
#else
  // push this page onto our stack
  page->_next_free = paged->_free_pages;
  paged->_free_pages = page;
#endif

  return;
}

void _ion_stream_page_clear(ION_PAGE *page)
{
  if (page) {
    page->_next_free  = NULL;
    page->_page_id    = -1;
    page->_page_start = 0;
    page->_page_limit = 0;
  }
}

iERR _ion_stream_page_register( ION_STREAM_PAGED *paged, ION_PAGE *page )
{
  iENTER;
  ION_PAGE *test_page;
  
  ASSERT(paged);
  ASSERT(_ion_stream_is_paged((ION_STREAM *)paged));
  ASSERT(page);

  // add this to the index - 
  test_page = _ion_index_find(&(paged->_index), &(page->_page_id));
  if (test_page) {
    if (test_page == page) {
      SUCCEED();
    }
    // we should never have two pages with the same id !
    FAILWITH(IERR_INTERNAL_ERROR);
  }
  IONCHECK(_ion_index_insert(&(paged->_index), &(page->_page_id), page));
 
  // keep our "last page read" up to date (aka furthest page read)
  if (!paged->_last_page || (page->_page_id > paged->_last_page->_page_id)) {
    paged->_last_page = page;
  }
  
  SUCCEED();
  
  iRETURN;
}

iERR _ion_stream_page_find(ION_STREAM_PAGED *paged, PAGE_ID page_id, ION_PAGE **pp_page)
{
  iENTER;
  ION_PAGE *page;
 
  ASSERT(paged);
  ASSERT(page_id >= 0);
  ASSERT(pp_page);
   
  page = (ION_PAGE *)_ion_index_find(&(paged->_index), &page_id);
  *pp_page = page;
   
  SUCCEED();
  
  iRETURN;
}

iERR _ion_stream_page_make_current(ION_STREAM_PAGED *paged, ION_PAGE *page)
{
  iENTER;
  ION_STREAM *stream = UNPAGED_STREAM(paged);
  ION_PAGE   *currpage;
   
  ASSERT(paged);
  ASSERT(page);

  // if we have a dirty bytes, flush them out first
  if (_ion_stream_is_dirty(stream )) {
    IONCHECK(_ion_stream_flush_helper(stream));
  }

  // determine the fate of the current page (if there is one)
  currpage = paged->_curr_page;
  if (currpage) {
      if (!_ion_stream_is_caching(stream) && currpage->_page_id < page->_page_id) {
          // if we're not cacheing release the (now old) curr page
        _ion_stream_page_release(paged, paged->_curr_page);
      }
      else {
        // if we're not releasing the page we need to update the page values
        currpage = paged->_curr_page;
        currpage->_page_start = (SIZE)(stream->_buffer - currpage->_buf);
        currpage->_page_limit = (SIZE)(stream->_limit  - currpage->_buf);
      }
  }
   
  // note this is same work takes place in ion_stream_open_buffered
  //      where the constructor fakes this setup
  // set the stream fields to make the page active
  stream->_buffer   =  page->_buf;                                 // zxxxx
  stream->_offset   = _ion_stream_offset_from_page_id(stream, page->_page_id);
  stream->_limit    =  page->_buf + page->_page_start + page->_page_limit;  /// zxxxx  huh? was on 2072
  stream->_curr     =  page->_buf + page->_page_start;
  paged->_curr_page =  page;
  if (!paged->_last_page || paged->_last_page->_page_id < page->_page_id) {
      paged->_last_page = page;
  }
  SUCCEED();
  
  iRETURN;
}  
  
iERR _ion_stream_page_get_last_read(ION_STREAM *stream, ION_PAGE **pp_page)
{
  iENTER;
  ION_PAGE *page = NULL;
  
  ASSERT(stream);
  ASSERT(pp_page);
   
  if (_ion_stream_is_paged(stream)) {
    page = PAGED_STREAM(stream)->_last_page;
  } // else fall through with page == NULL

  *pp_page = page;
  SUCCEED();
   
  iRETURN;
}

