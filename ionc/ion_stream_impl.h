/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 
#ifndef ION_STREAM_IMPL_H_
#define ION_STREAM_IMPL_H_

#include "ion_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BYTE_MASK 0xff
typedef uint32_t  ION_STREAM_FLAG;

// the initial flag bits make up the type of the stream
#define FLAG_CAN_READ           0x00100
#define FLAG_CAN_WRITE          0x00200
#define FLAG_RANDOM_ACCESS      0x00400
#define FLAG_IS_TTY             0x00800
#define FLAG_USER_HANDLING      0x01000
#define FLAG_IS_FILE_BACKED     0x02000
#define FLAG_IS_FD_BACKED       0x04000
#define FLAG_BUFFER_ALL         0x08000
#define FLAG_IS_USER_BUFFER     0x10000

// the low order bits are "operational" flags that
// may be turned on or off during runtime
#define FLAG_IS_OPEN            0x0001
#define FLAG_IS_DIRTY           0x0002
#define FLAG_IS_ANY_UPDATE      0x0004
#define FLAG_IS_AT_EOF          0x0008
#define FLAG_IS_FAKE_PAGE       0x0010

#define ION_STREAM_FILE_IN      (FLAG_IS_FILE_BACKED | FLAG_CAN_READ                   | FLAG_RANDOM_ACCESS )
#define ION_STREAM_FILE_OUT     (FLAG_IS_FILE_BACKED |                  FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS )
#define ION_STREAM_STDIN        (FLAG_IS_FILE_BACKED | FLAG_CAN_READ                                        | FLAG_IS_TTY)
#define ION_STREAM_STDOUT       (FLAG_IS_FILE_BACKED |                  FLAG_CAN_WRITE                      | FLAG_IS_TTY)
#define ION_STREAM_STDERR       (FLAG_IS_FILE_BACKED |                  FLAG_CAN_WRITE                      | FLAG_IS_TTY)
#define ION_STREAM_FILE_RW      (FLAG_IS_FILE_BACKED | FLAG_CAN_READ  | FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS)
#define ION_STREAM_USER_BUF     (FLAG_BUFFER_ALL     | FLAG_CAN_READ  | FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS | FLAG_IS_USER_BUFFER)
#define ION_STREAM_MEMORY_ONLY  (FLAG_BUFFER_ALL     | FLAG_CAN_READ  | FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS )

#define ION_STREAM_FD_IN        (FLAG_IS_FD_BACKED   | FLAG_CAN_READ                   | FLAG_RANDOM_ACCESS )
#define ION_STREAM_FD_OUT       (FLAG_IS_FD_BACKED   |                  FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS )
#define ION_STREAM_FD_RW        (FLAG_IS_FD_BACKED   | FLAG_CAN_READ  | FLAG_CAN_WRITE | FLAG_RANDOM_ACCESS )

#define ION_STREAM_USER_IN      (FLAG_IS_FILE_BACKED | FLAG_CAN_READ                                        | FLAG_USER_HANDLING)
#define ION_STREAM_USER_OUT     (FLAG_IS_FILE_BACKED |                  FLAG_CAN_WRITE                      | FLAG_USER_HANDLING)

#define FLAG_IS_CLOSED          (FLAG_IS_AT_EOF | FLAG_IS_FAKE_PAGE)

#define MARK_NOT_STARTED        (-1)

#define READ_EOF_LENGTH         (-1)
#define READ_ERROR_LENGTH       (-2)

#define IH_DEFAULT_PAGE_SIZE    (1024*8)

GLOBAL SIZE g_Ion_Stream_Default_Page_Size INITTO(IH_DEFAULT_PAGE_SIZE);  // a global so we could choose to change a runtime with effort

struct _ion_stream
{
  ION_STREAM_FLAG   _flags;        // CAN_READ, CAN_WRITE, RANDOM_ACCESS, CONSOLE_HANDLING, 
                                  // IS_FILE_BACKED, BUFFER_ALL, IS_USER_BUFFER, 
                                  // IS_OPEN, IS_DIRTY, IS_AT_EOF, IS_FAKE_PAGE, 
  FILE            *_fp;           // current file for read or read/write (will be NULL if user buffer)
  BYTE            *_buffer;       // current page of data in use
  SIZE             _buffer_size;  // current buffer size, this is page size if the is a paged stream, otherwise it's the one and only buffer length
  POSITION         _offset;       // offset of first byte of buffer in source
  BYTE            *_curr;         // position = _offset + (_curr - _buffer)
  BYTE            *_limit;        // end of buffered data

  POSITION         _mark;         // -1 for no mark otherwise the file position where the mark started

  BYTE            *_dirty_start;  // pointer to first dirty byte in current buffer
  SIZE             _dirty_length; // number of dirty bytes (only contiguous bytes in the current buffer are allowed to be dirty)
};

struct _ion_stream_paged // extends _ion_stream
{
  ION_STREAM        _base;
  ION_PAGE         *_curr_page;   // the current page we're on (may be NULL) TODO - should this be an idx?
  ION_PAGE         *_last_page;   // this is really the furthest page read, used for sequential reads
  SIZE              _page_size;   // size of buffers in all pages
  ION_PAGE         *_free_pages;  // list of allocated pages but unused pages 
  // the ION_INDEX is a hashed index which requires pages to all be the same 
  // size so that locations can be converted to page numbers functionally
  ION_INDEX         _index;       // index into current pages by page_offset (9 ptrs, 6 int32's, 1 byte == 61 or 97 bytes)
}; // ( 15 ptrs, 9 int32's, 1 byte = 97 - 157 bytes) which means it's probably still worth having the two structs

struct _ion_stream_user_paged // extends _ion_stream_paged
{
  struct _ion_stream_paged _paged_base;
  struct _ion_user_stream  _user_stream;
}; // (157 bytes + 4 ptrs) 

struct _ion_page
{
  ION_PAGE         *_next_free;
  PAGE_ID           _page_id;     // which page in the file is this (this equals position modulo page size)
  SIZE              _page_start;  // offset of the first byte of filled data, this is 0 unless the page has been filled via unread
  SIZE              _page_limit;  // number of bytes filled in the current page buf
  BYTE              _buf[0];      // buffer of bytes, the size is _ion_stream_paged->_page_size
};

#define IH_IS_BYTE(b) (((b) & ~(BYTE_MASK)) == 0)
/* make sure we don't have any wierd sign extension going on, although that may be just a Java issue */
#define IH_MAKE_BYTE(b) ((BYTE)( (b) & BYTE_MASK ))

#define STREAM_FLAGS(stream) ((stream)->_flags)

#define PAGED_STREAM( stream )  ((ION_STREAM_PAGED *)(stream))
#define UNPAGED_STREAM( paged ) ((ION_STREAM *)(&(paged->_base)))
#define IH_POSITION_OF( ptr )   (stream->_offset + ((ptr) - stream->_buffer))
#define IH_CURR_OF( pos )       (stream->_buffer + ((pos) - stream->_offset))  /* WARNING: this might need a cast of the pos-offset to SIZE */

#define ION_INPUT_STREAM_POSITION(s) ion_stream_get_position(s)


//////////////////////////////////////////////////////////////////////////////////////////////////////////

//                     internal constructor helpers

//////////////////////////////////////////////////////////////////////////////////////////////////////////

iERR _ion_stream_open_helper( ION_STREAM_FLAG flags, SIZE page_size, ION_STREAM **pp_stream );
iERR _ion_stream_flush_helper( ION_STREAM *stream );

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//  internal getters and other informational functions
//  (these are expected to be inlined by the compiler)
  
//////////////////////////////////////////////////////////////////////////////////////////////////////////

BOOL      _ion_stream_is_mark_open        ( ION_STREAM *stream );
BOOL      _ion_stream_can_random_seek     ( ION_STREAM *stream );
BOOL      _ion_stream_can_seek_to         ( ION_STREAM *stream, POSITION pos );
BOOL      _ion_stream_can_read            ( ION_STREAM *stream );
BOOL      _ion_stream_can_write           ( ION_STREAM *stream );
BOOL      _ion_stream_is_dirty            ( ION_STREAM *stream );
BOOL      _ion_stream_is_file_backed      ( ION_STREAM *stream );
BOOL      _ion_stream_is_fd_backed        ( ION_STREAM *stream );
BOOL      _ion_stream_is_tty              ( ION_STREAM *stream );
BOOL      _ion_stream_is_user_controlled  ( ION_STREAM *stream );
BOOL      _ion_stream_is_paged            ( ION_STREAM *stream );
BOOL      _ion_stream_is_fully_buffered   ( ION_STREAM *stream );
BOOL      _ion_stream_is_caching          ( ION_STREAM *stream );

FILE *    _ion_stream_get_file_stream     ( ION_STREAM *stream );
POSITION  _ion_stream_get_mark_start      ( ION_STREAM *stream );
POSITION  _ion_stream_get_marked_length   ( ION_STREAM *stream );
iERR      _ion_stream_mark_clear_helper   ( ION_STREAM_PAGED *paged, POSITION position );

PAGE_ID   _ion_stream_page_id_from_offset ( ION_STREAM *stream, POSITION file_offset );
POSITION  _ion_stream_offset_from_page_id ( ION_STREAM *stream, PAGE_ID page_id );
POSITION  _ion_stream_page_start_offset   ( ION_STREAM *stream, POSITION file_offset );
POSITION  _ion_stream_position            ( ION_STREAM *stream );

BOOL      _ion_stream_current_page_contains_position( ION_STREAM *stream, POSITION position );

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//            internal instance functions

//////////////////////////////////////////////////////////////////////////////////////////////////////////

iERR _ion_stream_fetch_position           ( ION_STREAM *stream, POSITION position );
iERR _ion_stream_fetch_fill_page          ( ION_STREAM *stream, ION_PAGE *page, POSITION target_position );
iERR _ion_stream_fseek                    ( ION_STREAM *stream, POSITION target_position );
iERR _ion_stream_read_for_seek            ( ION_STREAM *stream, POSITION target_position );
iERR _ion_stream_fread                    ( ION_STREAM *stream, BYTE *dst, BYTE *end, SIZE *p_bytes_read);
iERR _ion_stream_console_read             ( ION_STREAM *stream, BYTE *dst, BYTE *end, SIZE *p_bytes_read);

//////////////////////////////////////////////////////////////////////////////////////////////////////

//            PAGE ROUTINES - these manage pages for the paged streams

//////////////////////////////////////////////////////////////////////////////////////////////////////

int_fast8_t  _ion_stream_page_compare_page_ids(void *key1, void *key2, void *context );
int_fast32_t _ion_stream_page_hash_page_id(void *key, void *context );

iERR _ion_stream_page_allocate      ( ION_STREAM_PAGED *paged, PAGE_ID page_id, ION_PAGE **pp_page );
void _ion_stream_page_release       ( ION_STREAM_PAGED *paged, ION_PAGE *page );
void _ion_stream_page_clear         ( ION_PAGE *page );
iERR _ion_stream_page_register      ( ION_STREAM_PAGED *paged, ION_PAGE *page );
iERR _ion_stream_page_find          ( ION_STREAM_PAGED *paged, PAGE_ID page_id, ION_PAGE **pp_page );
iERR _ion_stream_page_make_current  ( ION_STREAM_PAGED *paged, ION_PAGE *page );
iERR _ion_stream_page_get_last_read ( ION_STREAM *stream, ION_PAGE **pp_page );


#ifdef __cplusplus
}
#endif

#endif /* ION_STREAM_IMPL_H_ */
