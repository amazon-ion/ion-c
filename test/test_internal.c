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

#include "tester.h"
#include <ion_internal.h>

#define READ_ALL_BUFFER_SIZE (80*1024)

iERR test_reader_read_all_just_file(hREADER hreader)
{
    iENTER;
    ION_READER *tw = (ION_READER *)hreader;
    ION_STREAM *is = tw->istream;
    BYTE        buf[READ_ALL_BUFFER_SIZE];
    SIZE        bytes_read;

    for(;;) {
        // err = ion_input_stream_next_block(is);
        err = ion_stream_read(is, buf, READ_ALL_BUFFER_SIZE, &bytes_read);
        if (err == IERR_EOF) {
            SUCCEED();
        }
        IONDEBUG(err, "ion stream read");
    }

    iRETURN;
}


iERR test_reader_read_all_just_file_byte_by_byte(hREADER hreader)
{
    iENTER;
    ION_READER *tw = (ION_READER *)hreader;
    ION_STREAM *is = tw->istream;
    int b = 0;

    for(;;) {
        // ION_READ2(is, b);
        ION_GET(is, b);
        if (b < 0) break;
    }

    SUCCEED();
    iRETURN;
}

iERR test_reader_read_all(hREADER hreader)
{
    iENTER;
    ION_TYPE t, t2;
    BOOL     more;
    for (;;) {
        IONDEBUG(ion_reader_next(hreader, &t), "read next");
        if (t == tid_EOF) {
            // TODO IONC-4 does next() return tid_EOF or tid_none at end of stream?
            // See ion_parser_next where it returns tid_none
            assert(t == tid_EOF && "next() at end");
            more = FALSE;
        }
        else {
            more = TRUE;
        }

        IONDEBUG(ion_reader_get_type(hreader, &t2), "get_type");
//      assert(t != t2 && "get_type() should match next()");    // TODO IONC-5

        if (!more) break;

        
        if (g_no_print) {
            IONDEBUG(test_reader_read_value_no_print(hreader, t), "read & dump value");
        }
        else {
            char * type_name = test_get_type_name(t);
            IONDEBUG(test_reader_read_value_print(hreader, t, type_name), "read & dump value");
        }
    }

    IF_PRINT("reader done\n");

    iRETURN;
}



SIZE copy_to_bytes(const char *image, BYTE *bytes, int limit)
{
    const char *cp = image;
    BYTE *bp = bytes;
    int   c, len = 0;

    if (!limit || !image) {
        return 0;
    }
    assert(bytes != NULL);

    while(*cp && len < limit) {
        c = *cp++;
        assert(c < 256 && "copy_to_bytes only handles byte sized characters <= 255");
        *bp++ = c;
        len++;
    }
    return len;
}

