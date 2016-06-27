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
//  helper routines for opening a stream and an ion reader or writer
//

#include "ionizer.h"
#include "options.h"
#include "memory.h"

iERR ionizer_reader_open_fstream( 
        FSTREAM_READER_STATE **p_read_helper, 
        FILE *fp_in, 
        ION_READER_OPTIONS *options 
) {
    iENTER;
    FSTREAM_READER_STATE *read_helper = NULL;
    ION_STREAM *stream = NULL;
    hREADER     hreader = 0;

    if (!fp_in || !p_read_helper) FAILWITH(IERR_INVALID_ARG);
    if (fp_in == stdout) {
        FAILWITH(IERR_INVALID_ARG);
    }

    read_helper = (FSTREAM_READER_STATE *)malloc(sizeof(FSTREAM_READER_STATE));
    if (!read_helper) FAILWITH(IERR_NO_MEMORY);

    if (fp_in == stdin) {
        CHECK(ion_stream_open_stdin( &stream ), "opening a stream over stdin");
    }
    else {
        CHECK(ion_stream_open_file_in( fp_in, &stream ), "opening an input stream");
    }

    CHECK(ion_reader_open( &hreader, stream, options), "opening the ion reader");

    read_helper->hreader = hreader;
    read_helper->in = stream;
    *p_read_helper = read_helper;
    SUCCEED();

fail:
    if (err != IERR_OK) {
        if (hreader) ion_reader_close(hreader);
        if (stream) ion_stream_close(stream);
        if (read_helper) free(read_helper);
    }
    return err;
}

iERR ionizer_reader_close_fstream( FSTREAM_READER_STATE *read_helper )
{
    iENTER;
    hREADER     h = 0;
    ION_STREAM *s = NULL;
    FILE       *f = NULL;

    if (!read_helper) FAILWITH(IERR_INVALID_ARG);
    
    if (read_helper->hreader) {
        h = read_helper->hreader;
        read_helper->hreader= 0;
        CHECK(ion_reader_close(h), "closing an ion writer");
    }
    if (read_helper->in) {
        s = read_helper->in;
        read_helper->in = NULL;
        f = ion_stream_get_file_stream(s);
        CHECK(ion_stream_close(s), "closing an input stream");
        if (f == stdin) {
            // DO NOTHING
        }
        else if (f == stdout) {
            // do nothing, but we shouldn't be here anyway
        }
        else {
            fclose(f);
        }
    }
    free(read_helper);
    SUCCEED();


    iRETURN;
}

iERR ionizer_writer_open_fstream( 
        FSTREAM_WRITER_STATE **p_write_helper, 
        FILE *fp_out, 
        ION_WRITER_OPTIONS *options)
{
    iENTER;
    FSTREAM_WRITER_STATE *write_helper = NULL;
    ION_STREAM *stream = NULL;
    hWRITER     hwriter = 0;

    if (!fp_out || !p_write_helper) FAILWITH(IERR_INVALID_ARG);
    if (fp_out == stdin) {
        FAILWITH(IERR_INVALID_ARG);
    }

    write_helper = (FSTREAM_WRITER_STATE *)malloc(sizeof(FSTREAM_WRITER_STATE));
    if (!write_helper) FAILWITH(IERR_NO_MEMORY);

    if (fp_out == stdout) {
        CHECK(ion_stream_open_stdout( &stream ), "opening a stdout as stream");
    }
    else {
        CHECK(ion_stream_open_file_out( fp_out, &stream ), "opening an output stream");
    }

    CHECK(ion_writer_open( &hwriter, stream, options), "opening the ion writer");

    write_helper->hwriter = hwriter;
    write_helper->out = stream;
    *p_write_helper = write_helper;
    SUCCEED();

fail:
    if (err != IERR_OK) {
        if (hwriter) ion_writer_close(hwriter);
        if (stream) ion_stream_close(stream);
        if (write_helper) free(write_helper);
    }
    return err;

}

iERR ionizer_writer_close_fstream( FSTREAM_WRITER_STATE *write_helper )
{
    iENTER;
    hWRITER     h;
    ION_STREAM *s;
    FILE       *f;

    if (!write_helper) FAILWITH(IERR_INVALID_ARG);
    if (write_helper->hwriter) {
        h = write_helper->hwriter;
        write_helper->hwriter = 0;
        CHECK(ion_writer_close(h), "closing an ion writer");
    }
    if (write_helper->out) {
        s = write_helper->out;
        write_helper->out = 0;
        f = ion_stream_get_file_stream(s);
        if (f == stdin) {
            // DO NOTHING
        }
        else if (f == stdout) {
            fflush(stdout);
        }
        else {
            fclose(f);
        }
    }
    free(write_helper);
    SUCCEED();

    iRETURN;
}

