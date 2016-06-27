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
// add on to initial testing program
//
// this loops through a set of files for regression tests
//

#include <ion_platform_config.h>

#ifdef ION_PLATFORM_WINDOWS
    #include <io.h>
#else
    #include <dirent.h>
#endif

#include "tester.h"


//============================================================================
// Basic file filtering

char *g_global_skip_list[] =
{
    "bad/equivs/utf8/stringUtf8.ion",
    "bad/symbolExplicitZero.10n",

    "good/decimalsWithUnderscores.ion",
    "good/intBinary.ion",
    "good/intsWithUnderscores.ion",
    "good/symbolEmpty.ion",
    "good/symbolEmptyWithCR.ion",
    "good/symbolEmptyWithCRLF.ion",
    "good/symbolEmptyWithLF.ion",
    "good/symbolEmptyWithLFLF.ion",
    "good/utf16.ion",
    "good/utf32.ion",

    "good/equivs/binaryInts.ion",
    "good/equivs/decimalsWithUnderscores.ion",
    "good/equivs/intsWithUnderscores.ion",

    // Unprintable fractions (only break when debug printing is enabled)
    "good/equivs/timestampFractions.10n",
    "good/timestamp/equivTimeline/timestamps.ion",
    NULL
};

BOOL string_ends_with(char* s, char* suffix) {
    size_t sLength = strlen(s);
    size_t suffixLength = strlen(suffix);

    if (sLength < suffixLength) {
        return FALSE;
    }

    char* sSuffix = s + (sLength - suffixLength);
    return 0 == strcmp(sSuffix, suffix);
}

BOOL is_always_skipped(char* filename)
{
    int   i;

    char *skip;

    for (i = 0; g_global_skip_list[i] != 0; i++) {
        skip = g_global_skip_list[i];
        if (string_ends_with(filename, skip)) {
            return TRUE;
        }
    }
    return FALSE;
}

BOOL suffix_matches(char* filename, char* suffix)
{
    size_t suffixLen = strlen(suffix);
    size_t fileLen = strlen(filename);

    if ((fileLen > suffixLen)
        && (0 == strcmp(suffix, filename + (fileLen - suffixLen))))
    {
        return TRUE;
    }
    return FALSE;
}

BOOL is_ion_text(char* filename)
{
    return suffix_matches(filename, ".ion");
}

BOOL is_ion_binary(char* filename)
{
    return suffix_matches(filename, ".10n");
}

BOOL is_ion(char* filename)
{
    return is_ion_text(filename) || is_ion_binary(filename);
}

//============================================================================

#ifdef ION_PLATFORM_WINDOWS

struct dirent {
    char d_name[_MAX_PATH + 6]; // +6 for *.* etc.
};

typedef struct _dir {
    struct _finddata_t fileinfo;
    intptr_t           fh;
    struct dirent      ent;
    BOOL               eof;
} DIR;

#define DIR_OPEN(pname) win_open(pname)
#define DIR_NEXT(pdir)  win_next(pdir)     
#define DIR_CLOSE(pdir) win_close(pdir)
void win_close(DIR *pdir);

char *win_fixname(char *pname)
{
    char    *cp, *fixedname;
    int32_t  len;

    if (!pname || !*pname) return NULL;
    len = strlen(pname);
    if (len < 0) return NULL;

    fixedname = malloc(len + 5); // + 5 for: '/' "*.*"  '\0'
    if (!fixedname) return NULL;

    memcpy(fixedname, pname, len + 1);
    for (cp = fixedname; *cp; cp++) {
        if (*cp == '/') *cp = '\\';    // switch to windows slash
    }

    return fixedname;
}

DIR *win_open(char *pname) 
{
    char    *fixedname = win_fixname(pname);
    DIR     *pdir      = (DIR *)malloc(sizeof(DIR));
    int32_t  fixednamelen;

    if (pdir && fixedname) {
        memset(pdir, 0, sizeof(DIR));
        pdir->fh = _findfirst(fixedname, &(pdir->fileinfo));
        if (pdir->fileinfo.attrib & _A_SUBDIR) {
            _findclose( pdir->fh );
            fixednamelen = strlen(fixedname);
            assert(fixednamelen > 0); // a 0 length name is not ok, nor is an int overflow
            if (fixedname[fixednamelen - 1] != '\\') {
                memcpy(fixedname + fixednamelen, "\\", 2);
                fixednamelen++;
            }
            memcpy(fixedname + fixednamelen, "*.*", 4);
            fixednamelen += 3;
            pdir->fh = _findfirst(fixedname, &(pdir->fileinfo));
            if (pdir->fh == -1) {
                win_close(pdir);
                pdir = NULL;
            }
        }
        else {
            // we only go on if this is a directory
            win_close(pdir);
            pdir = NULL;
        }
    }

    return pdir;
}

struct dirent *win_next(DIR *pdir) 
{
    int32_t fileinfonamelen;

    if (pdir->eof) {
        pdir->ent.d_name[0] = 0;
        return NULL;
    }
    assert(pdir->fileinfo.name);

    fileinfonamelen = strlen(pdir->fileinfo.name);
    assert(fileinfonamelen < sizeof(pdir->ent.d_name));

    memcpy(pdir->ent.d_name, pdir->fileinfo.name, fileinfonamelen+1);
    if (_findnext( pdir->fh, &(pdir->fileinfo) ) != 0) {
        pdir->eof = TRUE;
    }

    return &(pdir->ent);
}

void win_close(DIR *pdir) 
{
    if (pdir) {
        if (pdir->fh != -1) {
          _findclose( pdir->fh );
        }
        free(pdir);
    }
}


#else

#define DIR_OPEN(pname) opendir(pname)
#define DIR_NEXT(pdir)  readdir(pdir)     
#define DIR_CLOSE(pdir) closedir(pdir)

#endif


//============================================================================

iERR test_bulk_files()
{
    iENTER;

    char  *title1 = "test_bulk_files";
    char  *title2 = "test_bulk_files";

    IONDEBUG(visit_files(g_iontests_path, "bulk", is_ion, test_good_file, title1), title2);

    iRETURN;
}

iERR test_good_files(TEST_FILE_TYPE filetype)
{
    iENTER;
    char             *title1, *title2;
    FILE_PREDICATE_FN predicate;

    switch(filetype) {
    case FILETYPE_BINARY:
        predicate = is_ion_binary;
        title1 = "test_good_file(binary files)";
        title2 = "test_good(binary files)";
        break;
    case FILETYPE_TEXT:
        predicate = is_ion_text;
        title1 = "test_good_file(text files)";
        title2 = "test_good(text files)";
        break;
    case FILETYPE_ALL:
        predicate = is_ion;
        title1 = "test_good_file(all files)";
        title2 = "test_good(all files)";
        break;
    default:
        printf("ERROR !! bad file type passed to test_files: %d\n", filetype);
        IONDEBUG(IERR_INVALID_STATE, "invalid file type");
    }

    IONDEBUG(visit_files(g_iontests_path, "iontestdata/good", predicate, test_good_file, title1), title2);

    iRETURN;
}

iERR test_good_file(hREADER hreader)
{
    iENTER;

    IONDEBUG(test_reader_read_all(hreader), "test_reader_read_all in test_good_file");

    iRETURN;
}

iERR test_bad_files()
{
    iENTER;

    IONDEBUG(visit_files(g_iontests_path, "iontestdata/bad", is_ion, test_bad_file, "test_bad_file"), "test_bad");

    iRETURN;
}

iERR test_bad_file(hREADER hreader)
{
    iENTER;

    err = test_reader_read_all(hreader);
    if (IERR_OK == err) {
        FAILWITH(IERR_INVALID_ARG);
    } else {
        err = IERR_OK;
    }

    iRETURN;
};

#define TEST_FILE_BUF_MAX   1024  /* (8*1024*1024) */
BYTE      g_buffer[TEST_FILE_BUF_MAX];
TEST_FILE g_test_file = 
{
    NULL,
    TEST_FILE_BUF_MAX,
    g_buffer
};



/**
 * Walk the contents of the given path, executing fn over each real file for
 * which file_predicate returns true.
 * The path MAY end in slash if its a directory
 */
iERR visit_files(char             *parentpath
               , char             *filename
               , FILE_PREDICATE_FN file_predicate
               , LOOP_FN           fn
               , char             *fn_name
) {
    iENTER;
    DIR             *dir;
    BOOL             skip;
    char            *fullfilepath, *localfile;
    char             fullfilepathbuffer[MAX_TEMP_STRING];
    struct dirent   *d;

    fullfilepath = test_concat_filename(fullfilepathbuffer, MAX_TEMP_STRING, parentpath, filename);

    // Are we visiting a directory or a regular file?
    dir = DIR_OPEN(fullfilepath);
    if (dir) {
        IF_PRINT("STARTING %s over %s\n\n", fn_name, fullfilepath);

        while ((d = DIR_NEXT(dir)) != NULL)
        {
            localfile = d->d_name;

            // Ignore magic files in the directory listing.
            if (   strcmp(localfile, "." ) != 0
                && strcmp(localfile, "..") != 0)
            {
                IONDEBUG(visit_files(fullfilepath, localfile, file_predicate, fn, fn_name), "visit files");
            }
        }
        DIR_CLOSE(dir);

        IF_PRINT("\nFINISHED %s over %s\n\n", fn_name, localfile);
    }
    else {
        skip = is_always_skipped(fullfilepath);
        if (!skip && file_predicate) {
            skip = ((*file_predicate)(filename)) != TRUE;
        }
        if (!skip) {

            IONDEBUG(test_one_file(fullfilepath, fn, fn_name), "test one file");

        }
        else {
            // Skipping the file
            IF_PRINT("*** SKIPPING %s\n", fullfilepath);
        }
    }

    iRETURN;
}


iERR test_one_file(char *pathname, LOOP_FN fn, char *fn_name)
{
    iENTER;
    FILE        *fstream = NULL;
    ION_STREAM  *f_ion_stream = NULL;
    hREADER      reader;
    long         size;
    char        *buffer;
    long         result;

    // ------------ testing ion_reader_open_stream ----------------
    fstream = fopen(pathname, "rb");
    if (!fstream) {
        printf("\nERROR: can't open file %s\n", pathname);
        IONDEBUG(IERR_CANT_FIND_FILE, pathname);
    }
    IF_PRINT("\nProcessing file %s\n", pathname);

    IONDEBUG(ion_stream_open_file_in(fstream, &f_ion_stream), "opening ion stream reader");
    IONDEBUG(ion_reader_open(&reader, f_ion_stream, NULL), "opening reader");
    IONDEBUG2(((*fn)(reader)), fn_name, pathname);
    IONDEBUG(ion_reader_close(reader), "closing reader");
    IONDEBUG(ion_stream_close(f_ion_stream), "closing ion stream reader");
    fclose(fstream);

    // ------------ testing ion_reader_open_buffer ----------------
    // obtain file size:
    fstream = fopen(pathname, "rb");
    fseek (fstream, 0, SEEK_END);
    size = ftell (fstream);
    rewind(fstream);                // Set position indicator to the beginning
    buffer = (char*) malloc(size);
    result = fread (buffer, 1, size, fstream);  // copy the file into the buffer:
    fclose (fstream);

    IONDEBUG(ion_reader_open_buffer(&reader, (BYTE *)buffer, result, NULL), "opening buffer reader");
    IONDEBUG2(((*fn)(reader)), fn_name, pathname);

    IONDEBUG(ion_reader_close(reader), "closing reader");
    free (buffer);

    iRETURN;
}

iERR test_stream_handler(struct _ion_user_stream *pstream)
{
    iENTER;

    TEST_FILE *tfile;
    size_t len;

    tfile = (TEST_FILE *)pstream->handler_state;

    pstream->curr = tfile->buffer;

    len = fread( tfile->buffer, sizeof(*tfile->buffer), tfile->block_size, g_test_file.in );
    if (len < 1) {
        pstream->limit = NULL;
        DONTFAILWITH(IERR_EOF);
    }

    pstream->limit = pstream->curr + len;

    iRETURN;
}


