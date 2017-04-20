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

#ifdef ION_PLATFORM_WINDOWS
#include <io.h>
#else
#include <dirent.h>
#endif

#include "gather_vectors.h"
#include "ion_helpers.h"
#include <cstdarg>
#include <sys/stat.h>

//============================================================================
// File handling utilities

static std::string *test_concat_filename(std::string *dst, std::string path, std::string name) {
    size_t  pathlen, namelen;
    BOOL path_needs_separator, file_needs_separator;

    assert(dst);

    pathlen = path.size();
    namelen = name.size();

    // No separator is needed if there is no path or no name.
    path_needs_separator = (pathlen > 0) ? (path[pathlen - 1] != ION_TEST_PATH_SEPARATOR_CHAR) : FALSE;
    file_needs_separator = (namelen > 0) ? (name[0] != ION_TEST_PATH_SEPARATOR_CHAR) : FALSE;

    if (pathlen) {
        dst->append(path);
    }

    if (path_needs_separator && file_needs_separator) {
        dst->push_back(ION_TEST_PATH_SEPARATOR_CHAR);
    }

    if (namelen) {
        dst->append(name);
    }

    return dst;
}

static std::string *test_concat_filenames(std::string *dst, int num_components, ...) {
    va_list args;
    va_start(args, num_components);
    std::string path_separator_str(1, ION_TEST_PATH_SEPARATOR_CHAR);
    for(int i = 0; i < num_components; i++) {
        std::string component = std::string(va_arg(args, char *));
        std::string separator = (i == 0) ? "" : path_separator_str;
        test_concat_filename(dst, separator, component);
    }
    va_end(args);
    return dst;
}

inline std::string join_path(std::string prefix, std::string suffix) {
    std::string path;
    test_concat_filenames(&path, 2, prefix.c_str(), suffix.c_str());
    return path;
}

inline BOOL directory_exists(std::string path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0)
        return FALSE;
    return (info.st_mode & S_IFDIR);
}

inline std::string find_ion_tests_path() {
    // IDEs typically perform in-source builds, in which case this will be executed directly from the tests directory.
    // For out-of-source builds (such as the one performed by the build-release.sh script), this will be run from
    // build/release/test. This attempts to locate the ion-tests directory from both locations.
    std::string from_test_directory = join_path(/*ion-c*/"..", "ion-tests");
    if (directory_exists(from_test_directory)) {
        return from_test_directory;
    }
    std::string from_build_directory;
    test_concat_filenames(&from_build_directory, 5, /*test*/"..", /*release*/"..", /*build*/"..", /*ion-c*/"..", "ion-tests");
    if (directory_exists(from_build_directory)) {
        return from_build_directory;
    }
    return "";
}

//============================================================================
// File filtering

static const std::string iontests_path = find_ion_tests_path();
static const std::string good_path = join_path("iontestdata", "good");
static const std::string bad_path = join_path("iontestdata", "bad");
static const std::string bad_timestamp_path = join_path(bad_path, "timestamp");
static const std::string good_equivs_path = join_path(good_path, "equivs");
static const std::string good_nonequivs_path = join_path(good_path, "non-equivs");
static const std::string good_timestamp_path = join_path(good_path, "timestamp");
static const std::string good_timestamp_equivtimeline_path = join_path(good_timestamp_path, "equivTimeline");

static const std::string full_good_path = join_path(iontests_path, good_path);
static const std::string full_good_equivs_path = join_path(full_good_path, "equivs");
static const std::string full_good_nonequivs_path = join_path(full_good_path, "non-equivs");
static const std::string full_good_timestamp_equivtimeline_path = join_path(iontests_path, good_timestamp_equivtimeline_path);
static const std::string full_bad_path = join_path(iontests_path, bad_path);

std::vector<std::string> _skip_list;
std::vector<std::string> _whitelist; // For debugging. Populate this with filenames to exercise only those.

void add_path_to(std::vector<std::string> *list, std::string prefix, std::string suffix) {
    list->push_back(join_path(prefix, suffix));
}

void add_to_skip(std::string prefix, std::string suffix) {
    add_path_to(&_skip_list, prefix, suffix);
}

std::vector<std::string> *skip_list() {
    // Assumption: these tests are single-threaded.
    if (_skip_list.size() == 0) {
        add_to_skip(good_path, "item1.10n"); // TODO contains not-found imports. Fails on roundtrip. Should be able to hand off catalog (started) and roundtrip the unknown symbols.

        add_to_skip(good_path, "utf16.ion");
        add_to_skip(good_path, "utf32.ion");
    }
    return &_skip_list;
}

void add_to_whitelist(std::string prefix, std::string suffix) {
    add_path_to(&_whitelist, prefix, suffix);
}

std::vector<std::string> *whitelist() {
    if (_whitelist.size() == 0) {
        //add_to_whitelist(good_equivs_path, "timestampFractions.10n");
    }
    return &_whitelist;
}

BOOL string_ends_with(std::string s, std::string suffix) {
    size_t sLength = s.size();
    size_t suffixLength = suffix.size();

    if (sLength < suffixLength) {
        return FALSE;
    }

    const char* sSuffix = s.c_str() + (sLength - suffixLength);
    return 0 == suffix.compare(sSuffix);
}

BOOL is_always_skipped(std::string filename) {
    std::vector<std::string> whites = *whitelist();
    if (whites.size() != 0) {
        for (int i = 0; i < whites.size(); i++) {
            if (string_ends_with(filename, whites[i])) {
                return FALSE;
            }
        }
        return TRUE;
    }
    else {
        std::vector<std::string> skips = *skip_list();
        for (int i = 0; i < skips.size(); i++) {
            if (string_ends_with(filename, skips[i])) {
                return TRUE;
            }
        }
        return FALSE;
    }
}

BOOL suffix_matches(std::string filename, std::string suffix) {
    size_t suffixLen = suffix.size();
    size_t fileLen = filename.size();

    if ((fileLen > suffixLen)
        && (0 == suffix.compare(filename.c_str() + (fileLen - suffixLen))))
    {
        return TRUE;
    }
    return FALSE;
}

BOOL prefix_matches(std::string filename, std::string prefix) {
    size_t prefixLen = prefix.size();
    size_t fileLen = filename.size();

    if ((fileLen > prefixLen)
        && (0 == prefix.compare(0, prefixLen, filename, 0, prefixLen)))
    {
        return TRUE;
    }
    return FALSE;
}

BOOL is_ion_text(std::string filename) {
    return suffix_matches(filename, ".ion");
}

BOOL is_ion_binary(std::string filename) {
    return suffix_matches(filename, ".10n");
}

BOOL is_ion(std::string filename) {
    return is_ion_text(filename) || is_ion_binary(filename);
}

BOOL is_good_equivs(std::string filename) {
    return prefix_matches(filename, full_good_equivs_path);
}

BOOL is_good_nonequivs(std::string filename) {
    return prefix_matches(filename, full_good_nonequivs_path);
}

BOOL is_good_timestamp_equivtimeline(std::string filename) {
    return prefix_matches(filename, full_good_timestamp_equivtimeline_path);
}

BOOL is_good_basic(std::string filename) {
    return prefix_matches(filename, full_good_path)
           && !is_good_equivs(filename)
           && !is_good_nonequivs(filename)
           && !is_good_timestamp_equivtimeline(filename);
}

BOOL is_bad_basic(std::string filename) {
    return prefix_matches(filename, full_bad_path);
}

//============================================================================

#ifdef ION_PLATFORM_WINDOWS // TODO not tested recently.

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
// File gathering

iERR visit_files(
        std::string parentpath
        , std::string filename
        , FILE_PREDICATE_FN type_predicate
        , FILE_PREDICATE_FN classification_predicate
        , std::vector<std::string> *files_out
);

iERR gather_files(TEST_FILE_TYPE filetype, TEST_FILE_CLASSIFICATION classification, std::vector<std::string> *files_out) {
    iENTER;
    std::string classification_root_path;
    FILE_PREDICATE_FN type_predicate, classification_predicate;

    switch(filetype) {
        case FILETYPE_BINARY:
            type_predicate = is_ion_binary;
            break;
        case FILETYPE_TEXT:
            type_predicate = is_ion_text;
            break;
        case FILETYPE_ALL:
            type_predicate = is_ion;
            break;
        default:
            FAILWITHMSG(IERR_INVALID_STATE, "invalid file type");
    }

    switch(classification) {
        case CLASSIFICATION_GOOD_BASIC:
            classification_predicate = is_good_basic;
            classification_root_path = good_path;
            break;
        case CLASSIFICATION_GOOD_EQUIVS:
            classification_predicate = is_good_equivs;
            classification_root_path = good_equivs_path;
            break;
        case CLASSIFICATION_GOOD_NONEQUIVS:
            classification_predicate = is_good_nonequivs;
            classification_root_path = good_nonequivs_path;
            break;
        case CLASSIFICATION_GOOD_TIMESTAMP_EQUIVTIMELINE:
            classification_predicate = is_good_timestamp_equivtimeline;
            classification_root_path = good_timestamp_equivtimeline_path;
            break;
        case CLASSIFICATION_BAD:
            classification_predicate = is_bad_basic;
            classification_root_path = bad_path;
            break;
        default:
            FAILWITHMSG(IERR_INVALID_STATE, "invalid file type");
    }

    IONCHECK(visit_files(iontests_path, classification_root_path, type_predicate, classification_predicate, files_out));

    iRETURN;
}

/**
 * Walk the contents of the given path, adding each real file for
 * which both type_predicate and classification_predicate return true to files_out.
 */
iERR visit_files(
      std::string parentpath
    , std::string filename
    , FILE_PREDICATE_FN type_predicate
    , FILE_PREDICATE_FN classification_predicate
    , std::vector<std::string> *files_out
) {
    iENTER;
    DIR             *dir;
    BOOL             skip;
    std::string fullfilepath, localfile;
    struct dirent   *d;

    test_concat_filename(&fullfilepath, parentpath, filename);

    // Are we visiting a directory or a regular file?
    dir = DIR_OPEN(fullfilepath.c_str());
    if (dir) {

        while ((d = DIR_NEXT(dir)) != NULL)
        {
            localfile = d->d_name;

            // Ignore magic files in the directory listing.
            if (   localfile.compare(".") != 0
                   && localfile.compare("..") != 0)
            {
                IONCHECK(visit_files(fullfilepath, localfile, type_predicate, classification_predicate, files_out));
            }
        }
        DIR_CLOSE(dir);
    }
    else {
        skip = is_always_skipped(fullfilepath);
        if (!skip && type_predicate) {
            skip = ((*type_predicate)(filename)) != TRUE;
        }
        if (!skip && classification_predicate) {
            skip = ((*classification_predicate)(fullfilepath)) != TRUE;
        }
        if (!skip) {
            files_out->push_back(fullfilepath);
        }
    }

    iRETURN;
}
