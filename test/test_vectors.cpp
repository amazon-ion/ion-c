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

#include <ion_helpers.h>
#include <ion_event_util.h>
#include "ion_platform_config.h"
#include "gather_vectors.h"
#include "ion_event_stream.h"
#include "ion_assert.h"
#include "ion_timestamp_impl.h"

// NOTE: custom parameterized test names are not supported well by some IDEs (e.g. CLion). They will still run,
// but they don't integrate well with the GUI. Hence, it is best to disable then when debugging within an IDE.
// When better support comes to IDEs, these conditionals should be removed.
#ifndef ION_TEST_VECTOR_VERBOSE_NAMES
#define ION_TEST_VECTOR_VERBOSE_NAMES 1
#endif

#define ION_TEST_VECTOR_CLEANUP() { \
    if (initial_stream) { \
        delete initial_stream; \
        initial_stream = NULL; \
    } \
    if (roundtrip_stream) { \
        delete roundtrip_stream; \
        roundtrip_stream = NULL; \
    } \
    if (catalog) { \
        ion_catalog_close(catalog); \
        catalog = NULL; \
    } \
}

#define ION_TEST_VECTOR_INIT() { \
    g_TimestampEquals = ion_timestamp_equals; \
    g_CurrentTest = test_name; \
    initial_stream = new IonEventStream(); \
    roundtrip_stream = NULL; \
    catalog = NULL; \
    EXPECT_EQ(IERR_OK, ion_catalog_open(&catalog)); \
}

typedef enum _reader_input_type {
    /**
     * Creates an ION_STREAM for the input file using ion_stream_open_file_in, then a reader using ion_reader_open.
     */
            STREAM = 0,
    /**
     * Buffers the contents of the input file, then creates a reader over that buffer using ion_reader_open_buffer.
     */
            BUFFER
} READER_INPUT_TYPE;

typedef enum _vector_test_type {
    /**
     * Simply read the file.
     */
            READ = 0,
    /**
     * Read the file, then write the file in the text format (regardless of the input format), then read the file.
     * Compare the event streams from the first and second reads for equivalence.
     */
            ROUNDTRIP_TEXT,
    /**
     * Read the file, then write the file in the binary format (regardless of the input format), then read the file.
     * Compare the event streams from the first and second reads for equivalence.
     */
            ROUNDTRIP_BINARY
} VECTOR_TEST_TYPE;

TEST(TestVectors, HasRequiredDependencies) {
    // If this flag is false, these tests can't run.
    ASSERT_TRUE(GTEST_HAS_PARAM_TEST);
    ASSERT_TRUE(GTEST_HAS_TR1_TUPLE);
}

std::string simplifyFilename(std::string filename) {
    // Google test requires parameterized test names to be ASCII alphanumeric + underscore...
    std::string copy(filename);
    std::replace(copy.begin(), copy.end(), ION_TEST_PATH_SEPARATOR_CHAR, '_');
    std::replace(copy.begin(), copy.end(), '.', '_');
    std::replace(copy.begin(), copy.end(), '-', '_');
    return copy;
}

std::string getTestName(std::string filename, VECTOR_TEST_TYPE *test_type, READER_INPUT_TYPE input_type) {
    std::string test_name = simplifyFilename(filename) + "_";
    if (test_type) {
        switch (*test_type) {
            case READ:
                test_name += "READ";
                break;
            case ROUNDTRIP_TEXT:
                test_name += "ROUNDTRIP_TEXT";
                break;
            default:
                test_name += "ROUNDTRIP_BINARY";
                break;
        }
        test_name += "_";
    }
    switch (input_type) {
        case STREAM:
            test_name += "STREAM";
            break;
        case BUFFER:
            test_name += "BUFFER";
            break;
    }
    return test_name;
}

/**
 * Represents a test instance for any vector under the good/ subdirectory. Each instantiation of this class results
 * in its own test.
 */
class GoodVector : public ::testing::TestWithParam< ::testing::tuple<std::string, VECTOR_TEST_TYPE, READER_INPUT_TYPE> > {
public:
    virtual ~GoodVector() {
        ION_TEST_VECTOR_CLEANUP();
    }

    virtual void SetUp() {
        filename = ::testing::get<0>(GetParam());
        test_type = ::testing::get<1>(GetParam());
        input_type = ::testing::get<2>(GetParam());
        test_name = getTestName(filename, &test_type, input_type);
        ION_TEST_VECTOR_INIT();
    }

    virtual void TearDown() {
        ION_TEST_VECTOR_CLEANUP();
    }

    std::string test_name;
protected:
    std::string filename;
    VECTOR_TEST_TYPE test_type;
    READER_INPUT_TYPE input_type;
    IonEventStream *initial_stream;
    IonEventStream *roundtrip_stream;
    ION_CATALOG *catalog;
};

// Used by google test to create a custom string representation of a vector instance (if enabled).
struct GoodVectorToString {
    template <class ParamType>
    std::string operator()(const ::testing::TestParamInfo<ParamType>& info) const {
        std::string filename = ::testing::get<0>(info.param);
        VECTOR_TEST_TYPE test_type = ::testing::get<1>(info.param);
        READER_INPUT_TYPE input_type = ::testing::get<2>(info.param);
        return getTestName(filename, &test_type, input_type);
    }
};

// Google test uses class types to link a parameterized test instantiation to its implementation.
class GoodBasicVector : public GoodVector {};
class GoodEquivsVector : public GoodVector {};
class GoodTimestampEquivTimelineVector : public GoodVector {};
class GoodNonequivsVector : public GoodVector {};

/**
 * Represents a test instance for any vector under the bad/ subdirectory. Each instantiation of this class results
 * in its own test.
 */
class BadVector : public ::testing::TestWithParam< ::testing::tuple<std::string, READER_INPUT_TYPE> > {
public:
    virtual ~BadVector() {
        ION_TEST_VECTOR_CLEANUP();
    }

    virtual void SetUp() {
        filename = ::testing::get<0>(GetParam());
        input_type = ::testing::get<1>(GetParam());
        test_name = getTestName(filename, NULL, input_type);
        ION_TEST_VECTOR_INIT();
    }

    virtual void TearDown() {
        ION_TEST_VECTOR_CLEANUP();
    }

    std::string test_name;
protected:
    std::string filename;
    READER_INPUT_TYPE input_type;
    IonEventStream *initial_stream;
    IonEventStream *roundtrip_stream;
    ION_CATALOG *catalog;
};

// Used by google test to create a custom string representation of a vector instance (if enabled).
struct BadVectorToString {
    template <class ParamType>
    std::string operator()(const ::testing::TestParamInfo<ParamType>& info) const {
        std::string filename = ::testing::get<0>(info.param);
        READER_INPUT_TYPE input_type = ::testing::get<1>(info.param);
        return getTestName(filename, NULL, input_type);
    }
};

std::vector<std::string> gather(TEST_FILE_TYPE filetype, TEST_FILE_CLASSIFICATION classification) {
    std::vector<std::string> files;
    gather_files(filetype, classification, &files);
    return files;
}

/**
 * Constructs a reader using the given input type and catalog, then reads IonEvents from the Ion data contained
 * within the file at the given pathname, into the given IonEventStream.
 */
iERR read_value_stream(IonEventStream *stream, READER_INPUT_TYPE input_type, std::string pathname, ION_CATALOG *catalog)
{
    iENTER;
    FILE        *fstream = NULL;
    ION_STREAM  *f_ion_stream = NULL;
    hREADER      reader;
    long         size;
    char        *buffer = NULL;
    long         result;

    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    options.pcatalog = catalog;
    ion_event_register_symbol_table_callback(&options, stream);

    const char *pathname_c_str = pathname.c_str();

    switch (input_type) {
        case STREAM:
            // ------------ testing ion_reader_open_stream ----------------
            fstream = fopen(pathname_c_str, "rb");
            if (!fstream) {
                FAILWITHMSG(IERR_CANT_FIND_FILE, pathname_c_str);
            }

            IONCHECK(ion_stream_open_file_in(fstream, &f_ion_stream));
            IONCHECK(ion_reader_open(&reader, f_ion_stream, &options));
            IONCHECK(ion_event_stream_read_all(reader, stream));
            IONCHECK(ion_reader_close(reader));
            IONCHECK(ion_stream_close(f_ion_stream));
            fclose(fstream);
            break;
        case BUFFER:
            // ------------ testing ion_reader_open_buffer ----------------
            fstream = fopen(pathname_c_str, "rb");
            if (!fstream) {
                FAILWITHMSG(IERR_CANT_FIND_FILE, pathname_c_str);
            }

            fseek(fstream, 0, SEEK_END);
            size = ftell(fstream);
            rewind(fstream);                // Set position indicator to the beginning
            buffer = (char *) malloc(size);
            result = fread(buffer, 1, size, fstream);  // copy the file into the buffer:
            fclose(fstream);

            IONCHECK(ion_reader_open_buffer(&reader, (BYTE *) buffer, result, &options));
            IONCHECK(ion_event_stream_read_all(reader, stream));
            IONCHECK(ion_reader_close(reader));
            free(buffer);
            buffer = NULL;
            break;
        default:
            FAILWITH(IERR_INVALID_ARG);
    }
    fail:
    if (buffer) {
        free(buffer);
    }
    RETURN(__location_name__, __line__, __count__++, err);
}

/**
 * Constructs a writer using the given test type and catalog and uses it to write the given IonEventStream to BYTEs.
 */
iERR write_value_stream(IonEventStream *stream, VECTOR_TEST_TYPE test_type, ION_CATALOG *catalog, BYTE **out, SIZE *len) {
    iENTER;
    ION_EVENT_WRITER_CONTEXT writer_context;
    IONCHECK(ion_event_in_memory_writer_open(&writer_context, (test_type == ROUNDTRIP_BINARY), catalog, /*imports=*/NULL));
    IONCHECK(ion_event_stream_write_all(writer_context.writer, stream));
    IONCHECK(ion_event_in_memory_writer_close(&writer_context, out, len));
    iRETURN;
}

/**
 * Writes the given stream in the format dictated by test_type, re-reads the written stream, then compares the
 * two streams for equivalence.
 */
iERR ionTestRoundtrip(IonEventStream *initial_stream, IonEventStream **roundtrip_stream, ION_CATALOG *catalog,
                      std::string test_name, std::string filename, READER_INPUT_TYPE input_type,
                      VECTOR_TEST_TYPE test_type) {
    iERR status = IERR_OK;
    if (test_type > READ) {
        BYTE *written = NULL;
        SIZE len;
        status = write_value_stream(initial_stream, test_type, catalog, &written, &len);
        EXPECT_EQ(IERR_OK, status) << test_name << " FAILED ON WRITE" << std::endl;
        if (IERR_OK != status) goto finish;
        *roundtrip_stream = new IonEventStream();
        status = read_value_stream_from_bytes(written, len, *roundtrip_stream, catalog);
        EXPECT_EQ(IERR_OK, status) << test_name << " FAILED ON ROUNDTRIP READ" << std::endl;
        if (IERR_OK != status) goto finish;
        status = assertIonEventStreamEq(initial_stream, *roundtrip_stream, ASSERTION_TYPE_NORMAL) ? IERR_OK
                                                                                                  : IERR_INVALID_STATE;
        finish:
        if (written) {
            free(written);
        }
    }
    return status;
}

/**
 * Exercises good vectors without additional comparison semantics (like equivs, non-equivs, and timestamp/equivTimeline).
 */
TEST_P(GoodBasicVector, GoodBasic) {
    iERR status = read_value_stream(initial_stream, input_type, filename, catalog);
    EXPECT_EQ(IERR_OK, status) << test_name << " Error: " << ion_error_to_str(status) << std::endl;
    if (IERR_OK == status) {
        ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type, test_type);
    }
}

#ifdef ION_PLATFORM_WINDOWS
INSTANTIATE_TEST_CASE_P(
    TestVectors,
    GoodBasicVector,
    ::testing::Combine(
        ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_BASIC)),
        ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
        ::testing::Values(STREAM, BUFFER)
    )
);
#else
INSTANTIATE_TEST_CASE_P(
        TestVectors,
        GoodBasicVector,
        ::testing::Combine(
                ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_BASIC)),
                ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
                ::testing::Values(STREAM, BUFFER)
        )
#if ION_TEST_VECTOR_VERBOSE_NAMES
        , GoodVectorToString()
#endif
);
#endif


typedef void (*COMPARISON_FN)(IonEventStream *stream, size_t index_expected, size_t index_actual);

void comparisonEquivs(IonEventStream *stream, size_t index_expected, size_t index_actual) {
    EXPECT_TRUE(assertIonEventsEq(stream, index_expected, stream, index_actual, ASSERTION_TYPE_NORMAL))
                        << std::string("Test: ") << g_CurrentTest
                        << " comparing events at index " << index_expected << " and " << index_actual;
}

void comparisonNonequivs(IonEventStream *stream, size_t index_expected, size_t index_actual) {
    EXPECT_FALSE(assertIonEventsEq(stream, index_expected, stream, index_actual, ASSERTION_TYPE_SET_FLAG))
                        << std::string("Test: ") << g_CurrentTest
                        << " comparing events at index " << index_expected << " and " << index_actual;
}

/**
 * Compares each element in the current container to every other element in the container. The given index refers
 * to the starting index of the first element in the container.
 */
void testEquivsSet(IonEventStream *stream, size_t index, int target_depth, COMPARISON_TYPE comparison_type) {
    // TODO might as well compare each element to itself too (for equivs only). This isn't done currently.
    COMPARISON_FN comparison_fn = (comparison_type == COMPARISON_TYPE_EQUIVS) ? comparisonEquivs
                                                                              : comparisonNonequivs;
    size_t i = index;
    size_t j = index;
    size_t step = 1;
    BOOL are_containers = stream->at(i)->event_type == CONTAINER_START;
    while (TRUE) {
        if (are_containers) {
            // Find the start of the next container to compare its events for equivalence with this one.
            step = valueEventLength(stream, j);
        }
        j += step;
        if (stream->at(j)->event_type == CONTAINER_END && stream->at(j)->depth == target_depth) {
            i += valueEventLength(stream, i);
            j = i;
        } else {
            (*comparison_fn)(stream, i, j);
        }
        if (stream->at(i)->event_type == CONTAINER_END && stream->at(i)->depth == target_depth) {
            break;
        }
    }
}

/**
 * The 'embedded_documents' annotation denotes that the current container contains streams of Ion data embedded
 * in string values. These embedded streams are parsed and their resulting IonEventStreams compared.
 */
BOOL testEmbeddedDocumentSet(IonEventStream *stream, size_t index, int target_depth, COMPARISON_TYPE comparison_type) {
    // TODO could roundtrip the embedded event streams instead of the strings representing them
    ION_ENTER_ASSERTIONS;
    ASSERTION_TYPE assertion_type = (comparison_type == COMPARISON_TYPE_EQUIVS) ? ASSERTION_TYPE_NORMAL
                                                                                : ASSERTION_TYPE_SET_FLAG;
    size_t i = index;
    size_t j = index;
    while (TRUE) {
        j += 1;
        if (stream->at(j)->event_type == CONTAINER_END && stream->at(j)->depth == target_depth) {
            i += 1;
            j = i;
        } else {
            IonEvent *expected_event = stream->at(i);
            IonEvent *actual_event = stream->at(j);
            ION_ASSERT(tid_STRING == expected_event->ion_type, "Embedded documents must be strings.");
            ION_ASSERT(tid_STRING == actual_event->ion_type, "Embedded documents must be strings.");
            char *expected_ion_string = ionStringToString((ION_STRING *)expected_event->value);
            char *actual_ion_string = ionStringToString((ION_STRING *)actual_event->value);
            IonEventStream expected_stream, actual_stream;
            ION_ASSERT(IERR_OK == read_value_stream_from_string(expected_ion_string, &expected_stream, NULL),
                       "Embedded document failed to parse");
            ION_ASSERT(IERR_OK == read_value_stream_from_string(actual_ion_string, &actual_stream, NULL),
                       "Embedded document failed to parse");
            ION_EXPECT_TRUE_MSG(assertIonEventStreamEq(&expected_stream, &actual_stream, assertion_type),
                                std::string("Error comparing streams \"") << expected_ion_string << "\" and \""
                                                                          << actual_ion_string << "\".");
            free(expected_ion_string);
            free(actual_ion_string);
        }
        if (stream->at(i)->event_type == CONTAINER_END && stream->at(i)->depth == target_depth) {
            break;
        }
    }
    ION_EXIT_ASSERTIONS;
}

const char *embeddedDocumentsAnnotation = "embedded_documents";

/**
 * Comparison sets are conveyed as sequences. Each element in the sequence must be equivalent to all other elements
 * in the same sequence.
 */
void testComparisonSets(IonEventStream *stream, COMPARISON_TYPE comparison_type) {
    size_t i = 0;
    while (i < stream->size()) {
        IonEvent *event = stream->at(i);
        if (i == stream->size() - 1) {
            ASSERT_EQ(STREAM_END, event->event_type);
            i++;
        } else {
            ASSERT_EQ(CONTAINER_START, event->event_type);
            ASSERT_TRUE((tid_SEXP == event->ion_type) || (tid_LIST == event->ion_type));
            size_t step = valueEventLength(stream, i);
            char *first_annotation = (event->num_annotations == 1) ? ionStringToString(&event->annotations[0]->value) : NULL;
            if (first_annotation && !strcmp(first_annotation, embeddedDocumentsAnnotation)) {
                testEmbeddedDocumentSet(stream, i + 1, 0, comparison_type);
            } else {
                testEquivsSet(stream, i + 1, 0, comparison_type);
            }
            if (first_annotation) {
                free(first_annotation);
            }
            i += step;
        }
    }
}

/**
 * Exercises good vectors with equivs semantics.
 */
TEST_P(GoodEquivsVector, GoodEquivs) {
    iERR status = read_value_stream(initial_stream, input_type, filename, catalog);
    EXPECT_EQ(IERR_OK, status) << test_name << " Error: " << ion_error_to_str(status) << std::endl;
    if (IERR_OK == status) {
        testComparisonSets(initial_stream, COMPARISON_TYPE_EQUIVS);
        if (test_type > READ) {
            status = ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type,
                                      test_type);
            EXPECT_EQ(IERR_OK, status) << test_name << " Error: roundtrip failed." << std::endl;
            testComparisonSets(roundtrip_stream, COMPARISON_TYPE_EQUIVS);
        }
    }
}

#ifdef ION_PLATFORM_WINDOWS
INSTANTIATE_TEST_CASE_P(
    TestVectors,
    GoodEquivsVector,
    ::testing::Combine(
        ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_EQUIVS)),
        ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
        ::testing::Values(STREAM, BUFFER)
    )
);
#else
INSTANTIATE_TEST_CASE_P(
        TestVectors,
        GoodEquivsVector,
        ::testing::Combine(
                ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_EQUIVS)),
                ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
                ::testing::Values(STREAM, BUFFER)
        )
#if ION_TEST_VECTOR_VERBOSE_NAMES
        , GoodVectorToString()
#endif
);
#endif

/**
 * Exercises good vectors with equivTimeline semantics. This means that timestamps are compared for instant equivalence
 * rather than data model equivalence.
 */
TEST_P(GoodTimestampEquivTimelineVector, GoodTimestampEquivTimeline) {
    g_TimestampEquals = ion_timestamp_instant_equals;
    iERR status = read_value_stream(initial_stream, input_type, filename, catalog);
    EXPECT_EQ(IERR_OK, status) << test_name << " Error: " << ion_error_to_str(status) << std::endl;
    if (IERR_OK == status) {
        testComparisonSets(initial_stream, COMPARISON_TYPE_EQUIVS);
        if (test_type > READ) {
            status = ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type,
                                      test_type);
            EXPECT_EQ(IERR_OK, status) << test_name << " Error: roundtrip failed." << std::endl;
            testComparisonSets(roundtrip_stream, COMPARISON_TYPE_EQUIVS);
        }
    }
}

#ifdef ION_PLATFORM_WINDOWS
INSTANTIATE_TEST_CASE_P(
    TestVectors,
    GoodTimestampEquivTimelineVector,
    ::testing::Combine(
        ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_TIMESTAMP_EQUIVTIMELINE)),
        ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
        ::testing::Values(STREAM, BUFFER)
    )
);
#else
INSTANTIATE_TEST_CASE_P(
        TestVectors,
        GoodTimestampEquivTimelineVector,
        ::testing::Combine(
                ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_TIMESTAMP_EQUIVTIMELINE)),
                ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
                ::testing::Values(STREAM, BUFFER)
        )
#if ION_TEST_VECTOR_VERBOSE_NAMES
        , GoodVectorToString()
#endif
);
#endif

/**
 * Exercises good vectors with nonequivs semantics.
 */
TEST_P(GoodNonequivsVector, GoodNonequivs) {
    iERR status = read_value_stream(initial_stream, input_type, filename, catalog);
    EXPECT_EQ(IERR_OK, status) << test_name << " Error: " << ion_error_to_str(status) << std::endl;
    if (IERR_OK == status) {
        testComparisonSets(initial_stream, COMPARISON_TYPE_NONEQUIVS);
        if (test_type > READ) {
            status = ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type,
                                      test_type);
            EXPECT_EQ(IERR_OK, status) << test_name << " Error: roundtrip failed." << std::endl;
            testComparisonSets(roundtrip_stream, COMPARISON_TYPE_NONEQUIVS);
        }
    }
}

#ifdef ION_PLATFORM_WINDOWS
INSTANTIATE_TEST_CASE_P(
    TestVectors,
    GoodNonequivsVector,
    ::testing::Combine(
        ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_NONEQUIVS)),
        ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
        ::testing::Values(STREAM, BUFFER)
    )
);
#else
INSTANTIATE_TEST_CASE_P(
        TestVectors,
        GoodNonequivsVector,
        ::testing::Combine(
                ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_GOOD_NONEQUIVS)),
                ::testing::Values(READ, ROUNDTRIP_TEXT, ROUNDTRIP_BINARY),
                ::testing::Values(STREAM, BUFFER)
        )
#if ION_TEST_VECTOR_VERBOSE_NAMES
        , GoodVectorToString()
#endif
);
#endif

/**
 * Exercises bad vectors. Bad vectors must fail to parse in order to succeed the test.
 */
TEST_P(BadVector, Bad) {
    iERR status = read_value_stream(initial_stream, input_type, filename, catalog);
    EXPECT_NE(IERR_OK, status) << test_name << " FAILED" << std::endl;
}

#ifdef ION_PLATFORM_WINDOWS
INSTANTIATE_TEST_CASE_P(
    TestVectors,
    BadVector,
    ::testing::Combine(
        ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_BAD)),
        ::testing::Values(STREAM, BUFFER)
    )
);
#else
INSTANTIATE_TEST_CASE_P(
        TestVectors,
        BadVector,
        ::testing::Combine(
                ::testing::ValuesIn(gather(FILETYPE_ALL, CLASSIFICATION_BAD)),
                ::testing::Values(STREAM, BUFFER)
        )
#if ION_TEST_VECTOR_VERBOSE_NAMES
        , BadVectorToString()
#endif
);
#endif
