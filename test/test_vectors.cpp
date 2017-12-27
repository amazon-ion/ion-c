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
#include "ion_event_equivalence.h"

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
    initial_stream = new IonEventStream(filename); \
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

std::string testTypeToString(VECTOR_TEST_TYPE test_type) {
    switch (test_type) {
        case READ: return "READ";
        case ROUNDTRIP_TEXT: return "ROUNDTRIP_TEXT";
        case ROUNDTRIP_BINARY: return "ROUNDTRIP_BINARY";
        default: return "UNKNOWN";
    }
}

std::string getTestName(std::string filename, VECTOR_TEST_TYPE *test_type, READER_INPUT_TYPE input_type) {
    std::string test_name = simplifyFilename(filename) + "_";
    if (test_type) {
        test_name += testTypeToString(*test_type);
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
iERR read_value_stream(IonEventStream *stream, READER_INPUT_TYPE input_type, std::string pathname, ION_CATALOG *catalog, IonEventResult *result)
{
    iENTER;
    ION_SET_ERROR_CONTEXT(&pathname, NULL);
    FILE        *fstream = NULL;
    ION_STREAM  *f_ion_stream = NULL;
    hREADER      reader = NULL;
    size_t       size;
    char        *buffer = NULL;
    size_t       buffer_len;

    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    options.pcatalog = catalog;
    ion_event_register_symbol_table_callback(&options, stream);

    fstream = fopen(pathname.c_str(), "rb");
    if (!fstream) {
        IONFAILSTATE(IERR_CANT_FIND_FILE, pathname, result);
    }
    switch (input_type) {
        case STREAM:
            // Tests ion_reader_open_stream
            IONCREAD(ion_stream_open_file_in(fstream, &f_ion_stream));
            IONCREAD(ion_reader_open(&reader, f_ion_stream, &options));
            break;
        case BUFFER:
            // Tests ion_reader_open_buffer
            fseek(fstream, 0, SEEK_END);
            size = (size_t)ftell(fstream);
            rewind(fstream);                // Set position indicator to the beginning
            buffer = (char *)malloc(size);
            buffer_len = fread(buffer, 1, size, fstream);  // copy the file into the buffer:
            IONCREAD(ion_reader_open_buffer(&reader, (BYTE *)buffer, (SIZE)buffer_len, &options));
            break;
        default:
            IONFAILSTATE(IERR_INVALID_ARG, "Unknown READER_INPUT_TYPE.", result);
    }
    IONREPORT(ion_event_stream_read_all(reader, stream, result));
cleanup:
    if (reader) {
        ION_NON_FATAL(ion_reader_close(reader), "Failed to close reader.");
    }
    if (buffer) {
        free(buffer);
    }
    if (f_ion_stream) {
        ION_NON_FATAL(ion_stream_close(f_ion_stream), "Failed to close ION_STREAM.");
    }
    if (fstream) {
        fclose(fstream);
    }
    iRETURN;
}

/**
 * Constructs a writer using the given test type and catalog and uses it to write the given IonEventStream to BYTEs.
 */
iERR write_value_stream(IonEventStream *stream, VECTOR_TEST_TYPE test_type, ION_CATALOG *catalog, BYTE **out, SIZE *len, IonEventResult *result) {
    iENTER;
    ION_EVENT_WRITER_CONTEXT writer_context;
    IONREPORT(ion_event_in_memory_writer_open(&writer_context, stream->location, (test_type == ROUNDTRIP_BINARY ? ION_WRITER_OUTPUT_TYPE_BINARY : ION_WRITER_OUTPUT_TYPE_TEXT_UGLY), catalog, /*imports=*/NULL, result));
    IONREPORT(ion_event_stream_write_all(writer_context.writer, stream, result));
cleanup:
    UPDATEERROR(ion_event_in_memory_writer_close(&writer_context, out, len));
    iRETURN;
}

void write_ion_event_result(IonEventResult *result, std::string test_name) {
    ION_EVENT_WRITER_CONTEXT writer_context;
    std::string message;
    BYTE *out = NULL;
    SIZE len;
    ASSERT_EQ(IERR_OK, ion_event_in_memory_writer_open(&writer_context, test_name + "error result", ION_WRITER_OUTPUT_TYPE_TEXT_PRETTY, NULL, /*imports=*/NULL, /*result=*/NULL));
    if (result->has_error_description) {
        ASSERT_EQ(IERR_OK, ion_event_stream_write_error(writer_context.writer, &result->error_description));
    }
    if (result->has_comparison_result) {
        ASSERT_EQ(IERR_OK, ion_event_stream_write_comparison_result(writer_context.writer, &result->comparison_result, &test_name, NULL));
    }
    ASSERT_EQ(IERR_OK, ion_event_in_memory_writer_close(&writer_context, &out, &len));
    if (out) {
        message = std::string((char *)out, (size_t)len);
        free(out);
    }
    GTEST_FAIL() << test_name << " failed." << std::endl << message;
}

/**
 * Writes the given stream in the format dictated by test_type, re-reads the written stream, then compares the
 * two streams for equivalence.
 */
iERR ionTestRoundtrip(IonEventStream *initial_stream, IonEventStream **roundtrip_stream, ION_CATALOG *catalog,
                      std::string test_name, std::string filename, READER_INPUT_TYPE input_type,
                      VECTOR_TEST_TYPE test_type, IonEventResult *result) {
    iENTER;
    if (test_type > READ) {
        BYTE *written = NULL;
        SIZE len;
        IONREPORT(write_value_stream(initial_stream, test_type, catalog, &written, &len, result));
        *roundtrip_stream = new IonEventStream(test_name + "re-read");
        IONREPORT(read_value_stream_from_bytes(written, len, *roundtrip_stream, catalog, result));
        IONREPORT(assertIonEventStreamEq(initial_stream, *roundtrip_stream, result) ? IERR_OK : IERR_INVALID_STATE);
cleanup:
        if (written) {
            free(written);
        }
    }
    iRETURN;
}

#define ION_TEST_VECTOR_START \
    iERR err; \
    IonEventResult *result = new IonEventResult();

#define ION_TEST_VECTOR_COMPLETE \
cleanup: \
    if (err != IERR_OK) { \
        if (result->has_comparison_result || result->has_error_description) { \
            write_ion_event_result(result, test_name); \
        } \
        else { \
            GTEST_FAIL() << "Unknown failure in " << test_name << std::endl; \
        } \
    } \
    delete result;

/**
 * Exercises good vectors without additional comparison semantics (like equivs, non-equivs, and timestamp/equivTimeline).
 */
TEST_P(GoodBasicVector, GoodBasic) {
    ION_TEST_VECTOR_START;
    IONREPORT(read_value_stream(initial_stream, input_type, filename, catalog, result));
    IONREPORT(ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type, test_type, result));
    ION_TEST_VECTOR_COMPLETE;
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

/**
 * Exercises good vectors with equivs semantics.
 */
TEST_P(GoodEquivsVector, GoodEquivs) {
    ION_TEST_VECTOR_START;
    IONREPORT(read_value_stream(initial_stream, input_type, filename, catalog, result));
    IONREPORT(testComparisonSets(initial_stream, initial_stream, COMPARISON_TYPE_EQUIVS, result) ? IERR_OK : IERR_INVALID_STATE);
    if (test_type > READ) {
        IONREPORT(ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type,
                                  test_type, result));
        IONREPORT(testComparisonSets(roundtrip_stream, roundtrip_stream, COMPARISON_TYPE_EQUIVS, result) ? IERR_OK : IERR_INVALID_STATE);
    }
    ION_TEST_VECTOR_COMPLETE;
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
    ION_TEST_VECTOR_START;
    g_TimestampEquals = ion_timestamp_instant_equals;
    IONREPORT(read_value_stream(initial_stream, input_type, filename, catalog, result));
    IONREPORT(testComparisonSets(initial_stream, initial_stream, COMPARISON_TYPE_EQUIVS) ? IERR_OK : IERR_INVALID_STATE);
    if (test_type > READ) {
        IONREPORT(ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type,
                                  test_type, result));
        IONREPORT(testComparisonSets(roundtrip_stream, roundtrip_stream, COMPARISON_TYPE_EQUIVS) ? IERR_OK : IERR_INVALID_STATE);
    }
    ION_TEST_VECTOR_COMPLETE;
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
    ION_TEST_VECTOR_START;
    IONREPORT(read_value_stream(initial_stream, input_type, filename, catalog, result));
    IONREPORT(testComparisonSets(initial_stream, initial_stream, COMPARISON_TYPE_NONEQUIVS) ? IERR_OK : IERR_INVALID_STATE);
    if (test_type > READ) {
        IONREPORT(ionTestRoundtrip(initial_stream, &roundtrip_stream, catalog, test_name, filename, input_type,
                                  test_type, result));
        IONREPORT(testComparisonSets(roundtrip_stream, roundtrip_stream, COMPARISON_TYPE_NONEQUIVS) ? IERR_OK : IERR_INVALID_STATE);
    }
    ION_TEST_VECTOR_COMPLETE;
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
    // TODO report this better
    iERR status = read_value_stream(initial_stream, input_type, filename, catalog, NULL);
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

// TODO the current bad/ vectors only test the reader. Additional bad/ tests could be created which test the writer.
// This could be done by serializing a stream of IonEvents that are expected to produce an error when written as an
// Ion stream.
