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

#include <ion_event_util.h>
#include <ion_event_stream_impl.h>
#include "ion_assert.h"
#include "ion_event_stream.h"
#include "ion_helpers.h"
#include "ion_test_util.h"
#include <locale.h>

// Declaration for testing function from ion_writer_text.c
extern "C" void _ion_writer_text_set_platform_specific_locale_disabled(int is_disabled);

class WriterTest : public ::testing::Test {
protected:
    void SetUp() {
        out = tmpfile();
    }

    void TearDown() {
        fclose(out);
    }

    FILE *out;
};

iERR ion_test_open_file_writer(hWRITER *writer, FILE *out, BOOL is_binary, ION_STREAM **stream) {
    iENTER;
    ION_WRITER_OPTIONS options;
    ION_STREAM *ion_stream = NULL;

    ion_event_initialize_writer_options(&options);
    options.output_as_binary = is_binary;

    IONCHECK(ion_stream_open_file_out(out, &ion_stream));
    IONCHECK(ion_writer_open(writer, ion_stream, &options));

    *stream = ion_stream;

    iRETURN;
}

TEST_F(WriterTest, ResourcesNotLeakedOnWriteToTooSmallBuffer)
{
    hWRITER writer = NULL;
    ION_WRITER_OPTIONS options;
    ion_event_initialize_writer_options(&options);
    options.output_as_binary = true;

    uint8_t buf[1];
    SIZE len;

    ion_writer_open_buffer(&writer, buf, sizeof(buf), &options);
    ion_writer_write_int32(writer, 1);
    ion_writer_finish(writer, &len);

    ASSERT_EQ(IERR_BUFFER_TOO_SMALL, ion_writer_close(writer));
}

TEST_F(WriterTest, BinaryWriterCloseMustFlushStream) {
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;

    long file_size;

    ion_test_open_file_writer(&writer, out, TRUE, &stream);

    ION_ASSERT_OK(ion_writer_write_bool(writer, TRUE));

    ION_ASSERT_OK(ion_writer_close(writer));

    ION_ASSERT_OK(ion_stream_close(stream));

    // get the size of the file after closing the writer
    fseek(out, 0L, SEEK_END);
    file_size = ftell(out);

    // 4 bytes for the IVM 1 byte for Ion bool
    ASSERT_EQ(file_size, 4 + 1);
}

TEST_F(WriterTest, TextWriterCloseMustFlushStream) {
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;

    long file_size;

    ion_test_open_file_writer(&writer, out, FALSE, &stream);

    ION_ASSERT_OK(ion_writer_write_bool(writer, TRUE));

    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_stream_close(stream));

    // get the size of the file after closing the writer
    fseek(out, 0L, SEEK_END);
    file_size = ftell(out);

    ASSERT_EQ(file_size, 4);
}

// Parameterized test class for locale independence testing
class WriterLocaleTest : public WriterTest, public ::testing::WithParamInterface<bool> {
    // Parameter: true = disable platform-specific (test global locale switching), false = use platform-specific functions
};

TEST_P(WriterLocaleTest, TextWriterUsesCorrectDecimalPointRegardlessOfLocale) {
    hWRITER writer = NULL;
    ION_WRITER_OPTIONS options;
    BYTE buffer[1024];
    SIZE bytes_written;
    iERR err = IERR_OK;
    std::string output; // Declare early to avoid C++ goto issues
    bool disable_platform_specific = GetParam();
    char original_locale_copy[256] = {0};
    char locale_before_writer_copy[256] = {0};
    const char* test_locale = nullptr;
    char *original_locale = nullptr;
    char *locale_before_writer = nullptr;
    char *locale_after_writer = nullptr;

    // Save original locale
    original_locale = setlocale(LC_ALL, NULL);
    if (original_locale && strlen(original_locale) < sizeof(original_locale_copy)) {
        strcpy(original_locale_copy, original_locale);
    }

    // Test with Ukrainian locale that uses comma as decimal separator
    test_locale = setlocale(LC_ALL, "uk_UA.UTF-8");
    ASSERT_NE(nullptr, test_locale) << "Ukrainian locale uk_UA.UTF-8 not available - cannot test custom locales";

    // Store locale after setting to Ukrainian for comparison later
    locale_before_writer = setlocale(LC_ALL, NULL);
    if (locale_before_writer && strlen(locale_before_writer) < sizeof(locale_before_writer_copy)) {
        strcpy(locale_before_writer_copy, locale_before_writer);
    }

    // Set platform-specific behavior based on test parameter
    if (disable_platform_specific) {
        _ion_writer_text_set_platform_specific_locale_disabled(1);
    }

    memset(&options, 0, sizeof(ION_WRITER_OPTIONS));
    options.output_as_binary = FALSE;
    options.pretty_print = FALSE;

    memset(buffer, 0, sizeof(buffer));
    IONCHECK(ion_writer_open_buffer(&writer, buffer, sizeof(buffer), &options));
    IONCHECK(ion_writer_write_float(writer, 1.5f));
    IONCHECK(ion_writer_finish(writer, &bytes_written));
    IONCHECK(ion_writer_close(writer));

    // Verify global locale is unchanged after writer operations
    locale_after_writer = setlocale(LC_ALL, NULL);
    EXPECT_STREQ(locale_before_writer_copy, locale_after_writer)
        << "Global locale changed during writer operation - before: " << locale_before_writer_copy
        << ", after: " << locale_after_writer << " (platform_disabled=" << disable_platform_specific << ")";

    // Convert to string for verification
    output.assign((char*)buffer, bytes_written);

    // Should contain "1.5" and NOT contain any commas as decimal separators
    EXPECT_NE(std::string::npos, output.find("1.5"))
        << "Expected '1.5' in output, got: " << output << " (platform_disabled=" << disable_platform_specific << ")";
    EXPECT_EQ(std::string::npos, output.find("1,5"))
        << "Found incorrect comma decimal separator in output: " << output << " (platform_disabled=" << disable_platform_specific << ")";

    writer = NULL;

    // Disable platform-specific override if it was enabled
    if (disable_platform_specific) {
        _ion_writer_text_set_platform_specific_locale_disabled(0);
    }

    // Restore original locale
    if (original_locale_copy[0]) {
        setlocale(LC_ALL, original_locale_copy);
    }

fail:
    if (writer) {
        ion_writer_close(writer);
    }
    // Make sure to disable platform-specific override even if test fails
    _ion_writer_text_set_platform_specific_locale_disabled(0);
    ASSERT_EQ(IERR_OK, err);
}

INSTANTIATE_TEST_SUITE_P(LocaleIndependence, WriterLocaleTest,
                        ::testing::Values(false, true),
                        [](const ::testing::TestParamInfo<bool>& info) {
                            return info.param ? "FallbackPath" : "PlatformSpecificPath";
                        });
