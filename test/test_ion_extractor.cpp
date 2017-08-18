/*
 * Copyright 2009-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#include "ion_extractor_impl.h"
#include "ion_assert.h"
#include "ion_test_util.h"

/**
 * Max number of paths and path lengths used in these tests. If more are needed, just increase these limits. Having
 * them be as small as possible makes debugging easier.
 */
#define ION_EXTRACTOR_TEST_MAX_PATHS 5
#define ION_EXTRACTOR_TEST_PATH_LENGTH 5

/* -----------------------
 * Extractor test utilities (for reduction of boilerplate).
 */

/**
 * Initializes an extractor test with the given options.
 */
#define ION_EXTRACTOR_TEST_INIT_OPTIONS(options) \
    hREADER reader; \
    hEXTRACTOR extractor; \
    hPATH path; \
    int num_paths = 0; \
    ASSERTION_CONTEXT assertion_contexts[ION_EXTRACTOR_TEST_MAX_PATHS]; \
    ASSERTION_CONTEXT *assertion_context; \
    ION_ASSERT_OK(ion_extractor_open(&extractor, &options));

/**
 * Initializes an extractor test with the default options.
 */
#define ION_EXTRACTOR_TEST_INIT \
    ION_EXTRACTOR_OPTIONS options; \
    options.max_path_length = ION_EXTRACTOR_TEST_PATH_LENGTH; \
    options.max_num_paths = ION_EXTRACTOR_TEST_MAX_PATHS; \
    options.match_relative_paths = false; \
    ION_EXTRACTOR_TEST_INIT_OPTIONS(options);

/**
 * Prepares the next assertion context.
 */
#define ION_EXTRACTOR_TEST_NEXT_CONTEXT(assertion_func) \
    assertion_context = &assertion_contexts[num_paths++]; \
    assertion_context->assertion = assertion_func; \
    assertion_context->num_matches = 0;

/**
 * Starts a path and initializes its test assertion context, which will be passed through the extractor as user context.
 * `assert_matches` must first be set to the ASSERT_MATCHES that should be called when this path matches.
 */
#define ION_EXTRACTOR_TEST_PATH_START(path_length, assertion_func) \
    ION_EXTRACTOR_TEST_NEXT_CONTEXT(assertion_func); \
    ION_ASSERT_OK(ion_extractor_path_create(extractor, path_length, &testCallback, assertion_context, &path));

/**
 * Finishes the current path. Bookend to ION_EXTRACTOR_TEST_PATH_START.
 */
#define ION_EXTRACTOR_TEST_PATH_END \
    assertion_context->path = path;

/**
 * Registers a path from the given Ion text.
 */
#define ION_EXTRACTOR_TEST_PATH_FROM_TEXT(text, assertion_func) \
    ION_EXTRACTOR_TEST_NEXT_CONTEXT(assertion_func); \
    ION_ASSERT_OK(ion_extractor_path_create_from_ion(extractor, &testCallback, assertion_context, (BYTE *)text, (SIZE)strlen(text), &path)); \
    ION_EXTRACTOR_TEST_PATH_END;

/**
 * Generic execution and cleanup of an extractor over the provided reader.
 */
#define ION_EXTRACTOR_TEST_MATCH_READER(ion_reader) \
    ION_ASSERT_OK(ion_extractor_match(extractor, reader)); \
    ION_ASSERT_OK(ion_extractor_close(extractor)); \
    ION_ASSERT_OK(ion_reader_close(reader));

/**
 * Generic execution and cleanup of an extractor. A const char * named `ion_text` must be declared first.
 */
#define ION_EXTRACTOR_TEST_MATCH \
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader)); \
    ION_EXTRACTOR_TEST_MATCH_READER(reader);

/**
 * Generic execution and cleanup of an extractor that is expected to fail when matched over the provided reader.
 */
#define ION_EXTRACTOR_TEST_MATCH_READER_EXPECT_FAILURE(ion_reader) \
    ION_ASSERT_FAIL(ion_extractor_match(extractor, reader)); \
    ION_ASSERT_OK(ion_extractor_close(extractor)); \
    ION_ASSERT_OK(ion_reader_close(reader));

/**
 * Generic execution and cleanup of an extractor that is expected to fail. A const char * named `ion_text` must be
 * declared first.
 */
#define ION_EXTRACTOR_TEST_MATCH_EXPECT_FAILURE \
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader)); \
    ION_EXTRACTOR_TEST_MATCH_READER_EXPECT_FAILURE(reader);

/**
 * Asserts that the path at the given index matched n times. Paths are indexed in the order they are added, starting
 * at 0.
 */
#define ION_EXTRACTOR_TEST_ASSERT_MATCHED(index, n) ASSERT_EQ((n), assertion_contexts[index].num_matches)

/**
 * Test-specific assertion function to be provided to the extractor within the user context. This can't be done directly
 * in the callback because gtest's assertions need to be invoked from a void function.
 */
typedef void (*ASSERT_MATCHES)(ION_READER *reader, ION_EXTRACTOR_PATH_DESCRIPTOR *, ION_EXTRACTOR_PATH_DESCRIPTOR *, ION_EXTRACTOR_CONTROL *);

/**
 * Test-specific state to be provided to the extractor as user context. In addition to verifying that user context is
 * passed through correctly, this gives the test the ability to perform assertions on the callback results.
 */
typedef struct _assertion_context {
    ASSERT_MATCHES assertion;
    hPATH path;
    int num_matches;
} ASSERTION_CONTEXT;

/* -----------------------
 * Test assertions (with ASSERT_MATCHES signatures).
 */

void assertMatchesTextDEF(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                          ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    ION_STRING value;
    ION_TYPE type;

    ASSERT_TRUE(matched_path == original_path);
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ION_ASSERT_OK(ion_reader_read_string(reader, &value));
    ASSERT_EQ(tid_SYMBOL, type);
    assertStringsEqual("def", (char *)value.value, value.length);
}

void assertMatchesTextDEFThenStepOut2(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                                      ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    assertMatchesTextDEF(reader, matched_path, original_path, control);
    *control = ion_extractor_control_step_out(2);
}

void assertMatchesInt3(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                       ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    int value;
    ION_TYPE type;

    ASSERT_TRUE(matched_path == original_path);
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ION_ASSERT_OK(ion_reader_read_int(reader, &value));
    ASSERT_EQ(tid_INT, type);
    ASSERT_EQ(3, value);
}

void assertMatchesInt3ThenStepOut2(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                                   ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    assertMatchesInt3(reader, matched_path, original_path, control);
    *control = ion_extractor_control_step_out(2);
}

void assertMatchesAnyInt1to3(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                             ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    int value;
    ION_TYPE type;

    ASSERT_TRUE(matched_path == original_path);
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ION_ASSERT_OK(ion_reader_read_int(reader, &value));
    ASSERT_EQ(tid_INT, type);
    ASSERT_TRUE(value == 1 || value == 2 || value == 3);
}

void assertMatchesInt1or3(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                          ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    int value;
    ION_TYPE type;

    ASSERT_TRUE(matched_path == original_path);
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ION_ASSERT_OK(ion_reader_read_int(reader, &value));
    ASSERT_EQ(tid_INT, type);
    ASSERT_TRUE(value == 1 || value == 3);
}

void assertMatchesTextDEForInt123(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                                  ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    ION_STRING str_value;
    int int_value;
    ION_TYPE type;

    ASSERT_TRUE(matched_path == original_path);
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ASSERT_TRUE(tid_INT == type || tid_SYMBOL == type);
    if (tid_INT == type) {
        ION_ASSERT_OK(ion_reader_read_int(reader, &int_value));
        ASSERT_EQ(123, int_value);
    }
    else {
        ION_ASSERT_OK(ion_reader_read_string(reader, &str_value));
        assertStringsEqual("def", (char *)str_value.value, str_value.length);
    }
}

void assertPathNeverMatches(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                            ION_EXTRACTOR_PATH_DESCRIPTOR *original_path, ION_EXTRACTOR_CONTROL *control) {
    ASSERT_FALSE(TRUE) << "Path with ID " << matched_path->_path_id << " matched when it should not have.";
}

/* -----------------------
 * Test callbacks
 */

/**
 * Treats user_context as an ASSERT_MATCHES function pointer and invokes that function.
 */
iERR testCallback(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                  void *user_context, ION_EXTRACTOR_CONTROL *control) {
    iENTER;
    ASSERTION_CONTEXT *assertion_context = (ASSERTION_CONTEXT *)user_context;
    assertion_context->assertion(reader, matched_path, assertion_context->path, control);
    assertion_context->num_matches++;
    iRETURN;
}

/**
 * Useful for tests that don't need to make assertions in the callback.
 */
iERR testCallbackBasic(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                       void *user_context, ION_EXTRACTOR_CONTROL *control) {
    iENTER;
    iRETURN;
}

/**
 * Useful for tests that expect a callback to never be invoked. Simply raises an error.
 */
iERR testCallbackNeverInvoked(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                              void *user_context, ION_EXTRACTOR_CONTROL *control) {
    iENTER;
    FAILWITH(IERR_INVALID_STATE);
    iRETURN;
}

/* -----------------------
 * Success tests
 */

TEST(IonExtractorSucceedsWhen, FieldAtDepth1Matches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";
    ION_STRING value;
    ION_ASSERT_OK(ion_string_from_cstr("abc", &value));

    ION_EXTRACTOR_TEST_PATH_START(1, &assertMatchesTextDEF);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &value));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, FieldAtDepth1MatchesFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEF);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, OrdinalAtDepth1Matches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_START(1, &assertMatchesTextDEF);
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 0));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, OrdinalAtDepth1MatchesFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(0)", &assertMatchesTextDEF);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, FieldAndOrdinalAtDepth3Matches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";
    ION_STRING foo_field, bar_field;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo_field));
    ION_ASSERT_OK(ion_string_from_cstr("bar", &bar_field));

    ION_EXTRACTOR_TEST_PATH_START(3, &assertMatchesInt3);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &foo_field));
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &bar_field));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 2));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, FieldAndOrdinalAtDepth3MatchesFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar 2)", &assertMatchesInt3);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, FieldAndOrdinalAtDepth3MatchesFromIonAlternate) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("['foo', \"bar\", abc::2]", &assertMatchesInt3);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorSucceedsWhen, WildcardMatches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";
    ION_STRING foo_field, bar_field;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo_field));
    ION_ASSERT_OK(ion_string_from_cstr("bar", &bar_field));

    ION_EXTRACTOR_TEST_PATH_START(3, &assertMatchesAnyInt1to3);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &foo_field));
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &bar_field));
    ION_ASSERT_OK(ion_extractor_path_append_wildcard(path));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 3);
}

TEST(IonExtractorSucceedsWhen, WildcardMatchesFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar *)", &assertMatchesAnyInt1to3);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 3);
}

TEST(IonExtractorSucceedsWhen, WildcardAndFieldNamedStarMatches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {'*':[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo $ion_extractor_field::* *)", &assertMatchesAnyInt1to3);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 3);
}

TEST(IonExtractorSucceedsWhen, NonTerminalWildcardMatches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[{baz:1}, {zar:2}, {baz:3}]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar * baz)", &assertMatchesInt1or3);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 2);
}

TEST(IonExtractorSucceedsWhen, MultiplePathsMatch) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";
    ION_STRING abc_field, foo_field, bar_field;
    ION_ASSERT_OK(ion_string_from_cstr("abc", &abc_field));
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo_field));
    ION_ASSERT_OK(ion_string_from_cstr("bar", &bar_field));

    ION_EXTRACTOR_TEST_PATH_START(3, &assertMatchesInt3);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &foo_field));
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &bar_field));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 2));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_PATH_START(1, &assertMatchesTextDEF);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &abc_field));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 1);
}

TEST(IonExtractorSucceedsWhen, MultiplePathsMatchFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar 2)", &assertMatchesInt3);
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEF);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 1);
}

TEST(IonExtractorSucceedsWhen, MultiplePathsMatchOnTheSameValue) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "[1, 2, 3]";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(*)", &assertMatchesAnyInt1to3);
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(2)", &assertMatchesInt3);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 3);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 1);
}

TEST(IonExtractorSucceedsWhen, MultiplePathsCreatedUpFrontMatch) {
    ION_EXTRACTOR_TEST_INIT;
    hPATH path2, path3;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}, baz:def}";
    ION_STRING abc_field, foo_field, bar_field;

    ION_ASSERT_OK(ion_string_from_cstr("abc", &abc_field));
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo_field));
    ION_ASSERT_OK(ion_string_from_cstr("bar", &bar_field));

    // Create paths up front.
    ION_EXTRACTOR_TEST_NEXT_CONTEXT(&assertMatchesInt3);
    ION_ASSERT_OK(ion_extractor_path_create(extractor, 3, &testCallback, assertion_context, &path));
    ION_EXTRACTOR_TEST_NEXT_CONTEXT(&assertMatchesTextDEF);
    ION_ASSERT_OK(ion_extractor_path_create(extractor, 1, &testCallback, assertion_context, &path2));

    // Interleave path component appending between the paths.
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &foo_field));
    ION_ASSERT_OK(ion_extractor_path_append_field(path2, &abc_field));
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &bar_field));
    ION_EXTRACTOR_TEST_NEXT_CONTEXT(&assertMatchesTextDEF);
    ION_ASSERT_OK(ion_extractor_path_create_from_ion(extractor, &testCallback, assertion_context, (BYTE *)"(baz)", 5, &path3));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 2));

    assertion_contexts[path->_path_id].path = path;
    assertion_contexts[path2->_path_id].path = path2;
    assertion_contexts[path3->_path_id].path = path3;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(path->_path_id, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(path2->_path_id, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(path3->_path_id, 1);

}

TEST(IonExtractorSucceedsWhen, TheSamePathMatchesMultipleTimes) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc:def}{abc:123}";
    ION_STRING abc_field;
    ION_ASSERT_OK(ion_string_from_cstr("abc", &abc_field));

    ION_EXTRACTOR_TEST_PATH_START(1, &assertMatchesTextDEForInt123);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &abc_field));
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 2);
}

TEST(IonExtractorSucceedsWhen, TheSamePathMatchesMultipleTimesFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc:def}{abc:123}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEForInt123);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 2);
}

TEST(IonExtractorSucceedsWhen, NoPathMatches) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";
    ION_STRING foo_field, bar_field;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo_field));
    ION_ASSERT_OK(ion_string_from_cstr("bar", &bar_field));

    ION_EXTRACTOR_TEST_PATH_START(3, &assertPathNeverMatches);
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &foo_field));
    ION_ASSERT_OK(ion_extractor_path_append_field(path, &bar_field));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 3)); // This ordinal is out of range of the data.
    ION_EXTRACTOR_TEST_PATH_END;

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 0); // Matched zero times.
}

TEST(IonExtractorSucceedsWhen, NoPathMatchesFromIon) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar 3)", &assertPathNeverMatches);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 0); // Matched zero times.
}

TEST(IonExtractorSucceedsWhen, ARelativePathMatches) {
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_TEST_PATH_LENGTH;
    options.max_num_paths = ION_EXTRACTOR_TEST_MAX_PATHS;
    options.match_relative_paths = true;
    ION_EXTRACTOR_TEST_INIT_OPTIONS(options);
    ION_TYPE type;
    // Step in the reader to point to the first 'bar' at depth 2. The extractor will process all siblings of depth 2,
    // but will not step out past depth 2.
    const char *ion_text = "{foo:{bar:{baz:1}, bar:{baz:3}}, foo{bar:{baz:2}}}";
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type)); // {
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type)); // foo
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader)); //bar
    ION_ASSERT_OK(ion_extractor_open(&extractor, &options));

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(baz)", &assertMatchesInt1or3);
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar baz)", &assertPathNeverMatches); // Never matches because the extractor is scoped at depth 2.

    ION_EXTRACTOR_TEST_MATCH_READER(reader);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 2);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 0);
}

TEST(IonExtractorSucceedsWhen, StepOutControlIsReceivedAfterMatch) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3, 4], baz:123}, abc: def}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar 2)", &assertMatchesInt3ThenStepOut2);
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar 3)", &assertPathNeverMatches); // Would match, except this is skipped by control.
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar baz)", &assertPathNeverMatches); // Would match, except this is skipped by control.
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEF); // Matches twice.

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 0);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(2, 0);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(3, 2);
}

TEST(IonExtractorSucceedsWhen, StepOutControlIsReceivedAfterMatchOnRelativePath) {
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_TEST_PATH_LENGTH;
    options.max_num_paths = ION_EXTRACTOR_TEST_MAX_PATHS;
    options.match_relative_paths = true;
    ION_EXTRACTOR_TEST_INIT_OPTIONS(options);
    ION_TYPE type;
    const char *ion_text = "{a:{foo:{baz:{abc:def}, zar:{ghi:jkl}}, bar:{baz:{abc:def}}}";
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type)); // {
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type)); // a
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader)); // foo (at depth 2)

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(baz abc)", &assertMatchesTextDEFThenStepOut2); // Matches at foo and bar.
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(zar ghi)", &assertPathNeverMatches); // Would match, except this is skipped by control.

    // This scopes the extractor at depth 2. The callback will tell the extractor to step out 2 from depth 4, which
    // will return the extractor to its initial depth of 2. It will then continue processing at that depth and match
    // (baz abc) again at the 'bar' field.
    ION_EXTRACTOR_TEST_MATCH_READER(reader);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 2);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 0);

}

TEST(IonExtractorSucceedsWhen, StepOutControlIsReceivedAfterMatchOnWildcard) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[3, 2, 1, 4], baz:123}, abc: def}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar *)", &assertMatchesInt3ThenStepOut2); // Would match four times, but steps out after the first.
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo bar baz)", &assertPathNeverMatches); // Would match, except this is skipped by control.
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEF); // Matches twice.

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 0);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(2, 2);
}

TEST(IonExtractorSucceedsWhen, NumPathsAtMaximum) {
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_TEST_PATH_LENGTH;
    options.max_num_paths = ION_EXTRACTOR_MAX_NUM_PATHS;
    hREADER reader;
    hEXTRACTOR extractor;
    hPATH path;
    int num_paths = 0;
    ASSERTION_CONTEXT assertion_contexts[ION_EXTRACTOR_MAX_NUM_PATHS];
    ASSERTION_CONTEXT *assertion_context;
    ION_ASSERT_OK(ion_extractor_open(&extractor, &options));
    const char *ion_text = "{abc:def, foo:def}";
    int i;
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEF);
    for (i = 1; i < ION_EXTRACTOR_MAX_NUM_PATHS - 1; i++) {
        ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(zar)", &assertPathNeverMatches);
    }
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo)", &assertMatchesTextDEF);

    ION_EXTRACTOR_TEST_MATCH;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
    for (i = 1; i < ION_EXTRACTOR_MAX_NUM_PATHS - 1; i++) {
        ION_EXTRACTOR_TEST_ASSERT_MATCHED(i, 0);
    }
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(ION_EXTRACTOR_MAX_NUM_PATHS - 1, 1);
}

/* -----------------------
 * Failure tests
 */

TEST(IonExtractorFailsWhen, MaxPathLengthExceedsLimit) {
    hEXTRACTOR extractor;
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_MAX_PATH_LENGTH + 1;
    options.max_num_paths = ION_EXTRACTOR_MAX_NUM_PATHS;
    ION_ASSERT_FAIL(ion_extractor_open(&extractor, &options));
}

TEST(IonExtractorFailsWhen, MaxNumPathsExceedsLimit) {
    hEXTRACTOR extractor;
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_MAX_PATH_LENGTH;
    options.max_num_paths = ION_EXTRACTOR_MAX_NUM_PATHS + 1;
    ION_ASSERT_FAIL(ion_extractor_open(&extractor, &options));
}

TEST(IonExtractorFailsWhen, MaxPathLengthIsBelowMinimum) {
    hEXTRACTOR extractor;
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = 0;
    options.max_num_paths = ION_EXTRACTOR_MAX_NUM_PATHS;
    ION_ASSERT_FAIL(ion_extractor_open(&extractor, &options));
}

TEST(IonExtractorFailsWhen, MaxNumPathsIsBelowMinimum) {
    hEXTRACTOR extractor;
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_MAX_PATH_LENGTH;
    options.max_num_paths = 0;
    ION_ASSERT_FAIL(ion_extractor_open(&extractor, &options));
}

TEST(IonExtractorFailsWhen, PathLengthIsZero) {
    hEXTRACTOR extractor;
    hPATH path;
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_FAIL(ion_extractor_path_create(extractor, 0, &testCallbackNeverInvoked, NULL, &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathExceedsDeclaredLength) {
    hEXTRACTOR extractor;
    hPATH path;
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_OK(ion_extractor_path_create(extractor, 1, &testCallbackBasic, NULL, &path));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 1));
    ION_ASSERT_FAIL(ion_extractor_path_append_ordinal(path, 0));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathExceedsMaxLength) {
    hEXTRACTOR extractor;
    hPATH path;
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = 1;
    options.max_num_paths = ION_EXTRACTOR_MAX_NUM_PATHS;
    ION_ASSERT_OK(ion_extractor_open(&extractor, &options));
    ION_ASSERT_FAIL(ion_extractor_path_create(extractor, 2, &testCallbackBasic, NULL, &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsIncomplete) {
    hEXTRACTOR extractor;
    hPATH path;
    hREADER reader;
    const char *ion_text = "[1, [1, 2], 3]";
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_OK(ion_extractor_path_create(extractor, 3, &testCallbackBasic, NULL, &path));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 1));
    ION_ASSERT_OK(ion_extractor_path_append_ordinal(path, 0));

    ION_EXTRACTOR_TEST_MATCH_EXPECT_FAILURE;
}

TEST(IonExtractorFailsWhen, TooManyPathsAreRegistered) {
    hEXTRACTOR extractor;
    hPATH path;
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_MAX_PATH_LENGTH;
    options.max_num_paths = 1;
    ION_ASSERT_OK(ion_extractor_open(&extractor, &options));
    ION_ASSERT_OK(ion_extractor_path_create(extractor, 1, &testCallbackBasic, NULL, &path));
    ION_ASSERT_FAIL(ion_extractor_path_create(extractor, 1, &testCallbackNeverInvoked, NULL, &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsAppendedBeforeCreation) {
    hEXTRACTOR extractor;
    ION_EXTRACTOR_PATH_DESCRIPTOR *path = (ION_EXTRACTOR_PATH_DESCRIPTOR *)malloc(sizeof(ION_EXTRACTOR_PATH_DESCRIPTOR));
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    path->_path_id = 0;
    path->_current_length = 0;
    path->_path_length = 1;
    path->_extractor = extractor;
    ION_ASSERT_FAIL(ion_extractor_path_append_ordinal(path, 2));
    free(path);
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsCreatedFromIonWithMoreThanOneTopLevelValue) {
    hEXTRACTOR extractor;
    hPATH path;
    const char *data = "(foo) 123";
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_FAIL(ion_extractor_path_create_from_ion(extractor, &testCallbackNeverInvoked, NULL, (BYTE *)data, strlen(data), &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsCreatedFromIonWithZeroPathComponents) {
    hEXTRACTOR extractor;
    hPATH path;
    const char *data = "()";
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_FAIL(ion_extractor_path_create_from_ion(extractor, &testCallbackNeverInvoked, NULL, (BYTE *)data, strlen(data), &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsCreatedFromIonThatIsNotASequence) {
    hEXTRACTOR extractor;
    hPATH path;
    const char *data = "abc";
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_FAIL(ion_extractor_path_create_from_ion(extractor, &testCallbackNeverInvoked, NULL, (BYTE *)data, strlen(data), &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsCreatedFromIonThatExceedsMaxLength) {
    hEXTRACTOR extractor;
    hPATH path;
    const char *data = "(foo bar)";
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = 1;
    options.max_num_paths = ION_EXTRACTOR_MAX_NUM_PATHS;
    ION_ASSERT_OK(ion_extractor_open(&extractor, &options));
    ION_ASSERT_FAIL(ion_extractor_path_create_from_ion(extractor, &testCallbackNeverInvoked, NULL, (BYTE *)data, strlen(data), &path));
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, PathIsAppendedAfterCreationFromIon) {
    hEXTRACTOR extractor;
    hPATH path;
    const char *data = "(foo bar)";
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));
    ION_ASSERT_OK(ion_extractor_path_create_from_ion(extractor, &testCallbackNeverInvoked, NULL, (BYTE *)data, strlen(data), &path));
    ION_ASSERT_FAIL(ion_extractor_path_append_ordinal(path, 2)); // This will exceed the path's declared length.
    ION_ASSERT_OK(ion_extractor_close(extractor));
}

TEST(IonExtractorFailsWhen, ReaderStartsAtDepthOtherThanZero) {
    hEXTRACTOR extractor;
    hREADER reader;
    hPATH path;
    ION_TYPE type;
    const char *ion_text = "{foo:{bar:{baz:123}}}";
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_extractor_open(&extractor, NULL));

    ION_ASSERT_OK(ion_extractor_path_create_from_ion(extractor, &testCallbackNeverInvoked, NULL, (BYTE *)"(baz)", 5, &path));

    ION_EXTRACTOR_TEST_MATCH_READER_EXPECT_FAILURE(reader);
}

void assertMatchesStructWithListNamedBarNoStepOut(hREADER reader, ION_EXTRACTOR_PATH_DESCRIPTOR *matched_path,
                                                  ION_EXTRACTOR_PATH_DESCRIPTOR *original_path,
                                                  ION_EXTRACTOR_CONTROL *control) {
    ION_TYPE type;
    ION_STRING field_name;
    ASSERT_TRUE(matched_path == original_path);
    ION_ASSERT_OK(ion_reader_get_type(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_LIST, type);
    ION_ASSERT_OK(ion_reader_get_field_name(reader, &field_name));
    assertStringsEqual("bar", (char *)field_name.value, field_name.length);
    // No matching call to ion_reader_step_out. This will cause the extractor to raise an error.
}

TEST(IonExtractorFailsWhen, ReaderReturnsFromCallbackAtDifferentDepth) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def, foo: {bar:[1, 2, 3]}}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(foo)", &assertMatchesStructWithListNamedBarNoStepOut);

    ION_EXTRACTOR_TEST_MATCH_EXPECT_FAILURE;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorFailsWhen, ControlStepsOutBeyondReaderDepth) {
    ION_EXTRACTOR_TEST_INIT;
    const char *ion_text = "{abc: def}";

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEFThenStepOut2);

    ION_EXTRACTOR_TEST_MATCH_EXPECT_FAILURE;
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
}

TEST(IonExtractorFailsWhen, ControlStepsOutBeyondRelativePathDepth) {
    ION_EXTRACTOR_OPTIONS options;
    options.max_path_length = ION_EXTRACTOR_TEST_PATH_LENGTH;
    options.max_num_paths = ION_EXTRACTOR_TEST_MAX_PATHS;
    options.match_relative_paths = true;
    ION_EXTRACTOR_TEST_INIT_OPTIONS(options);
    ION_TYPE type;
    const char *ion_text = "{foo:{baz:{abc:def}, zar:{ghi:jkl}}, bar:123}";
    ION_ASSERT_OK(ion_test_new_text_reader(ion_text, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type)); // {
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type)); // foo
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader)); // at depth 2

    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(abc)", &assertMatchesTextDEFThenStepOut2);
    ION_EXTRACTOR_TEST_PATH_FROM_TEXT("(ghi)", &assertPathNeverMatches); // Would match, except this is skipped by control.

    // This scopes the extractor at depth 2. The callback will tell the extractor to step out 2 from depth 3, which
    // would normally be legal, but will fail in this case because it would exit the extractor's scope.
    ION_EXTRACTOR_TEST_MATCH_READER_EXPECT_FAILURE(reader);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(0, 1);
    ION_EXTRACTOR_TEST_ASSERT_MATCHED(1, 0);
}
