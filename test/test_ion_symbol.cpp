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

#include "ion_assert.h"
#include "ion_helpers.h"
#include "ion_test_util.h"

// Creates a BinaryAndTextTest fixture instantiation for IonSymbolTable tests. This allows tests to be declared with
// the BinaryAndTextTest fixture and receive the is_binary flag with both the TRUE and FALSE values.
INSTANTIATE_TEST_CASE_BOOLEAN_PARAM(IonSymbolTable);

#define ION_SYMBOL_TEST_DECLARE_WRITER \
    hWRITER writer; \
    ION_STREAM *stream; \
    SIZE bytes_flushed;

#define _ION_SYMBOL_TEST_REWRITE_AND_ASSERT_WITH_CATALOG(retrieve_data, cleanup_data, as_binary, expected, expected_len) \
    hREADER reader; \
    BYTE *result; \
    ION_READER_OPTIONS reader_options; \
    retrieve_data; \
    ion_test_initialize_reader_options(&reader_options); \
    reader_options.pcatalog = catalog; \
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, result, bytes_flushed, &reader_options)); \
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream)); \
    writer_options.output_as_binary = as_binary; \
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options)); \
    ION_ASSERT_OK(ion_writer_write_all_values(writer, reader)); \
    ION_ASSERT_OK(ion_reader_close(reader)); \
    cleanup_data; \
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed)); \
    if (as_binary) { \
        assertBytesEqual(expected, expected_len, result, bytes_flushed); \
    } \
    else { \
        assertStringsEqual(expected, (char *)result, bytes_flushed); \
    } \
    free(result);

#define _ION_SYMBOL_TEST_REWRITE_AND_ASSERT(retrieve_data, cleanup_data, as_binary, expected, expected_len) \
    ION_WRITER_OPTIONS writer_options; \
    ion_test_initialize_writer_options(&writer_options); \
    ION_CATALOG *catalog = NULL; \
    _ION_SYMBOL_TEST_REWRITE_AND_ASSERT_WITH_CATALOG(retrieve_data, cleanup_data, as_binary, expected, expected_len);

#define _ION_SYMBOL_TEST_REWRITE_FROM_BUFFER_AND_ASSERT_TEXT(base_macro, data, data_len, expected) \
    ION_SYMBOL_TEST_DECLARE_WRITER; \
    bytes_flushed = data_len; \
    base_macro(result = data, /*do nothing*/, FALSE, expected, strlen(expected));

#define ION_SYMBOL_TEST_REWRITE_WITH_CATALOG_FROM_BUFFER_AND_ASSERT_TEXT(data, data_len, expected) \
    _ION_SYMBOL_TEST_REWRITE_FROM_BUFFER_AND_ASSERT_TEXT(_ION_SYMBOL_TEST_REWRITE_AND_ASSERT_WITH_CATALOG, data, data_len, expected);

#define ION_SYMBOL_TEST_REWRITE_FROM_BUFFER_AND_ASSERT_TEXT(data, data_len, expected) \
    _ION_SYMBOL_TEST_REWRITE_FROM_BUFFER_AND_ASSERT_TEXT(_ION_SYMBOL_TEST_REWRITE_AND_ASSERT, data, data_len, expected);

#define _ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT(base_macro, expected) \
    base_macro( \
        ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed)), \
        free(result), FALSE, expected, strlen(expected) \
    );

#define ION_SYMBOL_TEST_REWRITE_WITH_CATALOG_FROM_WRITER_AND_ASSERT_TEXT(expected) \
    _ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT(_ION_SYMBOL_TEST_REWRITE_AND_ASSERT_WITH_CATALOG, expected);

#define ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT(expected) \
    _ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT(_ION_SYMBOL_TEST_REWRITE_AND_ASSERT, expected);

#define ION_SYMBOL_TEST_POPULATE_CATALOG \
    ION_SYMBOL_TABLE writer_imports[2]; \
    hSYMTAB import1 = &writer_imports[0], import2 = &writer_imports[1]; \
    ION_STRING import1_name, import2_name, sym1, sym2, sym3; \
    SID sid; \
    hCATALOG catalog; \
    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name)); \
    ION_ASSERT_OK(ion_string_from_cstr("import2", &import2_name)); \
    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1)); \
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2)); \
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3)); \
    ION_ASSERT_OK(ion_catalog_open(&catalog)); \
    ION_ASSERT_OK(ion_symbol_table_open_with_type(&import1, catalog, ist_SHARED)); \
    ION_ASSERT_OK(ion_symbol_table_open_with_type(&import2, catalog, ist_SHARED)); \
    ION_ASSERT_OK(ion_symbol_table_set_name(import1, &import1_name)); \
    ION_ASSERT_OK(ion_symbol_table_set_name(import2, &import2_name)); \
    ION_ASSERT_OK(ion_symbol_table_set_version(import1, 1)); \
    ION_ASSERT_OK(ion_symbol_table_set_version(import2, 1)); \
    ION_ASSERT_OK(ion_symbol_table_add_symbol(import1, &sym1, &sid)); \
    ION_ASSERT_OK(ion_symbol_table_add_symbol(import2, &sym2, &sid)); \
    ION_ASSERT_OK(ion_symbol_table_add_symbol(import2, &sym3, &sid)); \
    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog, import1)); \
    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog, import2));

#define ION_SYMBOL_TEST_OPEN_WRITER_WITH_IMPORTS(is_binary, imports, import_count) \
    ION_SYMBOL_TEST_DECLARE_WRITER; \
    ION_WRITER_OPTIONS writer_options; \
    ion_test_initialize_writer_options(&writer_options); \
    writer_options.encoding_psymbol_table_count = import_count; \
    writer_options.encoding_psymbol_table = imports; \
    writer_options.output_as_binary = is_binary; \
    writer_options.pcatalog = catalog; \
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream)); \
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));

TEST(IonSymbolTable, WriterAppendsLocalSymbolsOnFlush) {
    ION_SYMBOL_TEST_DECLARE_WRITER;
    ION_STRING sym1, sym2, sym3, sym4;

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym2));

    // This flushes the writer, but does not reset the symbol table context or write the IVM.
    ION_ASSERT_OK(ion_writer_flush(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym3)); // This gets SID 12.
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10)); // sym1 is still in scope.
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 12)); // This corresponds to sym3.

    // This flushes the writer and forces an IVM upon next write, thereby resetting the symbol table context.
    ION_ASSERT_OK(ion_writer_finish(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10)); // sym1 is no longer in scope. This refers to sym4.

    ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT("sym1\nsym2\nsym3\nsym1\nsym3\nsym4\nsym4");
}

TEST_P(BinaryAndTextTest, WriterAppendsLocalSymbolsWithImportsOnFlush) {
    // Add imports to the initial table, then append
    ION_STRING sym4;

    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ION_SYMBOL_TEST_POPULATE_CATALOG;
    ION_SYMBOL_TEST_OPEN_WRITER_WITH_IMPORTS(is_binary, import1, 2);

    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 12));

    ION_ASSERT_OK(ion_writer_flush(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym3)); // sym3 is already in the symbol table, with SID 12
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    if (is_binary) {
        ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 13)); // Corresponds to sym4.
    }
    else {
        // When local symbols are written by a text writer, they are not assigned SIDs, so trying to write SID 13 would
        // fail here.
        ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    }

    ION_SYMBOL_TEST_REWRITE_WITH_CATALOG_FROM_WRITER_AND_ASSERT_TEXT("sym1\nsym2\nsym3\nsym1\nsym3\nsym4\nsym4");
    ION_ASSERT_OK(ion_catalog_close(catalog)); // Closes the catalog and releases its tables.
}

TEST(IonSymbolTable, AppendingAfterIVMDoesNothing) {
    // LST-append syntax when the current symbol table is the system symbol table does nothing; the imports field is
    // treated as if it doesn't exist.
    // $4 $ion_symbol_table::{symbols:["sym"], imports:$ion_symbol_table} $10
    ION_SYMBOL_TEST_REWRITE_FROM_BUFFER_AND_ASSERT_TEXT(
        (BYTE *)"\xE0\x01\x00\xEA\x71\x04\xEC\x81\x83\xD9\x87\xB4\x83sym\x86\x71\x03\x71\x0A", 21,
        "name\nsym"
    );
}

TEST(IonSymbolTable, AppendingNoSymbolsDoesNotWriteSymbolTable) {
    // If, after flush, there are no additional local symbols, there is no need to write another LST.
    ION_SYMBOL_TEST_DECLARE_WRITER;
    ION_STRING sym1, sym2;

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, TRUE));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym2));

    // This flushes the writer, but does not reset the symbol table context or write the IVM.
    ION_ASSERT_OK(ion_writer_flush(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);
    ION_ASSERT_OK(ion_writer_write_int(writer, 0));
    // If any symbols were written since last flush, this would cause an LST-append. But only an int was written.
    ION_ASSERT_OK(ion_writer_flush(writer, &bytes_flushed));
    ASSERT_EQ(1, bytes_flushed); // int 0 occupies a single byte. An LST would require more.

    ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT("sym1\nsym2\n0");
}

TEST(IonSymbolTable, SharedSymbolTableCanBelongToMultipleCatalogs) {
    const char *ion_text_1 = "$ion_symbol_table::{imports:[{name:'''foo''', version: 1, max_id: 2}]} $10 $11";
    const char *ion_text_2 = "$ion_symbol_table::{imports:[{name:'''bar''', version: 1, max_id: 10}, {name:'''foo''', version: 1, max_id: 2}]} $20 $21";

    const char *foo_table = "$ion_shared_symbol_table::{name:'''foo''', version: 1, symbols:['''abc''', '''def''']}";

    hCATALOG catalog_1, catalog_2;
    hREADER reader_1, reader_2, shared_symtab_reader;
    ION_READER_OPTIONS reader_options_1, reader_options_2;
    hSYMTAB foo_table_shared;
    ION_STRING abc, def, sid_10, sid_11, sid_20, sid_21;
    ION_TYPE type;

    ION_ASSERT_OK(ion_string_from_cstr("abc", &abc));
    ION_ASSERT_OK(ion_string_from_cstr("def", &def));

    ION_ASSERT_OK(ion_test_new_text_reader(foo_table, &shared_symtab_reader));
    ION_ASSERT_OK(ion_reader_next(shared_symtab_reader, &type));
    ION_ASSERT_OK(ion_symbol_table_load(shared_symtab_reader, NULL, &foo_table_shared));
    ION_ASSERT_OK(ion_reader_close(shared_symtab_reader));

    ION_ASSERT_OK(ion_catalog_open(&catalog_1));
    ION_ASSERT_OK(ion_catalog_open(&catalog_2));

    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog_1, foo_table_shared));
    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog_2, foo_table_shared));

    ion_test_initialize_reader_options(&reader_options_1);
    reader_options_1.pcatalog = catalog_1;
    ion_test_initialize_reader_options(&reader_options_2);
    reader_options_2.pcatalog = catalog_2;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader_1, (BYTE *)ion_text_1, (SIZE)strlen(ion_text_1), &reader_options_1));
    ION_ASSERT_OK(ion_reader_open_buffer(&reader_2, (BYTE *)ion_text_2, (SIZE)strlen(ion_text_2), &reader_options_2));

    ION_ASSERT_OK(ion_reader_next(reader_1, &type));
    ION_ASSERT_OK(ion_reader_next(reader_2, &type));

    ION_ASSERT_OK(ion_reader_read_string(reader_1, &sid_10));
    ION_ASSERT_OK(ion_reader_read_string(reader_2, &sid_20));

    ION_ASSERT_OK(ion_reader_next(reader_1, &type));
    ION_ASSERT_OK(ion_reader_next(reader_2, &type));

    ION_ASSERT_OK(ion_reader_read_string(reader_1, &sid_11));
    ION_ASSERT_OK(ion_reader_read_string(reader_2, &sid_21));

    ASSERT_TRUE(assertIonStringEq(&sid_10, &sid_20));
    ASSERT_TRUE(assertIonStringEq(&sid_10, &abc));

    ASSERT_TRUE(assertIonStringEq(&sid_11, &sid_21));
    ASSERT_TRUE(assertIonStringEq(&sid_11, &def));

    ION_ASSERT_OK(ion_reader_close(reader_1));
    ION_ASSERT_OK(ion_reader_close(reader_2));

    ION_ASSERT_OK(ion_catalog_close(catalog_1));
    ION_ASSERT_OK(ion_catalog_close(catalog_2));
    ION_ASSERT_OK(ion_symbol_table_close(foo_table_shared));

}

TEST(IonSymbolTable, ManuallyWritingSymbolTableStructIsRecognizedAsSymbolTable) {
    // If the user manually writes a struct that is a local symbol table, it should become the active LST, and it
    // should be possible for the user to subsequently write any SID within the new table's max_id.
    // It should also be possible for the user to subsequently write additional symbols within the same context and
    // have them added to the symbol table. This means that instead of actually writing out the manually-written
    // symbol table up front, it must be intercepted, turned into a mutable ION_SYMBOL_TABLE, and written out at
    // flush as usual. Before the new context is started, the system should flush the previous one.
    ION_SYMBOL_TEST_DECLARE_WRITER;

    ION_STRING sym1, sym2;
    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, TRUE));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_sid(writer, ION_SYS_SID_SYMBOLS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym2));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end symbols list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    //ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10)); // This maps to sym1.
    // TODO implement this functionality
}

TEST(IonSymbolTable, ManuallyWriteSymbolTableAppendSucceeds) {
    // Like the previous test, but the manually written symbol table contains appended symbols.
}

TEST(IonSymbolTable, TextWritingKnownSymbolFromSIDResolvesText) {
    // If the user writes a SID in the import range and that import is found in the writer's imports list, that SID
    // should be resolved to its text representation. There is no need to include the local symbol table in the stream.
    ION_SYMBOL_TEST_POPULATE_CATALOG;
    ION_SYMBOL_TEST_OPEN_WRITER_WITH_IMPORTS(FALSE, import1, 2);
    BYTE *result;

    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_add_annotation_sid(writer, 11));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 12));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    assertStringsEqual("{sym1:sym2::sym3}", (char *)result, bytes_flushed);
}

TEST(IonSymbolTable, TextWritingSymbolFromNotFoundImportWritesIdentifierAndForcesSymbolTable) {
    // If the user writes a SID in the import range and that import is not found in the catalog, the SID must be
    // written as $<int> and the symbol table must be included in the stream. Future consumers may have access to
    // that import and be able to resolve the identifier.
}

TEST_P(BinaryAndTextTest, WritingOutOfRangeSIDFails) {
    // For both text and binary, manually writing a SID (from a pure SID or ION_SYMBOL with NULL text) that is out of
    // range of the current symbol table context should raise an error, since this condition must also raise an error
    // on read.
    ION_SYMBOL_TEST_DECLARE_WRITER;
    ION_STRING sym1;
    SID annotation_sids[2];

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    annotation_sids[0] = 4; // i.e. name
    annotation_sids[1] = 10; // out of range.

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_field_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &sym1));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_add_annotation_sid(writer, 10));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_annotation_sids(writer, annotation_sids, 2));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_annotation_sids(writer, annotation_sids, 1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_SYMBOL_TEST_REWRITE_FROM_WRITER_AND_ASSERT_TEXT("{sym1:name::sym1}");
}

TEST(IonSymbolTable, SettingSharedSymbolTableMaxIdLargerThanLengthOfSymbolsExtendsWithUnknownSymbols) {
    // If the user adds N symbols to a shared symbol table and sets that symbol table's maxId to N + M, there should
    // be M SIDs with unknown text.
}

TEST(IonSymbolTable, NullSlotsInSharedSymbolTableAreSIDsWithUnknownText) {
    // A shared symbol table with NULL elements within its symbols list are valid SID mappings with unknown text.
}

TEST_P(BinaryAndTextTest, WriterWithImportsListIncludesThoseImportsWithEveryNewLSTContext) {
    // A writer that was constructed with a list of shared imports to use must include those imports in each new local
    // symbol table context.
    ION_STRING sym4;

    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ION_SYMBOL_TEST_POPULATE_CATALOG;
    ION_SYMBOL_TEST_OPEN_WRITER_WITH_IMPORTS(is_binary, import1, 2);

    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 12));

    ION_ASSERT_OK(ion_writer_finish(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym3)); // sym3 is already in the symbol table, with SID 12
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    if (is_binary) {
        ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 13)); // Corresponds to sym4.
    }
    else {
        // When local symbols are written by a text writer, they are not assigned SIDs, so trying to write SID 13 would
        // fail here.
        ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    }

    ION_SYMBOL_TEST_REWRITE_WITH_CATALOG_FROM_WRITER_AND_ASSERT_TEXT("sym1\nsym2\nsym3\nsym1\nsym2\nsym3\nsym4\nsym4");
    ION_ASSERT_OK(ion_catalog_close(catalog)); // Closes the catalog and releases its tables.
}
