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
#include "ion_event_util.h"
#include "ion_event_equivalence.h"

// Creates a BinaryAndTextTest fixture instantiation for IonSymbolTable tests. This allows tests to be declared with
// the BinaryAndTextTest fixture and receive the is_binary flag with both the TRUE and FALSE values.
INSTANTIATE_TEST_CASE_BOOLEAN_PARAM(IonSymbolTable);

void populate_catalog(hCATALOG *pcatalog, ION_SYMBOL_TABLE *writer_imports[2]) {
    hSYMTAB import1, import2;
    ION_STRING import1_name, import2_name, sym1, sym2, sym3;
    SID sid;

    hCATALOG catalog = NULL;

    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name));
    ION_ASSERT_OK(ion_string_from_cstr("import2", &import2_name));
    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_catalog_open(&catalog));
    ION_ASSERT_OK(ion_symbol_table_open_with_type(&writer_imports[0], catalog, ist_SHARED));
    ION_ASSERT_OK(ion_symbol_table_open_with_type(&writer_imports[1], catalog, ist_SHARED));
    import1 = writer_imports[0];
    import2 = writer_imports[1];
    ION_ASSERT_OK(ion_symbol_table_set_name(import1, &import1_name));
    ION_ASSERT_OK(ion_symbol_table_set_name(import2, &import2_name));
    ION_ASSERT_OK(ion_symbol_table_set_version(import1, 1));
    ION_ASSERT_OK(ion_symbol_table_set_version(import2, 1));
    ION_ASSERT_OK(ion_symbol_table_add_symbol(import1, &sym1, &sid));
    ION_ASSERT_OK(ion_symbol_table_add_symbol(import2, &sym2, &sid));
    ION_ASSERT_OK(ion_symbol_table_add_symbol(import2, &sym3, &sid));
    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog, import1));
    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog, import2));
    *pcatalog = catalog;
}

void open_writer_with_imports(bool is_binary, hWRITER *pwriter, ION_STREAM **pstream, ION_WRITER_OPTIONS *pwriter_options, ION_SYMBOL_TABLE **imports, SIZE import_count) {
    hWRITER writer;
    ION_STREAM *stream;
    hCATALOG catalog = NULL;

    ion_event_initialize_writer_options(pwriter_options);
    ION_ASSERT_OK(ion_writer_options_initialize_shared_imports(pwriter_options));
    ION_ASSERT_OK(ion_writer_options_add_shared_imports_symbol_tables(pwriter_options, imports, import_count));

    pwriter_options->output_as_binary = is_binary;
    pwriter_options->pcatalog = catalog;

    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, pwriter_options));
    *pwriter = writer;
    *pstream = stream;
}

void rewrite_with_catalog(hCATALOG catalog, bool as_binary, BYTE *data, SIZE data_size, BYTE **p_output, SIZE *p_output_len) {
    hREADER reader = NULL;
    hWRITER writer = NULL;
    ION_READER_OPTIONS reader_options = {0};
    ION_WRITER_OPTIONS writer_options = {0};
    ION_STREAM *stream = NULL;

    ion_event_initialize_reader_options(&reader_options);
    reader_options.pcatalog = catalog;
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, data, data_size, &reader_options));
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));

    writer_options.output_as_binary = as_binary;
    ion_event_initialize_writer_options(&writer_options);
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));
    ION_ASSERT_OK(ion_writer_write_all_values(writer, reader));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, p_output, p_output_len));
}

void rewrite_and_assert_text_eq(BYTE *data, SIZE data_len, const char *expected) {
   BYTE *output_bytes = NULL;
   SIZE output_len = 0;

   ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(NULL, FALSE, data, data_len, &output_bytes, &output_len));

   ASSERT_NO_FATAL_FAILURE(assertStringsEqual(
         expected,
         (const char *)output_bytes,
         output_len
   ));
}

TEST(IonSymbolTable, WriterAppendsLocalSymbolsOnFlush) {
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    ION_STRING sym1, sym2, sym3, sym4;
    BYTE *written_bytes = NULL, *rewritten_bytes = NULL;
    SIZE written_len = 0, rewritten_len = 0;

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
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10)); // sym1 is still in scope.
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12)); // This corresponds to sym3.

    // This flushes the writer and forces an IVM upon next write, thereby resetting the symbol table context.
    ION_ASSERT_OK(ion_writer_finish(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10)); // sym1 is no longer in scope. This refers to sym4.
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));

    ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(NULL, FALSE, written_bytes, written_len, &rewritten_bytes, &rewritten_len));

    assertStringsEqual("sym1 sym2 sym3 sym1 sym3 sym4 sym4", (char *)rewritten_bytes, rewritten_len);
}

TEST_P(BinaryAndTextTest, WriterAppendsLocalSymbolsWithImportsOnFlush) {
    // Add imports to the initial table, then append
    ION_STRING sym3 = {0}, sym4 = {0};
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    ION_WRITER_OPTIONS writer_options;
    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};

    BYTE *written_bytes = NULL, *rewritten_bytes = NULL;
    SIZE written_len, rewritten_len;

    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));


    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));
    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(is_binary, &writer, &stream, &writer_options, imports, import_number));

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12));

    ION_ASSERT_OK(ion_writer_flush(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym3)); // sym3 is already in the symbol table, with SID 12
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    if (is_binary) {
        ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 13)); // Corresponds to sym4.
    }
    else {
        // When local symbols are written by a text writer, they are not assigned SIDs, so trying to write SID 13 would
        // fail here.
        ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    }

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(catalog, is_binary, written_bytes, written_len, &rewritten_bytes, &rewritten_len));

    assertStringsEqual(
          "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1},{name:\"import2\",version:1,max_id:2}]} sym1 sym2 sym3 sym1 sym3 sym4 sym4",
          (char *)rewritten_bytes,
          rewritten_len
    );

    free(written_bytes);
    free(rewritten_bytes);
    ION_ASSERT_OK(ion_catalog_close(catalog)); // Closes the catalog and releases its tables.
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
}

TEST(IonSymbolTable, AppendingAfterIVMDoesNothing) {
    // LST-append syntax when the current symbol table is the system symbol table does nothing; the imports field is
    // treated as if it doesn't exist.
    // $4 $ion_symbol_table::{symbols:["sym"], imports:$ion_symbol_table} $10
    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(
             (BYTE *)"\xE0\x01\x00\xEA\x71\x04\xEC\x81\x83\xD9\x87\xB4\x83sym\x86\x71\x03\x71\x0A", 21,
             "name sym"
    ));
}

TEST(IonSymbolTable, AppendingNoSymbolsDoesNotWriteSymbolTable) {
    // If, after flush, there are no additional local symbols, there is no need to write another LST.
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    ION_STRING sym1, sym2;

    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

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

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));

    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(
             written_bytes,
             written_len,
             "sym1 sym2 0"
    ));
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

    ion_event_initialize_reader_options(&reader_options_1);
    reader_options_1.pcatalog = catalog_1;
    ion_event_initialize_reader_options(&reader_options_2);
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

    ASSERT_TRUE(ion_equals_string(&sid_10, &sid_20));
    ASSERT_TRUE(ion_equals_string(&sid_10, &abc));

    ASSERT_TRUE(ion_equals_string(&sid_11, &sid_21));
    ASSERT_TRUE(ion_equals_string(&sid_11, &def));

    ION_ASSERT_OK(ion_reader_close(reader_1));
    ION_ASSERT_OK(ion_reader_close(reader_2));

    ION_ASSERT_OK(ion_catalog_close(catalog_1));
    ION_ASSERT_OK(ion_catalog_close(catalog_2));
    ION_ASSERT_OK(ion_symbol_table_close(foo_table_shared));

}

TEST_P(BinaryAndTextTest, ManuallyWritingSymbolTableStructIsRecognizedAsSymbolTable) {
    // If the user manually writes a struct that is a local symbol table, it should become the active LST, and it
    // should be possible for the user to subsequently write any SID within the new table's max_id.
    // It should also be possible for the user to subsequently write additional symbols within the same context and
    // have them added to the symbol table. This means that instead of actually writing out the manually-written
    // symbol table up front, it must be intercepted, turned into a mutable ION_SYMBOL_TABLE, and written out at
    // flush as usual. Before the new context is started, the system should flush the previous one.
    hWRITER writer;
    ION_STREAM *stream;
    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

    ION_STRING sym1, sym2;
    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_SYMBOLS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym2));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end symbols list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10)); // This maps to sym1.

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));

    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(
             written_bytes,
             written_len,
             "sym1"
    ));
}

TEST_P(BinaryAndTextTest, ManuallyWritingSymbolTableStructWithImportsIsRecognizedAsSymbolTable) {
    // Same as the previous test, but with imports.
    hWRITER writer;
    ION_STREAM *stream;
    const SIZE import_count = 2;
    ION_SYMBOL_TABLE *writer_imports[import_count] = {NULL, NULL};
    hCATALOG catalog = NULL;
    ION_STRING import1_name;

    BYTE *written_bytes = NULL, *rewritten_bytes = NULL;
    SIZE written_len = 0, rewritten_len = 0;

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, writer_imports));

    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name));

    ION_WRITER_OPTIONS writer_options;
    ion_event_initialize_writer_options(&writer_options);
    writer_options.pcatalog = catalog; // This contains 'import1' and 'import2'.
    writer_options.output_as_binary = is_binary;
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));

    ION_SYMBOL symbol_table_symbol, version_symbol, name_symbol;
    memset(&symbol_table_symbol, 0, sizeof (ION_SYMBOL));
    memset(&version_symbol, 0, sizeof (ION_SYMBOL));
    memset(&name_symbol, 0, sizeof (ION_SYMBOL));
    symbol_table_symbol.sid = ION_SYS_SID_SYMBOL_TABLE;
    ION_STRING_ASSIGN(&version_symbol.value, &ION_SYMBOL_VERSION_STRING);
    ION_STRING_ASSIGN(&name_symbol.import_location.name, &ION_SYMBOL_ION_STRING);
    name_symbol.import_location.location = ION_SYS_SID_NAME;

    ION_ASSERT_OK(ion_writer_add_annotation_symbol(writer, &symbol_table_symbol)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &ION_SYMBOL_IMPORTS_STRING));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &name_symbol));
    ION_ASSERT_OK(ion_writer_write_string(writer, &import1_name));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &version_symbol));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_MAX_ID));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end import struct
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end imports list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10)); // This maps to sym1.
                                                                           //
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));

    ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(catalog, FALSE, written_bytes, written_len, &rewritten_bytes, &rewritten_len));

    ION_ASSERT_OK(ion_catalog_close(catalog));

    assertStringsEqual(
             "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1}]} sym1",
             (const char *)rewritten_bytes,
             rewritten_len
     );
}

TEST_P(BinaryAndTextTest, ManuallyWritingSymbolTableStructWithImportsAndOpenContentIsRecognizedAsSymbolTable) {
    // Same as the previous test, but with imports.
    hWRITER writer;
    ION_STREAM *stream;
    hCATALOG catalog = NULL;
    const SIZE import_count = 2;
    ION_SYMBOL_TABLE *writer_imports[import_count] = {NULL, NULL};
    ION_STRING import1_name;
    SIZE bytes_flushed;


    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, writer_imports));

    ION_STRING foo_name;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo_name));
    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name)); \

    ION_WRITER_OPTIONS writer_options;
    ion_event_initialize_writer_options(&writer_options);
    writer_options.pcatalog = catalog; // This contains 'import1' and 'import2'.
    writer_options.output_as_binary = is_binary;
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));

    ION_SYMBOL symbol_table_symbol, foo_symbol, bar_symbol;
    memset(&symbol_table_symbol, 0, sizeof (ION_SYMBOL));
    memset(&foo_symbol, 0, sizeof (ION_SYMBOL));
    memset(&bar_symbol, 0, sizeof (ION_SYMBOL));
    ION_STRING_ASSIGN(&symbol_table_symbol.value, &ION_SYMBOL_SYMBOL_TABLE_STRING);
    symbol_table_symbol.sid = UNKNOWN_SID; // irrelevant.
    ION_STRING_ASSIGN(&foo_symbol.value, &foo_name);
    ION_STRING_ASSIGN(&bar_symbol.import_location.name, &ION_SYMBOL_ION_STRING);
    bar_symbol.import_location.location = ION_SYS_SID_IVM; // open content; ignored.

    ION_ASSERT_OK(ion_writer_add_annotation_symbol(writer, &symbol_table_symbol)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_IMPORTS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_NAME));
    ION_ASSERT_OK(ion_writer_write_string(writer, &import1_name));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &foo_name)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_add_annotation_symbol(writer, &bar_symbol)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &foo_symbol)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_write_int(writer, 123)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &bar_symbol)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_write_int(writer, 456)); // open content; ignored.
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_MAX_ID));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1)); // $10 = sym1
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end import struct
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_NAME));
    ION_ASSERT_OK(ion_writer_write_string(writer, &foo_name)); // Not in catalog.
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_MAX_ID));
    ION_ASSERT_OK(ion_writer_write_int(writer, 2)); // $11 and $12
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &foo_name)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_write_int(writer, 456)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end open content list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end import struct
    ION_ASSERT_OK(ion_writer_write_int(writer, 789)); // open content; ignored.
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end imports list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10)); // This maps to sym1.
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12)); // This maps to unknown text.

    hREADER reader;
    BYTE *result;
    ION_READER_OPTIONS reader_options;
    hWRITER roundtrip_writer;
    ION_COLLECTION *imports;
    ION_SYMBOL_TABLE *reader_symtab;
    ION_TYPE type;
    ION_SYMBOL symbol;

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    ion_event_initialize_reader_options(&reader_options);
    reader_options.pcatalog = catalog;
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, result, bytes_flushed, &reader_options));
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    writer_options.output_as_binary = FALSE;
    ION_ASSERT_OK(ion_writer_options_initialize_shared_imports(&writer_options));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &reader_symtab));
    ION_ASSERT_OK(ion_symbol_table_get_imports(reader_symtab, &imports));
    ION_ASSERT_OK(ion_writer_options_add_shared_imports(&writer_options, imports));
    ION_ASSERT_OK(ion_writer_open(&roundtrip_writer, stream, &writer_options));
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));

    ION_ASSERT_OK(ion_reader_read_ion_symbol(reader, &symbol));
    ION_ASSERT_OK(ion_writer_write_ion_symbol(roundtrip_writer, &symbol));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_read_ion_symbol(reader, &symbol));
    ION_ASSERT_OK(ion_writer_write_ion_symbol(roundtrip_writer, &symbol));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_catalog_close(catalog));
    free(result);

    ION_ASSERT_OK(ion_test_writer_get_bytes(roundtrip_writer, stream, &result, &bytes_flushed));
    assertStringsEqual("$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1},{name:\"foo\",version:1,max_id:2}]} sym1 $12", (char *)result, bytes_flushed);
    free(result);
}

TEST_P(BinaryAndTextTest, ManuallyWritingSymbolTableAfterAutomaticSymbolTableSucceeds) {
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING sym1, sym2, sym3, sym4;
    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym2));

    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_SYMBOLS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym3));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym4));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end symbols list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(written_bytes, written_len, "sym1 sym2 sym3 sym4"));
}

TEST_P(BinaryAndTextTest, ManuallyWritingSymbolTableWithDuplicateFieldsFails) {
    hWRITER writer;
    ION_STREAM *stream;
    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

    ION_STRING foo;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));
    ION_SYMBOL symbols_symbol, max_id_symbol, name_symbol;
    memset(&symbols_symbol, 0, sizeof (ION_SYMBOL));
    memset(&max_id_symbol, 0, sizeof (ION_SYMBOL));
    memset(&name_symbol, 0, sizeof (ION_SYMBOL));
    symbols_symbol.sid = ION_SYS_SID_SYMBOLS;
    ION_STRING_ASSIGN(&max_id_symbol.value, &ION_SYMBOL_MAX_ID_STRING);
    ION_STRING_ASSIGN(&name_symbol.import_location.name, &ION_SYMBOL_ION_STRING);
    name_symbol.import_location.location = ION_SYS_SID_NAME;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_IMPORTS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &name_symbol));
    ION_ASSERT_OK(ion_writer_write_string(writer, &foo));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_MAX_ID));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_writer_write_field_name_symbol(writer, &max_id_symbol));
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_writer_write_field_name_symbol(writer, &name_symbol));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end import struct
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end imports list
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_writer_write_field_name(writer, &ION_SYMBOL_IMPORTS_STRING));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &symbols_symbol));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &foo));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end symbols list
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_SYMBOLS));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11)); // foo

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(
             written_bytes,
             written_len,
             "$ion_symbol_table::{imports:[{name:\"foo\",version:1,max_id:1}]} foo"
    ));
}

TEST_P(BinaryAndTextTest, ManuallyWritingImportWithNoNameIsIgnored) {
    hWRITER writer;
    ION_STREAM *stream;
    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

    ION_STRING foo;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_SYMBOLS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &foo));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_IMPORTS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_NAME));
    ION_ASSERT_OK(ion_writer_write_int(writer, 123)); // This is not a string, so it's ignored.
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_MAX_ID));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_NAME));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    // NOTE: the ignored import had space for one symbol. If it weren't ignored, SID 10 would fall within its range.
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(
             written_bytes,
             written_len,
             "foo"
    ));
}

TEST_P(BinaryAndTextTest, ManuallyWritingAmbiguousImportFails) {
    hWRITER writer;
    ION_STREAM *stream;

    ION_STRING foo;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_IMPORTS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_NAME));
    ION_ASSERT_OK(ion_writer_write_string(writer, &foo));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ASSERT_EQ(IERR_INVALID_SYMBOL_TABLE, ion_writer_finish_container(writer));
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_close(writer));
    ion_stream_close(stream);
}

TEST_P(BinaryAndTextTest, ManuallyWriteSymbolTableAppendSucceeds) {
    hWRITER writer;
    ION_STREAM *stream;
    BYTE *written_bytes = NULL;
    SIZE written_len = 0, bytes_flushed = 0;
    ION_STRING sym1, sym2, sym3, sym4;

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym2));

    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_IMPORTS));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &ION_SYMBOL_SYMBOL_TABLE_STRING));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_SYMBOLS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym3));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    if (is_binary) {
        ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
        ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));
        ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12));
    }
    else {
        // Text writers never intern symbols with known text, so sym1 and sym2 never had SID mappings.
        // Because sym3 is manually interned, it gets SID 10.
        ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
        ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym2));
        ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    }

    // This flushes the writer and forces an IVM upon next write, thereby resetting the symbol table context.
    ION_ASSERT_OK(ion_writer_finish(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_and_assert_text_eq(
             written_bytes,
             written_len,
             "sym1 sym2 sym1 sym2 sym3 sym4"
    ));
}

TEST_P(BinaryAndTextTest, ManuallyWriteSymbolTableAppendWithImportsSucceeds) {
    // Tests a manually written appended symbol table.
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;
    ION_WRITER_OPTIONS writer_options = {0};

    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};

    BYTE *written_bytes = NULL, *rewritten_bytes = NULL;
    SIZE written_len = 0, rewritten_len = 0;

    ION_STRING sym3, sym4;
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ION_SYMBOL imports_symbol, symbol_table_symbol, symbols_symbol;
    memset(&imports_symbol, 0, sizeof(ION_SYMBOL));
    memset(&symbol_table_symbol, 0, sizeof(ION_SYMBOL));
    memset(&symbols_symbol, 0, sizeof(ION_SYMBOL));
    imports_symbol.sid = ION_SYS_SID_IMPORTS;
    ION_STRING_ASSIGN(&symbol_table_symbol.value, &ION_SYMBOL_SYMBOL_TABLE_STRING);
    ION_STRING_ASSIGN(&symbols_symbol.import_location.name, &ION_SYMBOL_ION_STRING);
    symbols_symbol.import_location.location = ION_SYS_SID_SYMBOLS;

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));
    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(is_binary, &writer, &stream, &writer_options, imports, import_number));

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12));

    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &imports_symbol));
    ION_ASSERT_OK(ion_writer_write_ion_symbol(writer, &symbol_table_symbol));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &symbols_symbol));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_write_string(writer, &sym4));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym3)); // sym3 is already in the symbol table, with SID 12
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 13)); // Corresponds to sym4.

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(catalog, is_binary, written_bytes, written_len, &rewritten_bytes, &rewritten_len));
    assertStringsEqual(
             "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1},{name:\"import2\",version:1,max_id:2}]} sym1 sym2 sym3 sym1 sym3 sym4 sym4",
             (const char *)rewritten_bytes,
             rewritten_len
    );
    ION_ASSERT_OK(ion_catalog_close(catalog)); // Closes the catalog and releases its tables.
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
}

TEST_P(BinaryAndTextTest, SymbolTableGettersWithManualLSTInProgressReturnsPreviousSymbolTable) {
    // Test that the previous LST remains in scope until the end of the next LST struct.
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING sym1;
    ION_SYMBOL_TABLE *symbol_table_1, *symbol_table_2;

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_get_symbol_table(writer, &symbol_table_1));

    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));

    ION_ASSERT_OK(ion_writer_get_symbol_table(writer, &symbol_table_2));
    ASSERT_EQ(symbol_table_1, symbol_table_2);

    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_writer_get_symbol_table(writer, &symbol_table_2));
    ASSERT_NE(symbol_table_1, symbol_table_2);

    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST_P(BinaryAndTextTest, SymbolTableSetterWithManualLSTInProgressFails) {
    // Tests that an error is raised if the user tries to set the symbol table while manually writing one.
    hWRITER writer;
    ION_STREAM *stream;
    ION_SYMBOL_TABLE *symbol_table;

    ION_ASSERT_OK(ion_symbol_table_open(&symbol_table, NULL));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));

    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));

    ASSERT_EQ(IERR_INVALID_STATE, ion_writer_set_symbol_table(writer, symbol_table));

    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_symbol_table_close(symbol_table));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST(IonSymbolTable, TextWritingKnownSymbolFromSIDResolvesText) {
    // If the user writes a SID in the import range and that import is found in the writer's imports list, that SID
    // should be resolved to its text representation. There is no need to include the local symbol table in the stream.
    hWRITER writer = NULL;
    hCATALOG catalog = NULL;
    ION_STREAM *stream = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};
    ION_WRITER_OPTIONS writer_options = {0};

    SIZE bytes_flushed = 0;

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));
    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(FALSE, &writer, &stream, &writer_options, imports, import_number));

    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
    BYTE *result;

    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, 11));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    assertStringsEqual(
            "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1},{name:\"import2\",version:1,max_id:2}]} {sym1:sym2::sym3}",
            (char *)result, bytes_flushed
    );
    free(result);

    ion_catalog_close(catalog);
}

TEST(IonSymbolTable, TextWritingSymbolWithUnknownTextAsSidFromImportWritesIdentifierAndForcesSymbolTable) {
    // If the user writes a local SID in the import range and that import is not found in the catalog, the SID must be
    // written as $<int> and the symbol table must be included in the stream. Future consumers may have access to
    // that import and be able to resolve the identifier.
    // This test also verifies that a shared symbol table with NULL elements within its symbols list are valid SID
    // mappings with unknown text.
    const char *shared_table = "$ion_shared_symbol_table::{name:'''foo''', version: 1, symbols:['''abc''', null, '''def''']}";
    hREADER shared_symtab_reader;
    ION_TYPE type;
    ION_SYMBOL_TABLE *import;
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;
    ION_WRITER_OPTIONS writer_options = {0};
    BYTE *result;
    SIZE bytes_flushed;

    ION_ASSERT_OK(ion_test_new_text_reader(shared_table, &shared_symtab_reader));
    ION_ASSERT_OK(ion_reader_next(shared_symtab_reader, &type));
    ION_ASSERT_OK(ion_symbol_table_load(shared_symtab_reader, NULL, &import));
    ION_ASSERT_OK(ion_reader_close(shared_symtab_reader));

    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(FALSE, &writer, &stream, &writer_options, &import, 1));
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    assertStringsEqual("$ion_symbol_table::{imports:[{name:\"foo\",version:1,max_id:3}]} abc $11 def", (char *)result, bytes_flushed);
    ion_symbol_table_close(import);
    free(result);
}

TEST_P(BinaryAndTextTest, WritingSymbolTokensWithUnknownTextFromImport) {
    // If the user writes a symbol token with an import location using the writer, writing should succeed if the
    // import is one of the shared symbol tables the writer is configured to use. Being in the catalog is not sufficient
    // because (in the case of the text writer) the symbol table has already been written (to avoid buffering) by the
    // time the writer reaches the value region of the stream -- UNLESS the import is found in the catalog AND the text
    // is known. In that case, for text writers, the text can simply be written; for binary writers, the text can be
    // interned into the LST and a local SID written.
    // If the user writes an ION_SYMBOL from an import and that import is not found in the catalog, the SID must be
    // written as $<int> and the symbol table must be included in the stream. Future consumers may have access to
    // that import and be able to resolve the identifier.
    const char *shared_table = "$ion_shared_symbol_table::{name:'''foo''', version: 1, symbols:['''abc''', null, '''def''']}";
    hREADER shared_symtab_reader;
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;
    ION_WRITER_OPTIONS writer_options = {0};
    ION_TYPE type;
    ION_SYMBOL_TABLE *import;
    ION_CATALOG *catalog = NULL;
    BYTE *result;
    SIZE bytes_flushed = 0;

    ION_STRING foo;
    ION_SYMBOL sym1, sym2, sym3;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));
    memset(&sym1, 0, sizeof(ION_SYMBOL));
    memset(&sym2, 0, sizeof(ION_SYMBOL));
    memset(&sym3, 0, sizeof(ION_SYMBOL));
    ION_STRING_ASSIGN(&sym1.import_location.name, &foo);
    sym1.import_location.location = 1;
    ION_STRING_ASSIGN(&sym2.import_location.name, &foo);
    sym2.import_location.location = 2;
    ION_STRING_ASSIGN(&sym3.import_location.name, &foo);
    sym3.import_location.location = 3;

    ION_ASSERT_OK(ion_test_new_text_reader(shared_table, &shared_symtab_reader));
    ION_ASSERT_OK(ion_reader_next(shared_symtab_reader, &type));
    ION_ASSERT_OK(ion_symbol_table_load(shared_symtab_reader, NULL, &import));
    ION_ASSERT_OK(ion_reader_close(shared_symtab_reader));

    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(FALSE, &writer, &stream, &writer_options, &import, 1));

    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_write_annotation_symbols(writer, &sym2, 1));
    ION_ASSERT_OK(ion_writer_write_ion_symbol(writer, &sym3));

    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    if (!is_binary) {
        assertStringsEqual("$ion_symbol_table::{imports:[{name:\"foo\",version:1,max_id:3}]} {abc:$11::def}", (char *)result, bytes_flushed);
    }
    else {
        BYTE *rewritten_bytes = NULL;
        SIZE rewritten_len = 0;
        ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(catalog, is_binary, result, bytes_flushed, &rewritten_bytes, &rewritten_len));
        assertStringsEqual("$ion_symbol_table::{imports:[{name:\"foo\",version:1,max_id:3}]} {abc:$11::def}", (char *)rewritten_bytes, rewritten_len);
        free(rewritten_bytes);
    }
    free(result);
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
    ION_ASSERT_OK(ion_symbol_table_close(import));
}

TEST_P(BinaryAndTextTest, WritingSymbolTokensWithUnknownTextFromCatalog) {
    // Tests that a writer can write symbol tokens with import locations that are matched by the catalog even when the
    // matched shared symbol tables are not in the writer's imports list, as long as the text is known.
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    hCATALOG catalog = NULL;
    ION_SYMBOL_TABLE *imports[2];

    ION_STRING import1_name, import2_name;
    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name));
    ION_ASSERT_OK(ion_string_from_cstr("import2", &import2_name));

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));

    BYTE *result;
    ION_WRITER_OPTIONS writer_options;
    ion_event_initialize_writer_options(&writer_options);
    writer_options.output_as_binary = is_binary;
    writer_options.pcatalog = catalog;
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));

    ION_SYMBOL sym1_loc, sym2_loc, sym3_loc;
    memset(&sym1_loc, 0, sizeof(ION_SYMBOL));
    memset(&sym2_loc, 0, sizeof(ION_SYMBOL));
    memset(&sym3_loc, 0, sizeof(ION_SYMBOL));
    ION_STRING_ASSIGN(&sym1_loc.import_location.name, &import1_name);
    sym1_loc.import_location.location = 1;
    ION_STRING_ASSIGN(&sym2_loc.import_location.name, &import2_name);
    sym2_loc.import_location.location = 1;
    ION_STRING_ASSIGN(&sym3_loc.import_location.name, &import2_name);
    sym3_loc.import_location.location = 2;

    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, &sym1_loc));
    ION_ASSERT_OK(ion_writer_write_annotation_symbols(writer, &sym2_loc, 1));
    ION_ASSERT_OK(ion_writer_write_ion_symbol(writer, &sym3_loc));

    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    if (!is_binary) {
        assertStringsEqual("{sym1:sym2::sym3}", (char *)result, bytes_flushed);
    }
    else {
        BYTE *rewritten_bytes = NULL;
        SIZE rewritten_len = 0;
        ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(
                    catalog,
                    is_binary,
                    result,
                    bytes_flushed,
                    &rewritten_bytes,
                    &rewritten_len
        ));
        assertStringsEqual("{sym1:sym2::sym3}", (char *)rewritten_bytes, rewritten_len);
        free(rewritten_bytes);
    }
    free(result);
    ION_ASSERT_OK(ion_catalog_close(catalog));
}

TEST_P(BinaryAndTextTest, WritingInvalidIonSymbolFails) {
    // Tests that an invalid ION_SYMBOL (undefined text, import location, and local SID) raises an error.
    hWRITER writer;
    ION_STREAM *stream;
    ION_WRITER_OPTIONS writer_options;
    ION_SYMBOL symbol;
    memset(&symbol, 0, sizeof(ION_SYMBOL));
    symbol.sid = UNKNOWN_SID;

    ion_event_initialize_writer_options(&writer_options);
    writer_options.output_as_binary = is_binary;
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_annotation_symbols(writer, &symbol, 1));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_ion_symbol(writer, &symbol));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_field_name_symbol(writer, &symbol));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST_P(BinaryAndTextTest, WritingIonSymbolWithUnknownTextNotFoundInImportsOrCatalogFails) {
    // Tests that an ION_SYMBOL with a valid import location but undefined text, which is not found in the writer's
    // shared imports and IS found in the writer's catalog, but with unknown text, raises an error.
    hWRITER writer;
    ION_STREAM *stream;
    hCATALOG catalog = NULL;
    ION_SYMBOL_TABLE *imports[2] = {NULL, NULL};
    ION_WRITER_OPTIONS writer_options = {0};


    const char *shared_table = "$ion_shared_symbol_table::{name:'''foo''', version: 1, symbols:['''abc''', null, '''def''']}";
    hREADER shared_symtab_reader;
    ION_TYPE type;
    ION_SYMBOL_TABLE *import;

    ION_STRING foo;
    ION_SYMBOL symbol;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));
    memset(&symbol, 0, sizeof(ION_SYMBOL));
    ION_STRING_ASSIGN(&symbol.import_location.name, &foo);
    symbol.import_location.location = 2; // $11, unknown text

    ION_ASSERT_OK(ion_test_new_text_reader(shared_table, &shared_symtab_reader));
    ION_ASSERT_OK(ion_reader_next(shared_symtab_reader, &type));
    ION_ASSERT_OK(ion_symbol_table_load(shared_symtab_reader, NULL, &import));
    ION_ASSERT_OK(ion_reader_close(shared_symtab_reader));

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));

    ION_ASSERT_OK(ion_catalog_add_symbol_table(catalog, import)); // In the catalog, not in the writer's imports.

    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(is_binary, &writer, &stream, &writer_options, imports, 2));

    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_annotation_symbols(writer, &symbol, 1));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_ion_symbol(writer, &symbol));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_field_name_symbol(writer, &symbol));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
    ION_ASSERT_OK(ion_symbol_table_close(import));
    ION_ASSERT_OK(ion_catalog_close(catalog));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST(IonSymbolTable, TextWritingSymbolWithUnknownTextFromManuallyWrittenSymbolTableWritesIdentifierAndForcesSymbolTable) {
    // If the user writes a SID in the import range of a manually written symbol table with import(s) and that import is
    // not found in the catalog, the SID must be written as $<int> and the symbol table must be included in the stream.
    // Future consumers may have access to that import and be able to resolve the identifier.
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;
    SIZE bytes_flushed = 0;

    BYTE *result;
    ION_STRING foo;
    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, FALSE));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, ION_SYS_SID_SYMBOL_TABLE)); // $ion_symbol_table
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_IMPORTS));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_LIST));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_NAME));
    ION_ASSERT_OK(ion_writer_write_string(writer, &foo));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_VERSION));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_test_writer_write_field_name_sid(writer, ION_SYS_SID_MAX_ID));
    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end import struct
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end imports list
    ION_ASSERT_OK(ion_writer_finish_container(writer)); // end LST struct

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10)); // $10 (unknown text).

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));
    assertStringsEqual("$ion_symbol_table::{imports:[{name:\"foo\",version:1,max_id:1}]} $10", (char *)result, bytes_flushed);
    free(result);
}

TEST_P(BinaryAndTextTest, WritingOutOfRangeSIDFails) {
    // For both text and binary, manually writing a SID (from a pure SID or ION_SYMBOL with NULL text) that is out of
    // range of the current symbol table context should raise an error, since this condition must also raise an error
    // on read.
    hWRITER writer = NULL;
    ION_STREAM *stream = NULL;
    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

    ION_STRING sym1;
    ION_SYMBOL annotation_symbols[2];

    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    memset(&annotation_symbols[0], 0, sizeof(ION_SYMBOL));
    memset(&annotation_symbols[1], 0, sizeof(ION_SYMBOL));
    annotation_symbols[0].sid = 4; // i.e. name
    annotation_symbols[1].sid = 10; // out of range.


    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_test_writer_write_field_name_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_field_name(writer, &sym1));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_test_writer_add_annotation_sid(writer, 10));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_writer_write_annotation_symbols(writer, annotation_symbols, 2));
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_write_annotation_symbols(writer, annotation_symbols, 1));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym1));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    rewrite_and_assert_text_eq(written_bytes, written_len, "{sym1:name::sym1}");
}

TEST(IonSymbolTable, SettingSharedSymbolTableMaxIdLargerThanLengthOfSymbolsExtendsWithUnknownSymbols) {
    // If the user adds N symbols to a shared symbol table and sets that symbol table's maxId to N + M, there should
    // be M SIDs with unknown text.
    // NOTE: the data below declares import1 with version 2, which is greater than version 1 available in the catalog.
    // Because the max_id is defined in the import declaration, the catalog will return version 1 as the best match and
    // allocate enough symbols with unknown text to match the declared max_id of 3. As a result, import1's SIDs will
    // range from 10 through 12 (with 11 and 12 having unknown text); import2's SIDs will range from 13 through 14.
    const char *ion_data = "$ion_symbol_table::{imports:[{name:\"import1\",version:2,max_id:3},{name:\"import2\",version:1,max_id:2}]} $10 $11 $12 $13 $14";
    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};
    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));

    hREADER reader;
    ION_READER_OPTIONS reader_options;
    ION_TYPE type;
    ion_event_initialize_reader_options(&reader_options);
    reader_options.pcatalog = catalog;

    ION_STRING sid_10, sid_11, sid_12, sid_13, sid_14;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, (SIZE)strlen(ion_data), &reader_options));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &sid_10));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &sid_11));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &sid_12));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &sid_13));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &sid_14));

    assertStringsEqual("sym1", (char *)sid_10.value, (SIZE)sid_10.length);
    ASSERT_TRUE(ION_STRING_IS_NULL(&sid_11));
    ASSERT_TRUE(ION_STRING_IS_NULL(&sid_12));
    assertStringsEqual("sym2", (char *)sid_13.value, (SIZE)sid_13.length);
    assertStringsEqual("sym3", (char *)sid_14.value, (SIZE)sid_14.length);

    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_catalog_close(catalog));
}

TEST_P(BinaryAndTextTest, WriterWithImportsListIncludesThoseImportsWithEveryNewLSTContext) {
    // A writer that was constructed with a list of shared imports to use must include those imports in each new local
    // symbol table context.
    ION_STRING sym4;
    hWRITER writer = NULL;
    ION_WRITER_OPTIONS writer_options = {0};
    ION_STREAM *stream = NULL;
    SIZE bytes_flushed = 0;
    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *writer_imports[import_number] = {NULL, NULL};
    ION_STRING sym3;

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, writer_imports));

    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));
    ION_ASSERT_OK(ion_string_from_cstr("sym4", &sym4));

    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(is_binary, &writer, &stream, &writer_options, writer_imports, import_number));

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 12));

    ION_ASSERT_OK(ion_writer_finish(writer, &bytes_flushed));
    ASSERT_NE(0, bytes_flushed);

    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym3)); // sym3 is already in the symbol table, with SID 12
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    if (is_binary) {
        ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 13)); // Corresponds to sym4.
    }
    else {
        // When local symbols are written by a text writer, they are not assigned SIDs, so trying to write SID 13 would
        // fail here.
        ION_ASSERT_OK(ion_writer_write_symbol(writer, &sym4));
    }

    BYTE *written_bytes = NULL, *rewritten_bytes = NULL;
    SIZE written_len = 0, rewritten_len = 0;

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(catalog, FALSE, written_bytes, written_len, &rewritten_bytes, &rewritten_len));
    assertStringsEqual(
            "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1},{name:\"import2\",version:1,max_id:2}]} sym1 sym2 sym3 sym1 sym2 sym3 sym4 sym4",
            (char *)rewritten_bytes,
            rewritten_len
    );
    ION_ASSERT_OK(ion_catalog_close(catalog)); // Closes the catalog and releases its tables.
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
}

TEST_P(BinaryAndTextTest, FlushingOrFinishingOrClosingWriterBelowTopLevelFails) {
    // Symbol table structs are only treated as system values at the top level. Both flushing and finishing have the
    // potential to require a symbol table to be written immediately afterward. Therefore, these must only be done
    // at the top level.
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed = 0;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_flush(writer, &bytes_flushed));
    ASSERT_EQ(0, bytes_flushed);
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_finish(writer, &bytes_flushed));
    ASSERT_EQ(0, bytes_flushed);
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_close(writer));

    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST_P(BinaryAndTextTest, ClosingWriterWithPendingLobFails) {
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed = 0;

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_start_lob(writer, tid_CLOB));
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_flush(writer, &bytes_flushed));
    ASSERT_EQ(0, bytes_flushed);
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_finish(writer, &bytes_flushed));
    ASSERT_EQ(0, bytes_flushed);
    ASSERT_EQ(IERR_UNEXPECTED_EOF, ion_writer_close(writer));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST(IonSymbolTable, LoadSymbolTableWithAnnotationSecondFails) {
    const char *ion_data = "annotated::$ion_symbol_table::{'''symbols''':['''foo''']}";
    hREADER reader;
    hSYMTAB symbol_table;
    ION_TYPE type;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_data, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(IERR_NOT_A_SYMBOL_TABLE, ion_symbol_table_load(reader, reader, &symbol_table));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST(IonSymbolTable, ReadThenWriteSymbolsWithUnknownText) {
    const char *ion_data = "$ion_symbol_table::{imports:[{name:\"foo\",version:1,max_id:2}]} {$10:$11}";
    hREADER reader;
    ION_TYPE type;
    ION_SYMBOL_TABLE *reader_symtab;
    ION_COLLECTION *imports;
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed = 0;
    ION_WRITER_OPTIONS writer_options;
    BYTE *result;
    SID sid;
    ION_SYMBOL *symbol;

    ION_ASSERT_OK(ion_test_new_text_reader(ion_data, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_get_symbol_table(reader, &reader_symtab));
    ION_ASSERT_OK(ion_symbol_table_get_imports(reader_symtab, &imports));

    ion_event_initialize_writer_options(&writer_options);
    ION_ASSERT_OK(ion_writer_options_initialize_shared_imports(&writer_options));
    ION_ASSERT_OK(ion_writer_options_add_shared_imports(&writer_options, imports));
    writer_options.output_as_binary = FALSE;
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));

    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_writer_start_container(writer, type));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_get_field_name_symbol(reader, &symbol));
    ION_ASSERT_OK(ion_writer_write_field_name_symbol(writer, symbol));
    ION_ASSERT_OK(ion_test_reader_read_symbol_sid(reader, &sid));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, sid));
    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_writer_finish_container(writer));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    assertStringsEqual(ion_data, (char *)result, bytes_flushed);
    free(result);
}

TEST_P(BinaryAndTextTest, WriteAnnotationsFromSidAndText) {
    // Tests that annotation SIDs and text may be mixed on the same value. Previously, this was not possible.
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING foo;

    ION_ASSERT_OK(ion_string_from_cstr("foo", &foo));

    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    ION_ASSERT_OK(ion_writer_add_annotation(writer, &foo));
    ION_ASSERT_OK(ion_test_writer_add_annotation_sid(writer, 4));
    ION_ASSERT_OK(ion_writer_write_int(writer, 123));

    BYTE *written_bytes = NULL, *rewritten_bytes = NULL;
    SIZE written_len = 0, rewritten_len = 0;

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    ASSERT_NO_FATAL_FAILURE(rewrite_with_catalog(NULL, FALSE, written_bytes, written_len, &rewritten_bytes, &rewritten_len));
    assertStringsEqual(
            "foo::name::123",
            (char *)rewritten_bytes,
            rewritten_len
    );
}

TEST_P(BinaryAndTextTest, ReaderCorrectlySetsImportLocation) {
    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};
    ION_STRING import1_name, import2_name, sym1, sym2, sym3;

    hREADER reader;
    ION_TYPE type;
    const char *ion_data = NULL;
    size_t ion_data_len;
    if (is_binary) {
        ion_data = "\xE0\x01\x00\xEA\xEE\xA9\x81\x83\xDE\xA5\x86\xBE\xA2\xDE\x8F\x84\x87import1\x85\x21\x01\x88\x21\x01\xDE\x8F\x84\x87import2\x85\x21\x01\x88\x21\x02\xD6\x8A\xE4\x81\x8B\x71\x0C";
        ion_data_len = 54;
    }
    else {
        ion_data = "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1}, {name:\"import2\",version:1,max_id:2}]} {$10:$11::$12}";
        ion_data_len = strlen((char *)ion_data);
    }
    ION_SYMBOL annotation[1], *field_name, value;
    SIZE annotation_count;
    ION_READER_OPTIONS reader_options;


    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));

    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name));
    ION_ASSERT_OK(ion_string_from_cstr("import2", &import2_name));
    ION_ASSERT_OK(ion_string_from_cstr("sym1", &sym1));
    ION_ASSERT_OK(ion_string_from_cstr("sym2", &sym2));
    ION_ASSERT_OK(ion_string_from_cstr("sym3", &sym3));

    ion_event_initialize_reader_options(&reader_options);
    reader_options.pcatalog = catalog;
    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, ion_data_len, &reader_options));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_step_in(reader));

    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_get_annotation_symbols(reader, annotation, 1, &annotation_count));
    ASSERT_EQ(1, annotation_count);
    ASSERT_TRUE(ion_equals_string(&import2_name, &annotation[0].import_location.name));
    ASSERT_EQ(1, annotation[0].import_location.location);
    ASSERT_TRUE(ion_equals_string(&sym2, &annotation[0].value));
    ASSERT_EQ(11, annotation[0].sid);

    ION_ASSERT_OK(ion_reader_get_field_name_symbol(reader, &field_name));
    ASSERT_TRUE(ion_equals_string(&import1_name, &field_name->import_location.name));
    ASSERT_EQ(1, field_name->import_location.location);
    ASSERT_TRUE(ion_equals_string(&sym1, &field_name->value));
    ASSERT_EQ(10, field_name->sid);

    ION_ASSERT_OK(ion_reader_read_ion_symbol(reader, &value));
    ASSERT_TRUE(ion_equals_string(&import2_name, &value.import_location.name));
    ASSERT_EQ(2, value.import_location.location);
    ASSERT_TRUE(ion_equals_string(&sym3, &value.value));
    ASSERT_EQ(12, value.sid);

    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_catalog_close(catalog));

}

TEST_P(BinaryAndTextTest, LocalSymbolHasNoImportLocation) {
    hREADER reader;
    ION_TYPE type;
    const char *ion_data = NULL;
    size_t ion_data_len;
    if (is_binary) {
        ion_data = "\xE0\x01\x00\xEA\xEE\x92\x81\x83\xDE\x8E\x87\xBC\x83zoo\x83zar\x83zaz\xD6\x8A\xE4\x81\x8B\x71\x0C";
        ion_data_len = 31;
    }
    else {
        ion_data = "{zoo:zar::zaz}";
        ion_data_len = strlen(ion_data);
    }
    ION_SYMBOL annotation[1], *field_name, value;
    ION_STRING zoo, zar, zaz;
    SIZE annotation_count;

    ION_ASSERT_OK(ion_string_from_cstr("zoo", &zoo));
    ION_ASSERT_OK(ion_string_from_cstr("zar", &zar));
    ION_ASSERT_OK(ion_string_from_cstr("zaz", &zaz));

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, ion_data_len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);

    ION_ASSERT_OK(ion_reader_get_field_name_symbol(reader, &field_name));
    ASSERT_TRUE(ion_equals_string(&zoo, &field_name->value));
    ASSERT_TRUE(ION_SYMBOL_IMPORT_LOCATION_IS_NULL(field_name));
    ASSERT_EQ(is_binary ? 10 : UNKNOWN_SID, field_name->sid);

    ION_ASSERT_OK(ion_reader_get_annotation_symbols(reader, annotation, 1, &annotation_count));
    ASSERT_EQ(1, annotation_count);
    ASSERT_TRUE(ion_equals_string(&zar, &annotation[0].value));
    ASSERT_TRUE(ION_SYMBOL_IMPORT_LOCATION_IS_NULL(&annotation[0]));
    ASSERT_EQ(is_binary ? 11 : UNKNOWN_SID, annotation[0].sid);

    ION_ASSERT_OK(ion_reader_read_ion_symbol(reader, &value));
    ASSERT_TRUE(ion_equals_string(&zaz, &value.value));
    ASSERT_TRUE(ION_SYMBOL_IMPORT_LOCATION_IS_NULL(&value));
    ASSERT_EQ(is_binary ? 12 : UNKNOWN_SID, value.sid);

    ION_ASSERT_OK(ion_reader_step_out(reader));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST_P(BinaryAndTextTest, ReadingOutOfRangeAnnotationSIDFails) {
    // Tests that out-of-range annotation SIDs fail consistently in both text and binary.
    hREADER reader;
    ION_TYPE type;
    const char *ion_data = NULL;
    size_t ion_data_len;
    if (is_binary) {
        ion_data = "\xE0\x01\x00\xEA\xE3\x81\x8A\x20";
        ion_data_len = 8;
    }
    else {
        ion_data = "$10::0";
        ion_data_len = strlen(ion_data);
    }
    ION_SYMBOL annotations[1];
    SIZE annotation_count;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, ion_data_len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_reader_get_annotation_symbols(reader, annotations, 1, &annotation_count));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST_P(BinaryAndTextTest, ReadingOutOfRangeFieldNameSIDFails) {
    // Tests that out-of-range field name SIDs fail consistently in both text and binary.
    hREADER reader;
    ION_TYPE type;
    const char *ion_data = NULL;
    size_t ion_data_len;
    if (is_binary) {
        ion_data = "\xE0\x01\x00\xEA\xD2\x8A\x20";
        ion_data_len = 7;
    }
    else {
        ion_data = "{$10:0}";
        ion_data_len = strlen(ion_data);
    }
    ION_SYMBOL *field_name;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, ion_data_len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_STRUCT, type);
    ION_ASSERT_OK(ion_reader_step_in(reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_INT, type);
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_reader_get_field_name_symbol(reader, &field_name));
    ION_ASSERT_OK(ion_reader_close(reader));
}

TEST_P(BinaryAndTextTest, ReadingOutOfRangeSymbolValueSIDFails) {
    // Tests that out-of-range symbol value SIDs fail consistently in both text and binary.
    hREADER reader;
    ION_TYPE type;
    const char *ion_data = NULL;
    size_t ion_data_len;
    if (is_binary) {
        ion_data = "\xE0\x01\x00\xEA\x71\x0A";
        ion_data_len = 6;
    }
    else {
        ion_data = "$10";
        ion_data_len = strlen(ion_data);
    }
    ION_SYMBOL symbol;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, ion_data_len, NULL));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ASSERT_EQ(IERR_INVALID_SYMBOL, ion_reader_read_ion_symbol(reader, &symbol));
    ION_ASSERT_OK(ion_reader_close(reader));
}

iERR _ion_symbol_test_notify_context_change(void *context, ION_COLLECTION *imports) {
    iENTER;
    ION_SYMBOL_TABLE_IMPORT *import;
    ION_STRING import1_name, import2_name;
    int *changes = (int *)context;

    ion_string_from_cstr("import1", &import1_name);
    ion_string_from_cstr("import2", &import2_name);

    if (ION_COLLECTION_SIZE(imports) != 1) {
        FAILWITH(IERR_INVALID_STATE);
    }
    import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_head(imports);
    switch(*changes) {
        case 0:
            if (!ION_STRING_EQUALS(&import1_name, &import->descriptor.name)) {
                FAILWITH(IERR_INVALID_STATE);
            }
            break;
        case 1:
            if (!ION_STRING_EQUALS(&import2_name, &import->descriptor.name)) {
                FAILWITH(IERR_INVALID_STATE);
            }
            break;
        default:
            FAILWITH(IERR_INVALID_STATE);
    }
    *changes = *changes + 1;
    iRETURN;
}

TEST(IonSymbolTable, ReaderNotifiesWhenSymbolTableContextChanges) {
    const char *ion_data = "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1}]} $10 $ion_symbol_table::{imports:[{name:\"import2\",version:1,max_id:2}]} $11";
    hREADER reader;
    ION_TYPE type;
    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    int changes = 0;
    options.context_change_notifier.context = &changes;
    options.context_change_notifier.notify = &_ion_symbol_test_notify_context_change;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, strlen(ion_data), &options));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_EQ(2, changes);
}

TEST(IonSymbolTable, ReaderDoesNotNotifyWhenSymbolTableContextDoesNotChange) {
    const char *ion_data = "$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1}]} $10 $ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1}]} $10";
    hREADER reader;
    ION_TYPE type;
    ION_READER_OPTIONS options;
    ion_event_initialize_reader_options(&options);
    int changes = 0;
    options.context_change_notifier.context = &changes;
    options.context_change_notifier.notify = &_ion_symbol_test_notify_context_change;

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, strlen(ion_data), &options));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_close(reader));

    ASSERT_EQ(1, changes);
}

TEST_P(BinaryAndTextTest, WriterAcceptsImportsAfterConstruction) {
    hWRITER writer;
    ION_STREAM *stream;
    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};
    ION_WRITER_OPTIONS writer_options = {0};
    hSYMTAB import1, import2;

    ION_STRING import1_name, import2_name;
    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name));
    ION_ASSERT_OK(ion_string_from_cstr("import2", &import2_name));

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));
    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(is_binary, &writer, &stream, &writer_options, imports, import_number));
    import1 = imports[0];
    import2 = imports[1];

    ION_STRING foo, bar;
    ION_SYMBOL_TABLE_IMPORT *foo_import, *bar_import, import1_import, import2_import;
    ION_COLLECTION new_imports_1, new_imports_2;
    ION_SYMBOL_TABLE *writer_table;
    ION_COLLECTION *writer_table_import_list;
    BOOL contains_import;

    ion_string_from_cstr("foo", &foo);
    ion_string_from_cstr("bar", &bar);

    _ion_collection_initialize(writer, &new_imports_1, sizeof(ION_SYMBOL_TABLE_IMPORT));
    _ion_collection_initialize(writer, &new_imports_2, sizeof(ION_SYMBOL_TABLE_IMPORT));

    foo_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports_1);
    bar_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports_2);

    ION_STRING_ASSIGN(&foo_import->descriptor.name, &foo);
    foo_import->descriptor.max_id = 10;
    foo_import->descriptor.version = 2;

    ION_STRING_ASSIGN(&bar_import->descriptor.name, &bar);
    bar_import->descriptor.max_id = 3;
    bar_import->descriptor.version = 1;

    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &new_imports_1));
    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &new_imports_2));

    ION_ASSERT_OK(ion_writer_get_symbol_table(writer, &writer_table));

    ION_ASSERT_OK(ion_symbol_table_get_imports(writer_table, &writer_table_import_list));
    ION_ASSERT_OK(_ion_collection_contains(writer_table_import_list, foo_import, &_ion_symbol_table_import_compare_fn, &contains_import));
    ASSERT_TRUE(contains_import);

    ION_ASSERT_OK(_ion_collection_contains(writer_table_import_list, bar_import, &_ion_symbol_table_import_compare_fn, &contains_import));
    ASSERT_TRUE(contains_import);

    ION_STRING_ASSIGN(&import1_import.descriptor.name, &import1_name);
    ION_STRING_ASSIGN(&import2_import.descriptor.name, &import2_name);
    ION_ASSERT_OK(ion_symbol_table_get_max_sid(import1, &import1_import.descriptor.max_id));
    ION_ASSERT_OK(ion_symbol_table_get_max_sid(import2, &import2_import.descriptor.max_id));
    ION_ASSERT_OK(ion_symbol_table_get_version(import1, &import1_import.descriptor.version));
    ION_ASSERT_OK(ion_symbol_table_get_version(import2, &import2_import.descriptor.version));

    ION_ASSERT_OK(_ion_collection_contains(writer_table_import_list, &import1_import, &_ion_symbol_table_import_compare_fn, &contains_import));
    ASSERT_TRUE(contains_import);

    ION_ASSERT_OK(_ion_collection_contains(writer_table_import_list, &import2_import, &_ion_symbol_table_import_compare_fn, &contains_import));
    ASSERT_TRUE(contains_import);

    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_catalog_close(catalog));
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST_P(BinaryAndTextTest, AddImportedTablesFailsBelowTopLevel) {
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING foo;
    ION_SYMBOL_TABLE_IMPORT *foo_import;
    ION_COLLECTION new_imports;

    ion_string_from_cstr("foo", &foo);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    _ion_collection_initialize(writer, &new_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    foo_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports);
    ION_STRING_ASSIGN(&foo_import->descriptor.name, &foo);
    foo_import->descriptor.max_id = 10;
    foo_import->descriptor.version = 2;

    ION_ASSERT_OK(ion_writer_start_container(writer, tid_STRUCT));
    ASSERT_EQ(IERR_INVALID_STATE, ion_writer_add_imported_tables(writer, &new_imports));
    ION_ASSERT_OK(ion_writer_finish_container(writer));

    BYTE *written_bytes = NULL;
    SIZE written_len = 0;

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &written_bytes, &written_len));
    rewrite_and_assert_text_eq(written_bytes, written_len, "{}");
}

TEST_P(BinaryAndTextTest, AddImportedTablesForcesFinishWhenNecessary) {
    // Tests that adding imported tables finishes writers that already have a local symbol table.
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;

    ION_STRING foo;
    ION_SYMBOL_TABLE_IMPORT *foo_import;
    ION_COLLECTION new_imports;
    BYTE *result;
    ION_SYMBOL zoo_symbol;

    ion_string_from_cstr("zoo", &foo);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    _ion_collection_initialize(writer, &new_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    foo_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports);
    ION_STRING_ASSIGN(&foo_import->descriptor.name, &foo);
    foo_import->descriptor.max_id = 10;
    foo_import->descriptor.version = 2;

    ION_STRING_INIT(&zoo_symbol.value); // NULL
    zoo_symbol.sid = UNKNOWN_SID;
    ION_STRING_ASSIGN(&zoo_symbol.import_location.name, &foo);
    zoo_symbol.import_location.location = 1;

    ION_ASSERT_OK(ion_writer_write_symbol(writer, &foo));
    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &new_imports));
    ION_ASSERT_OK(ion_writer_write_ion_symbol(writer, &zoo_symbol));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    if (is_binary) {
        assertBytesEqual("\xE0\x01\x00\xEA\xE9\x81\x83\xD6\x87\xB4\x83zoo\x71\x0A\xEE\x92\x81\x83\xDE\x8E\x86\xBC\xDB\x84\x83zoo\x85\x21\x02\x88\x21\x0A\x71\x0A", 38, result, bytes_flushed);
    }
    else {
        assertStringsEqual("zoo $ion_symbol_table::{imports:[{name:\"zoo\",version:2,max_id:10}]} $10", (char *)result, bytes_flushed);
    }
    free(result);
}

TEST_P(BinaryAndTextTest, AddImportedTablesSkipsDuplicateImports) {
    // Tests that when imports are added that already exist in the writer's context, the context is unchanged and
    // no flushing occurs.
    ION_COLLECTION duplicate_1, duplicate_2;
    ION_SYMBOL_TABLE_IMPORT *import1_import, *import2_import;
    BYTE *result;
    hWRITER writer;
    ION_WRITER_OPTIONS writer_options = {0};
    SIZE bytes_flushed = 0;
    ION_STREAM *stream;
    hCATALOG catalog = NULL;
    const int import_number = 2;
    ION_SYMBOL_TABLE *imports[import_number] = {NULL, NULL};

    ION_STRING import1_name, import2_name;
    ION_ASSERT_OK(ion_string_from_cstr("import1", &import1_name));
    ION_ASSERT_OK(ion_string_from_cstr("import2", &import2_name));

    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));
    ASSERT_NO_FATAL_FAILURE(open_writer_with_imports(is_binary, &writer, &stream, &writer_options, imports, import_number));

    _ion_collection_initialize(writer, &duplicate_1, sizeof(ION_SYMBOL_TABLE_IMPORT));
    import1_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&duplicate_1);
    _ion_collection_initialize(writer, &duplicate_2, sizeof(ION_SYMBOL_TABLE_IMPORT));
    import2_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&duplicate_2);

    ION_STRING_ASSIGN(&import1_import->descriptor.name, &import1_name);
    import1_import->descriptor.version = 1;
    import1_import->descriptor.max_id = 1;

    ION_STRING_ASSIGN(&import2_import->descriptor.name, &import2_name);
    import2_import->descriptor.version = 1;
    import2_import->descriptor.max_id = 2;

    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &duplicate_1));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 10));
    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &duplicate_2));
    ION_ASSERT_OK(ion_test_writer_write_symbol_sid(writer, 11));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    if (is_binary) {
        assertBytesEqual("\xE0\x01\x00\xEA\xEE\xA9\x81\x83\xDE\xA5\x86\xBE\xA2\xDE\x8F\x84\x87import1\x85\x21\x01\x88\x21\x01\xDE\x8F\x84\x87import2\x85\x21\x01\x88\x21\x02\x71\x0A\x71\x0B", 51, result, bytes_flushed);
    }
    else {
        assertStringsEqual("$ion_symbol_table::{imports:[{name:\"import1\",version:1,max_id:1},{name:\"import2\",version:1,max_id:2}]} sym1 sym2", (char *)result, bytes_flushed);
    }

    ION_ASSERT_OK(ion_catalog_close(catalog)); // Closes the catalog and releases its tables.
    ION_ASSERT_OK(ion_writer_options_close_shared_imports(&writer_options));
    free(result);
}

TEST_P(BinaryAndTextTest, AddImportedTablesDoesNotFlushWhenNotNecessaryNoSymbolsBinary) {
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    ION_STRING foo;
    ION_SYMBOL_TABLE_IMPORT *foo_import;
    ION_COLLECTION new_imports;
    BYTE *result;

    ion_string_from_cstr("zoo", &foo);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    _ion_collection_initialize(writer, &new_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    foo_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports);
    ION_STRING_ASSIGN(&foo_import->descriptor.name, &foo);
    foo_import->descriptor.max_id = 10;
    foo_import->descriptor.version = 2;

    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &new_imports));
    ION_ASSERT_OK(ion_writer_write_int(writer, 2));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    if (is_binary) {
        // No symbol table is needed.
        assertBytesEqual("\xE0\x01\x00\xEA\x21\x01\x21\x02", 8, result, bytes_flushed);
    }
    else {
        // A symbol table is needed in text because values aren't buffered; a symbol token that refers to the import
        // could come later.
        assertStringsEqual("1 $ion_symbol_table::{imports:[{name:\"zoo\",version:2,max_id:10}]} 2", (char *)result, bytes_flushed);
    }
    free(result);
}

TEST_P(BinaryAndTextTest, AddImportedTablesDoesNotFlushWhenNotNecessaryNoValues) {
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    ION_STRING foo;
    ION_SYMBOL_TABLE_IMPORT *foo_import;
    ION_COLLECTION new_imports;
    BYTE *result;

    ion_string_from_cstr("zoo", &foo);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    _ion_collection_initialize(writer, &new_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    foo_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports);
    ION_STRING_ASSIGN(&foo_import->descriptor.name, &foo);
    foo_import->descriptor.max_id = 10;
    foo_import->descriptor.version = 2;

    ION_ASSERT_OK(ion_writer_write_int(writer, 1));
    ION_ASSERT_OK(ion_writer_add_imported_tables(writer, &new_imports));

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    // No symbol table is needed in either text or binary because no values follow the context change.
    if (is_binary) {
        assertBytesEqual("\xE0\x01\x00\xEA\x21\x01", 6, result, bytes_flushed);
    }
    else {
        assertStringsEqual("1", (char *)result, bytes_flushed);
    }
    free(result);
}

TEST_P(BinaryAndTextTest, AddImportedTablesFailsWithPendingAnnotations) {
    // Tests that ion_writer_add_imported_tables fails when the writer has pending annotations (because it would be
    // ambiguous which context the annotations belong to).
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING foo;
    ION_SYMBOL_TABLE_IMPORT *foo_import;
    ION_COLLECTION new_imports;

    ion_string_from_cstr("zoo", &foo);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, is_binary));
    _ion_collection_initialize(writer, &new_imports, sizeof(ION_SYMBOL_TABLE_IMPORT));
    foo_import = (ION_SYMBOL_TABLE_IMPORT *)_ion_collection_append(&new_imports);
    ION_STRING_ASSIGN(&foo_import->descriptor.name, &foo);
    foo_import->descriptor.max_id = 10;
    foo_import->descriptor.version = 2;

    ION_ASSERT_OK(ion_writer_add_annotation(writer, &foo));
    ASSERT_EQ(IERR_INVALID_STATE, ion_writer_add_imported_tables(writer, &new_imports));
    ION_ASSERT_OK(ion_writer_close(writer));
    ION_ASSERT_OK(ion_stream_close(stream));
}

TEST_P(BinaryAndTextTest, LSTNullSlotsRoundtrippedAsSymbolZero) {
    // Tests that local symbol tokens with unknown text (due to null slots in the LST) are treated equivalently to
    // symbol zero.
    const char *ion_data = "$ion_symbol_table::{symbols:[null, 123, '''hello''', null.string]} {$10:$11::$12} $13";
    hWRITER writer;
    ION_STREAM *stream;
    SIZE bytes_flushed;
    hREADER reader;
    BYTE *result;
    ION_WRITER_OPTIONS writer_options;
    ION_READER_OPTIONS reader_options;

    ion_event_initialize_reader_options(&reader_options);
    ion_event_initialize_writer_options(&writer_options);

    ION_ASSERT_OK(ion_reader_open_buffer(&reader, (BYTE *)ion_data, strlen(ion_data), &reader_options));
    ION_ASSERT_OK(ion_stream_open_memory_only(&stream));
    writer_options.output_as_binary = is_binary;
    ION_ASSERT_OK(ion_writer_open(&writer, stream, &writer_options));
    ION_ASSERT_OK(ion_writer_write_all_values(writer, reader));
    ION_ASSERT_OK(ion_reader_close(reader));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    if (is_binary) {
        assertBytesEqual("\xE0\x01\x00\xEA\xEB\x81\x83\xD8\x87\xB6\x85hello\xD6\x80\xE4\x81\x80\x71\x0A\x70", 24, result, bytes_flushed);
    }
    else {
        assertStringsEqual("{$0:$0::hello} $0", (char *)result, bytes_flushed);
    }
    free(result);
}

TEST_P(BinaryAndTextTest, ReaderSkipsOverSymbol) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, def_read;
    BYTE *data;
    SIZE data_length;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, 123));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &abc_written));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &def_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));
    free(data);
}

TEST_P(BinaryAndTextTest, ReaderSkipsOverIVMBoundary) {
    hWRITER writer = NULL;
    hREADER reader = NULL;
    ION_TYPE type;
    ION_STREAM *ion_stream = NULL;
    ION_STRING abc_written, def_written, def_read;
    BYTE *data;
    SIZE data_length;

    ion_string_from_cstr("abc", &abc_written);
    ion_string_from_cstr("def", &def_written);
    ION_ASSERT_OK(ion_test_new_writer(&writer, &ion_stream, is_binary));
    ION_ASSERT_OK(ion_writer_write_int32(writer, 123));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &abc_written));
    // Forces an IVM and symbol table boundary.
    ION_ASSERT_OK(ion_writer_finish(writer, NULL));
    ION_ASSERT_OK(ion_writer_write_symbol(writer, &def_written));
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, ion_stream, &data, &data_length));

    ION_ASSERT_OK(ion_test_new_reader(data, data_length, &reader));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    // This crosses over the boundary.
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_SYMBOL, type);
    ION_ASSERT_OK(ion_reader_read_string(reader, &def_read));
    assertStringsEqual((char *)def_written.value, (char *)def_read.value, def_written.length);
    ION_ASSERT_OK(ion_reader_next(reader, &type));
    ASSERT_EQ(tid_EOF, type);
    ION_ASSERT_OK(ion_reader_close(reader));
    free(data);
}

TEST(IonSymbolTable, CanBeRemovedFromCatalog) {
    hCATALOG catalog = NULL;
    ION_SYMBOL_TABLE *imports[2];
    int32_t cnt;


    ASSERT_NO_FATAL_FAILURE(populate_catalog(&catalog, imports));

    ION_ASSERT_OK(ion_catalog_get_symbol_table_count(catalog, &cnt));
    ASSERT_EQ(2, cnt);
    ION_ASSERT_OK(ion_catalog_release_symbol_table(catalog, imports[0]));
    ION_ASSERT_OK(ion_catalog_get_symbol_table_count(catalog, &cnt));
    ASSERT_EQ(1, cnt);
    ION_ASSERT_OK(ion_catalog_release_symbol_table(catalog, imports[0]));
    ION_ASSERT_OK(ion_catalog_get_symbol_table_count(catalog, &cnt));
    ASSERT_EQ(1, cnt);
    ION_ASSERT_OK(ion_catalog_release_symbol_table(catalog, imports[1]));
    ION_ASSERT_OK(ion_catalog_get_symbol_table_count(catalog, &cnt));
    ASSERT_EQ(0, cnt);
    ION_ASSERT_OK(ion_catalog_release_symbol_table(catalog, imports[1]));
    ION_ASSERT_OK(ion_catalog_get_symbol_table_count(catalog, &cnt));
    ASSERT_EQ(0, cnt);

    ION_ASSERT_OK(ion_catalog_close(catalog));
}
