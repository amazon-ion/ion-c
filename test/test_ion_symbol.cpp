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

TEST(IonSymbolTable, WriterAppendsLocalSymbolsOnFlush) {
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING sym1, sym2, sym3, sym4;
    SIZE bytes_flushed;

    hREADER reader;
    BYTE *result;

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

    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    ion_test_print_bytes(result, bytes_flushed);

    ION_ASSERT_OK(ion_test_new_reader(result, bytes_flushed, &reader));
    ION_ASSERT_OK(ion_test_new_writer(&writer, &stream, FALSE));
    ION_ASSERT_OK(ion_writer_write_all_values(writer, reader));
    ION_ASSERT_OK(ion_reader_close(reader));

    free(result);
    ION_ASSERT_OK(ion_test_writer_get_bytes(writer, stream, &result, &bytes_flushed));

    assertStringsEqual("sym1\nsym2\nsym3\nsym1\nsym3\nsym4\nsym4", (char *)result, bytes_flushed);
    //std::cout << std::string((char *)result, (unsigned long)bytes_flushed) << std::endl;
    free(result);

}

TEST(IonSymbolTable, WriterAppendsLocalSymbolsWithImportsOnFlush) {
    // Add imports to the initial table, then append
    hWRITER writer;
    ION_STREAM *stream;
    ION_STRING sym1, sym2, sym3, sym4;
    SIZE bytes_flushed;
    ION_WRITER_OPTIONS writer_options;
    memset(&writer_options, 0, sizeof(ION_WRITER_OPTIONS));
    writer_options.encoding_psymbol_table;

    // TODO the current writer_options.encoding_psymbol_table needs to be an ION_COLLECTION of shared symbol tables,
    // which should be present in writer_options.pcatalog (if the user wants the symbols to be written out with known
    // text).

}

TEST(IonSymbolTable, AppendingAfterIVMDoesNothing) {
    // LST-append syntax when the current symbol table is the system symbol table does nothing; the symbols field is
    // treated as if it doesn't exist.
}

TEST(IonSymbolTable, AppendingNoSymbolsDoesNotWriteSymbolTable) {
    // If an LST-append LST declares no symbols field or an empty symbols list, there is no need to write that LST.
}

TEST(IonSymbolTable, ManuallyWritingSymbolTableStructIsRecognizedAsSymbolTable) {
    // If the user manually writes a struct that is a local symbol table, it should become the active LST, and it
    // should be possible for the user to subsequently write any SID within the new table's max_id.
    // It should also be possible for the user to subsequently write additional symbols within the same context and
    // have them added to the symbol table. This means that instead of actually writing out the manually-written
    // symbol table up front, it must be intercepted, turned into a mutable ION_SYMBOL_TABLE, and written out at
    // flush as usual. Before the new context is started, the system should flush the previous one.
    hWRITER writer;
    ION_STREAM *stream;

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

    ION_ASSERT_OK(ion_writer_write_symbol_sid(writer, 10)); // This maps to sym1.
    // TODO implement this functionality
}

TEST(IonSymbolTable, ManuallyWriteSymbolTableAppendSucceeds) {
    // Like the previous test, but the manually written symbol table contains appended symbols.
}

TEST(IonSymbolTable, TextWritingKnownSymbolFromSIDResolvesText) {
    // If the user writes a SID in the import range and that import is found in the catalog, that SID should be resolved
    // to its text representation. There is no need to include the local symbol table in the stream.
}

TEST(IonSymbolTable, TextWritingSymbolFromNotFoundImportWritesIdentifierAndForcesSymbolTable) {
    // If the user writes a SID in the import range and that import is not found in the catalog, the SID must be
    // written as $<int> and the symbol table must be included in the stream. Future consumers may have access to
    // that import and be able to resolve the identifier.
}

TEST(IonSymbolTable, WritingOutOfRangeSIDFails) {
    // For both text and binary, manually writing a SID (from a pure SID or ION_SYMBOL with NULL text) that is out of
    // range of the current symbol table context should raise an error, since this condition must also raise an error
    // on read.
}
