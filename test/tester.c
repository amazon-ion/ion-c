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
// initial testing program
//

#define TESTER_INIT
#include "tester.h"

#include "ion_binary_test.h"
#include "ion_test_utils.h"

BOOL  g_no_print             = TRUE;
long  g_value_count          = 0;
long  g_failure_count        = 0;
long  g_total_failure_count  = 0;
char *g_iontests_path        = ".";
int   g_repeat_count         = 1; 

void report_test(char* title, char *filename)
{
    printf("\n%s over %s\n  Value COUNT: %ld\n  FAILURES: %ld\n", title, filename, g_value_count, g_failure_count);
    g_total_failure_count += g_failure_count;
    g_failure_count = 0;
}

#define RUNTEST(f,p)          \
do {                          \
    title = #f;               \
    IONDEBUG(f (), title);    \
    report_test(title,p);     \
} while(0)


/**
 * This test program tests every file in iontestdata/good, bad and equivs directory.
 * First arg is the parent path of the IonTests/iontestdata directory.
 * E.g. "./tester ~/IonTests/src"
 *
 * One way to use this program to test a single file is to create the same directory structure, but
 * only leave the desired test file in it.
 *
 * To get the testing progress, change g_no_print in this file to FALSE.
 * To get detailed logging from IonC library, change _ion_parser_debug to 1 in ion_parser.h
 *
 */
int main(int argc, char **argv)
{
    iENTER;
    int32_t  ii;
    char    *title;

    printf("===============\n%smain started\n\n", __FILE__);


    // initialize a decimal context
    decContextDefault(&deccontext, DEC_INIT_DECQUAD);

    if (argc > 1) {
        g_iontests_path = argv[1];
        if (g_no_print == FALSE) printf("TEST_FILES: %s\n", g_iontests_path);
        RUNTEST(ion_binary_test, NULL);
        RUNTEST(test_step_out_nested_s_expressions, NULL);
        RUNTEST(test_reader_good_files, g_iontests_path);
        RUNTEST(test_reader_bad_files, g_iontests_path);
        RUNTEST(test_bulk_files, g_iontests_path);
        RUNTEST(test_text_reader_1, g_iontests_path);
    }
    else {
        printf("Please provide the parent path of the IonTests/iontestdata directory. E.g. ./tester ~/IonTests/src\n");
        g_total_failure_count = 1;
    }

    printf("===============\ndone\n");

    // Fail the build if any file failed.
    if (g_total_failure_count != 0) {
        printf("\nTOTAL FAILURES: %d\n", g_total_failure_count);
        err = -1;
    }

    goto return_error;

fail:
return_error:
	ion_symbol_table_free_system_table();

    return err;
}

iERR test_reader_one_file(char *filename)
{
    iENTER;
    IONDEBUG(test_one_file(filename, test_good_file, "test_reader_one_file"), "test_one_file");
    iRETURN;
}

iERR test_reader_one_file_file_only(char *filename)
{
    iENTER;
    err = test_one_file(filename, test_reader_read_all_just_file_byte_by_byte, "test_reader_one_file_file_only");
    IONDEBUG(err, "test_one_file");
    iRETURN;
}


iERR test_text_reader_good_files()
{
    iENTER;
    IONDEBUG(test_good_files(FILETYPE_TEXT), "test_file_list(FILETYPE_TEXT)");
    iRETURN;
}

iERR test_binary_reader_good_files()
{
    iENTER;
    IONDEBUG(test_good_files(FILETYPE_BINARY), "test_file_list(FILETYPE_BINARY)");
    iRETURN;
}

iERR test_reader_good_files()
{
    iENTER;
    IONDEBUG(test_good_files(FILETYPE_ALL), "test_file_list(FILETYPE_ALL)");
    iRETURN;
}

iERR test_reader_bad_files()
{
    iENTER;
    IONDEBUG(test_bad_files(), "test_bad_files()");
    iRETURN;
}

iERR test_text_reader_1()
{
    iENTER;
    BYTE buffer[BUF_SIZE+1];
    char *ion_image = "$ion_1_0 1 2 3";
    int   ion_image_len = strlen(ion_image);

    hREADER hreader;
    ION_READER_OPTIONS options;

    assert(BUF_SIZE >= ion_image_len);

    memset(&buffer[0], 0, BUF_SIZE+1);
    memset(&options, 0, sizeof(options));

    options.return_system_values = TRUE;

    memcpy((char *)(&buffer[0]), ion_image, ion_image_len + 1);

    IONDEBUG(ion_reader_open_buffer(&hreader, buffer, BUF_SIZE, &options), "opening buffer");

    IONDEBUG(test_reader_read_all(hreader), "read all input");

    if (hreader != NULL) {
        ion_reader_close(hreader);
        hreader = NULL;
    }
    iRETURN;
}

iERR test_step_out_nested_s_expressions()
{
    iENTER;

    char* ion_text = "((first)(second))((third)(fourth))";

    hREADER reader;
    IONDEBUG(test_open_string_reader(ion_text, &reader), ion_text);

    ION_TYPE type;
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SEXP, type));
    IONCHECK(ion_reader_step_in(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SEXP, type));
    IONCHECK(ion_reader_step_out(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SEXP, type));
    IONCHECK(ion_reader_step_in(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SEXP, type));
    IONCHECK(ion_reader_step_in(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SYMBOL, type));
    IONCHECK(expect_string("third", reader));
    IONCHECK(ion_reader_step_out(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SEXP, type));
    IONCHECK(ion_reader_step_in(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_SYMBOL, type));
    IONCHECK(expect_string("fourth", reader));
    IONCHECK(ion_reader_step_out(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(ion_reader_step_out(reader));
    IONCHECK(ion_reader_next(reader, &type));
    IONCHECK(expect_type(tid_EOF, type));

    if (reader != NULL) {
        ion_reader_close(reader);
    }

    iRETURN;
}

iERR test_open_string_reader(char* ion_text, hREADER* reader)
{
    iENTER;

    size_t buffer_length = strlen(ion_text);
    BYTE* buffer = calloc(buffer_length, sizeof(BYTE));
    memcpy((char *)(&buffer[0]), ion_text, buffer_length);

    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.return_system_values = TRUE;

    IONDEBUG(ion_reader_open_buffer(reader, buffer, buffer_length, &options), "opening buffer");

    iRETURN;
}

#define TEMP_BUF_SIZE 0x10000
iERR test_reader_read_value_print(hREADER hreader, ION_TYPE t, char *type_name)
{
    iENTER;

    ION_TYPE    ion_type;
    BOOL        is_null;
    BOOL        bool_value;
    ION_INT     ion_int_value;
    double      double_value;
    decQuad     decimal_value;
    ION_TIMESTAMP timestamp_value;
    SID         sid;
    ION_STRING  string_value, *indirect_string_value = NULL;
    SIZE        length;
    BYTE        buf[TEMP_BUF_SIZE];
    char        decimal_image_buffer[DECQUAD_String];
    char        time_buffer[ION_TIMESTAMP_STRING_LENGTH + 1];
    char       *sym_name;
    hSYMTAB     hsymtab = 0;

    printf("read type %s:: ", type_name);

    IONCHECK(ion_reader_is_null(hreader, &is_null));
    if (is_null) {
        t = tid_NULL;
    }

    switch (ION_TYPE_INT(t)) {
    case tid_EOF_INT:
        // do nothing
        break;
    case tid_NULL_INT:
        IONCHECK(ion_reader_read_null(hreader, &ion_type));
        printf("value: null typed as %d", (intptr_t)ion_type);
        break;
    case tid_BOOL_INT:
        IONCHECK(ion_reader_read_bool(hreader, &bool_value));
        printf("value: %d (%s)", bool_value, bool_value ? "true" : "false");
        break;
    case tid_INT_INT:
        //IONCHECK(ion_reader_read_int(hreader, &xx));
        //IONCHECK(ion_reader_read_int64(hreader, &long_value));
        IONCHECK(ion_int_init(&ion_int_value, hreader));
        IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
        ION_STRING_INIT(&string_value);
        IONCHECK(ion_int_to_string(&ion_int_value, hreader, &string_value));
        printf("value: %s", string_value.value);
        break;
    case tid_FLOAT_INT:
        IONCHECK(ion_reader_read_double(hreader, &double_value));
        printf("value: %f", double_value);
        break;
    case tid_DECIMAL_INT:
        IONCHECK(ion_reader_read_decimal(hreader, &decimal_value));
        decQuadToString(&decimal_value, decimal_image_buffer);
//        IONCHECK(ion_reader_read_double(hreader, &double_value));
//        printf("value: %f, '%s'", double_value, decimal_image_buffer);
        printf("value: '%s'", decimal_image_buffer);
        break;
    case tid_TIMESTAMP_INT:
        IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
        IONCHECK( ion_timestamp_to_string(&timestamp_value, time_buffer, sizeof(time_buffer), &length, &deccontext));
        assert( length > 1 && length < sizeof(time_buffer) && time_buffer[length] == '\0' );
        printf("value: %s", time_buffer);
        break;
    case tid_STRING_INT:
        IONCHECK(ion_reader_read_string(hreader, &string_value));
        printf("value: \"%s\"", test_make_cstr(&string_value));
        break;
    case tid_SYMBOL_INT:
        IONCHECK(ion_reader_read_symbol_sid(hreader, &sid));
        // you can only read a value once! IONCHECK(ion_reader_read_string(hreader, &string_value));
        // so we look it up
        IONCHECK(ion_reader_get_symbol_table(hreader, &hsymtab));
        IONCHECK(ion_symbol_table_find_by_sid(hsymtab, sid, &indirect_string_value));
        if (!indirect_string_value) {
            sym_name = "<not found>";
        }
        else {
            sym_name = test_make_cstr(indirect_string_value);
        }
        printf("value: %d \"%s\"", sid, sym_name);
        break;
    case tid_CLOB_INT:
    case tid_BLOB_INT:
        IONCHECK(ion_reader_get_lob_size(hreader, &length));
        printf("value length: %d", length);
        IONCHECK(ion_reader_read_lob_bytes(hreader, buf, TEMP_BUF_SIZE, &length));
        //IONCHECK(ion_reader_read_chunk(hreader, &xx));
        break;
    case tid_STRUCT_INT:
    case tid_LIST_INT:
    case tid_SEXP_INT:
        //printf("--- support for collections is not ready");
        printf("value - step into %s\n", type_name);
        IONCHECK(ion_reader_step_in(hreader));
        IONCHECK(test_reader_read_all(hreader));
        IONCHECK(ion_reader_step_out(hreader));
        printf("value - now out of %s\n", type_name);
        break;

    case tid_DATAGRAM_INT:
    default:
        printf(" - can't read value on an illegal type\n");
        break;
    }

    printf("\n");

    iRETURN;
}


iERR test_reader_read_value_no_print(hREADER hreader, ION_TYPE t)
{
    iENTER;

    ION_TYPE    ion_type;
    BOOL        is_null;
    BOOL        bool_value;
    ION_INT     ion_int_value;
    double      double_value;
    decQuad     decimal_value;
    ION_TIMESTAMP timestamp_value;
    SID         sid;
    ION_STRING  string_value, *indirect_string_value = NULL;
    SIZE        length, remaining;
    BYTE        buf[TEMP_BUF_SIZE];
    hSYMTAB     hsymtab = 0;

    IONCHECK(ion_reader_is_null(hreader, &is_null));
    if (is_null) {
        t = tid_NULL;
    }

    switch (ION_TYPE_INT(t)) {
    case tid_EOF_INT:
        // do nothing
        break;
    case tid_NULL_INT:
        IONCHECK(ion_reader_read_null(hreader, &ion_type));
        break;
    case tid_BOOL_INT:
        IONCHECK(ion_reader_read_bool(hreader, &bool_value));
        break;
    case tid_INT_INT:
        IONCHECK(ion_int_init(&ion_int_value, hreader));
        IONCHECK(ion_reader_read_ion_int(hreader, &ion_int_value));
        // IONCHECK(ion_reader_read_int64(hreader, &long_value));
        break;
    case tid_FLOAT_INT:
        IONCHECK(ion_reader_read_double(hreader, &double_value));
        break;
    case tid_DECIMAL_INT:
        IONCHECK(ion_reader_read_decimal(hreader, &decimal_value));
        break;
    case tid_TIMESTAMP_INT:
        IONCHECK(ion_reader_read_timestamp(hreader, &timestamp_value));
        break;
    case tid_STRING_INT:
        IONCHECK(ion_reader_read_string(hreader, &string_value));
        break;
    case tid_SYMBOL_INT:
        IONCHECK(ion_reader_read_symbol_sid(hreader, &sid));
        // you can only read a value once! IONCHECK(ion_reader_read_string(hreader, &string_value));
        // so we look it up
        IONCHECK(ion_reader_get_symbol_table(hreader, &hsymtab));
        IONCHECK(ion_symbol_table_find_by_sid(hsymtab, sid, &indirect_string_value));
        break;
    case tid_CLOB_INT:
    case tid_BLOB_INT:
        IONCHECK(ion_reader_get_lob_size(hreader, &length));
        // just to cover both API's
        if (length < TEMP_BUF_SIZE) {
            IONCHECK(ion_reader_read_lob_bytes(hreader, buf, TEMP_BUF_SIZE, &length));
        }
        else {
            for (remaining = length; remaining > 0; remaining -= length) {
                IONCHECK(ion_reader_read_lob_bytes(hreader, buf, TEMP_BUF_SIZE, &length));
                // IONCHECK(ion_reader_read_chunk(hreader, buf, TEMP_BUF_SIZE, &length));
            }
        }
        break;
    case tid_STRUCT_INT:
    case tid_LIST_INT:
    case tid_SEXP_INT:
        IONCHECK(ion_reader_step_in(hreader));
        IONCHECK(test_reader_read_all(hreader));
        IONCHECK(ion_reader_step_out(hreader));
        break;

    case tid_DATAGRAM_INT:
    default:
        break;
    }

    iRETURN;
}

iERR test_writer_1(void)
{
    iENTER;

    BYTE buffer[BUF_SIZE+1];
    BYTE bytes[100];
    hWRITER hwriter;
    ION_WRITER_OPTIONS options;
    decQuad    dec1;
    ION_TIMESTAMP timestamp;
    ION_STRING    str;
    char *image;
    SIZE  used, len;


    memset(&buffer[0], 0, BUF_SIZE+1);
    memset(&options, 0, sizeof(options));

    IONDEBUG(ion_writer_open_buffer(&hwriter, buffer, BUF_SIZE, &options), "opening writer");

    IONDEBUG(ion_writer_write_null(hwriter), "writing simple null");

    // integer values
    IONDEBUG(ion_writer_write_int(hwriter, 10), "writing simple int");

    // double value

    IONDEBUG(ion_writer_write_double(hwriter, 123.5), "writing double");

    IONDEBUG(ion_writer_write_double(hwriter, -124.5), "writing double");

    IONDEBUG(ion_writer_write_double(hwriter, -123456789e20), "writing double");

    IONDEBUG(ion_writer_write_double(hwriter, -123456789e-20), "writing double");

    // decimal values
    IONDEBUG(ion_writer_write_decimal(hwriter, NULL), "writing decimal");

    decQuadFromUInt32(&dec1, 1234567890);
    IONDEBUG(ion_writer_write_decimal(hwriter, &dec1), "writing decimal");

    decQuadFromString(&dec1, "-0.0", &deccontext);
    IONDEBUG(ion_writer_write_decimal(hwriter, &dec1), "writing decimal");

    decQuadFromString(&dec1, "-123456789.987654321e12", &deccontext);
    IONDEBUG(ion_writer_write_decimal(hwriter, &dec1), "writing decimal");

    // try a few timestamp values
    image = "0001-01-01";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    image = "9999-12-31T23:59:59.1234567890123456789Z";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    image = "2008-07-24T12:14:15.6789+08:00";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    image = "2008-07-24T13:14:15.6789-12:00";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    // a couple of symbols
    str.value = (BYTE *)"test_symbol";
    str.length = (SIZE)strlen((char*)str.value);
    err = ion_writer_write_symbol(hwriter, &str);
    IONDEBUG2(err, "writing symbol value: ", image);

    str.value = (BYTE *)"*%test_symbol";
    str.length = (SIZE)strlen((char*)str.value);
    err = ion_writer_write_symbol(hwriter, &str);
    IONDEBUG2(err, "writing symbol value: ", image);

    // a couple of strings
    str.value = (BYTE *)"test string";
    str.length = (SIZE)strlen((char*)str.value);
    err = ion_writer_write_string(hwriter, &str);
    IONDEBUG2(err, "writing string value: ", image);

    str.value = (BYTE *)"";
    str.length = (SIZE)strlen((char*)str.value);
    err = ion_writer_write_string(hwriter, &str);
    IONDEBUG2(err, "writing string value: ", image);

    str.value = (BYTE *)"line1\nline2";
    str.length = (SIZE)strlen((char*)str.value);
    err = ion_writer_write_string(hwriter, &str);
    IONDEBUG2(err, "writing string value: ", image);

    //clob
    image = "test clob \0 \n more stuff";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    err = ion_writer_write_clob(hwriter, bytes, len);
    IONDEBUG2(err, "writing clob value: ", image);

    //blob
    // The Input: leasure.   Encodes to bGVhc3VyZS4=
    image = "leasure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    err = ion_writer_write_blob(hwriter, bytes, len);
    IONDEBUG2(err, "writing blob value: ", image);

    // The Input: easure.    Encodes to ZWFzdXJlLg==
    image = "easure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    err = ion_writer_write_blob(hwriter, bytes, len);
    IONDEBUG2(err, "writing blob value: ", image);

    // The Input: asure.     Encodes to YXN1cmUu
    image = "asure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    err = ion_writer_write_blob(hwriter, bytes, len);
    IONDEBUG2(err, "writing blob value: ", image);

    // The Input: sure.      Encodes to c3VyZS4=
    image = "sure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    err = ion_writer_write_blob(hwriter, bytes, len);
    IONDEBUG2(err, "writing blob value: ", image);

    //struct
    IONDEBUG(ion_writer_start_container(hwriter, tid_STRUCT), "writing ion_writer_start_struct");

    IONDEBUG(ion_writer_finish_container(hwriter), "writing ion_writer_end_struct");

    //list
    IONDEBUG(ion_writer_start_container(hwriter, tid_LIST), "starting a LIST container");

    IONDEBUG(ion_writer_finish_container(hwriter), "ion_writer_finish_container for lisT");

    //sexp
    IONDEBUG(ion_writer_start_container(hwriter, tid_SEXP), "writing ion_writer_start_sexp");

    IONDEBUG(ion_writer_finish_container(hwriter), "ion_writer_finish_container for sexp");

    // reader - real soon now

    IONDEBUG(ion_writer_close(hwriter), "closing writer");

    if (g_no_print == FALSE) {
        printf("[---\n");
        printf("%s\n", buffer);
        printf("---]\n");
    }

    iRETURN;
}


iERR test_writer_2(void)
{
    iENTER;

    BYTE                buffer[BUF_SIZE+1];
    BYTE                bytes[100];
    hWRITER             hwriter;
    ION_WRITER_OPTIONS  options;
    decQuad             dec1;
    double              d;
    ION_TIMESTAMP       timestamp;
    ION_STRING          str;
    char               *image;
    SIZE                used, len, bytes_flushed;


    memset(&buffer[0], 0, BUF_SIZE+1);
    memset(&options, 0, sizeof(options));

    options.output_as_binary = TRUE;

    IONDEBUG(ion_writer_open_buffer(&hwriter, buffer, BUF_SIZE, &options), "opening writer");

    IONDEBUG(ion_writer_write_null(hwriter), "writing simple null");

    // integer values
    IONDEBUG(ion_writer_write_int(hwriter, 10), "writing simple int");
    IONDEBUG(ion_writer_write_int(hwriter, -10), "writing simple int");
    IONDEBUG(ion_writer_write_int(hwriter, 0x81), "writing simple int");
    IONDEBUG(ion_writer_write_int(hwriter, 1000003), "writing simple int");

    // double value
    IONDEBUG(ion_writer_write_double(hwriter, 1.0), "writing double");
    IONDEBUG(ion_writer_write_double(hwriter, -1.0), "writing double");

    // hex should be: 0x7e 45 79 8e e2 30 8c 26
    d = 1.79769313486231e300;
    IONDEBUG2(ion_writer_write_double(hwriter, d), "writing double: ", double_to_cstr(d));

    d = 123.5;
    IONDEBUG2(ion_writer_write_double(hwriter, d), "writing double: ", double_to_cstr(d));

    d = -124.5;
    IONDEBUG2(ion_writer_write_double(hwriter, d), "writing double: ", double_to_cstr(d));

    d = -123456789e20;
    IONDEBUG2(ion_writer_write_double(hwriter, d), "writing double: ", double_to_cstr(d));

    d = -123456789e-20;
    IONDEBUG2(ion_writer_write_double(hwriter, d), "writing double: ", double_to_cstr(d));

    // decimal values
    IONDEBUG(ion_writer_write_decimal(hwriter, NULL), "writing decimal");

    decQuadFromUInt32(&dec1, 1234567890);
    IONDEBUG(ion_writer_write_decimal(hwriter, &dec1), "writing decimal from int 1234567890");

    image = "-0.0";
    decQuadFromString(&dec1, image, &deccontext);
    IONDEBUG2(ion_writer_write_decimal(hwriter, &dec1), "writing decimal from string: ", image);

    image = "-123456789.987654321e12";
    decQuadFromString(&dec1, image, &deccontext);
    IONDEBUG2(ion_writer_write_decimal(hwriter, &dec1), "writing decimal from string: ", image);

    // try a few timestamp values
    image = "0001-01-01";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    image = "9999-12-31T23:59:59.1234567890123456789Z";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value:", image);
    assert(used == len);

    image = "2008-07-24T12:14:15.6789+08:00";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    image = "2008-07-24T13:14:15.6789-12:00";
    len = (SIZE)strlen(image);
    err = ion_timestamp_parse(&timestamp, image, len, &used, &deccontext);
    if (!err) err = ion_writer_write_timestamp(hwriter, &timestamp);
    IONDEBUG2(err, "writing timestamp value: ", image);
    assert(used == len);

    // a couple of symbols
    str.value = (BYTE *)"test_symbol";
    str.length = (SIZE)strlen((char*)str.value);
    IONDEBUG2(ion_writer_write_symbol(hwriter, &str), "writing symbol value : ", str.value);

    str.value = (BYTE *)"*%test_symbol";
    str.length = (SIZE)strlen((char*)str.value);
    IONDEBUG2(ion_writer_write_symbol(hwriter, &str), "writing symbol value : ", str.value);

    // a couple of strings
    str.value = (BYTE *)"test string";
    str.length = (SIZE)strlen((char*)str.value);
    IONDEBUG2(ion_writer_write_symbol(hwriter, &str), "writing string value : ", str.value);

    str.value = (BYTE *)"";
    str.length = (SIZE)strlen((char*)str.value);
    IONDEBUG2(ion_writer_write_symbol(hwriter, &str), "writing string value : ", str.value);

    str.value = (BYTE *)"line1\nline2";
    str.length = (SIZE)strlen((char*)str.value);
    IONDEBUG2(ion_writer_write_symbol(hwriter, &str), "writing string value : ", str.value);

    //clob
    image = "test clob \0 \n more stuff";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    IONDEBUG2(ion_writer_write_symbol(hwriter, &str), "writing clob value : ", image);

    //blob
    // The Input: leasure.   Encodes to bGVhc3VyZS4=
    image = "leasure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    IONDEBUG2(ion_writer_write_blob(hwriter, bytes, len), "writing blob value: ", image);

    // The Input: easure.    Encodes to ZWFzdXJlLg==
    image = "easure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    IONDEBUG2(ion_writer_write_blob(hwriter, bytes, len), "writing blob value: ", image);

    // The Input: asure.     Encodes to YXN1cmUu
    image = "asure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    IONDEBUG2(ion_writer_write_blob(hwriter, bytes, len), "writing blob value: ", image);

    // The Input: sure.      Encodes to c3VyZS4=
    image = "sure.";
    len = copy_to_bytes(image, bytes, sizeof(bytes));
    IONDEBUG2(ion_writer_write_blob(hwriter, bytes, len), "writing blob value: ", image);

    //struct
    IONDEBUG(ion_writer_start_container(hwriter, tid_STRUCT), "ion_writer_start_container(hwriter, tid_STRUCT)");

    IONDEBUG(ion_writer_finish_container(hwriter), "ion_writer_finish_container(hwriter) for struct");

    //list
    IONDEBUG(ion_writer_start_container(hwriter, tid_LIST), "ion_writer_start_container(hwriter, tid_LIST)");

    IONDEBUG(ion_writer_finish_container(hwriter), "ion_writer_finish_container(hwriter) for list");

    //sexp
    IONDEBUG(ion_writer_start_container(hwriter, tid_SEXP), "ion_writer_start_container(hwriter, tid_SEXP)");

    IONDEBUG(ion_writer_finish_container(hwriter), "ion_writer_finish_container(hwriter) for sexp");

    IONDEBUG(ion_writer_flush(hwriter, &bytes_flushed), "ion_writer_flush(hwriter)");

    IONDEBUG(ion_writer_close(hwriter), "closing writer");

    if (g_no_print == FALSE) {
        printf("[---\n");
        ion_dump_binary(buffer, bytes_flushed);
        printf("---]\n");
    }

    iRETURN;
}

char temp_buffer[MAX_TEMP_STRING];

char *test_make_cstr(iSTRING str)
{
    assert(str && str->value);

    int len;
    if ( str->length < MAX_TEMP_STRING ) {
        len = str->length;
    }
    else {
        len = MAX_TEMP_STRING - 1;
    }

    // needs to have UTF-8 support here
    memcpy(temp_buffer, str->value, len);

    temp_buffer[len] = '\0';

    return temp_buffer;
}

char temp_path_buffer[MAX_TEMP_STRING];

char *test_make_fullpathname(char *localpathname)
{
    return test_concat_filename(temp_path_buffer, MAX_TEMP_STRING, g_iontests_path, localpathname);
}

char *test_concat_filename(char *dst, int dstlen, char *path, char *name)
{
    int  pathlen, namelen;
    BOOL path_needs_separator, file_needs_separator;

    assert(dst);
    assert(path);
    assert(name);
    assert(dstlen > 0);

    pathlen = strnlen(path, dstlen);
    namelen = strnlen(name, dstlen);
    assert(pathlen + namelen + 1 < dstlen); // extra +1 in case we have to append a trailing slash on the root path

    path_needs_separator = (pathlen > 0) ? (path[pathlen - 1] != PATH_SEPARATOR_CHAR) : FALSE; // it there's no path we don't a separator
    file_needs_separator = (namelen > 0) ? (name[0] != PATH_SEPARATOR_CHAR) : FALSE; // no name? no need to separate

    if (pathlen) {
        memcpy(dst, path, pathlen);
    }

    if (path_needs_separator && file_needs_separator) {
        dst[pathlen] = PATH_SEPARATOR_CHAR;
        pathlen++;
    }

    if (namelen) {
        memcpy(dst + pathlen, name, namelen);
    }

    dst[pathlen + namelen] ='\0';

    return dst;
}


char temp_double_image[100];
char *double_to_cstr(double d) {
    sprintf(temp_double_image, "%lf", d);
    return temp_double_image;
}


void ion_dump_binary(BYTE *buffer, SIZE buffer_length)
{
    int  len, pos = 0;

    for (pos = 0; pos < buffer_length; pos += ION_BINARY_DUMP_LINE_LENGTH) {
        len = ION_BINARY_DUMP_LINE_LENGTH;
        if (len > buffer_length - pos) len = buffer_length - pos;
        ion_dump_binary_line1(pos, buffer + pos, len);
        ion_dump_binary_line2(pos, buffer + pos, len);
        ion_dump_binary_line3(pos, buffer + pos, len);
    }

    printf("\n");
}

void ion_dump_binary_line1(int offset, BYTE *buffer, SIZE line_length)
{
    int ii;

    printf("%6d: ", offset);
    for (ii=0; ii<line_length; ii++) {
        printf("%02x ", *(buffer+ii));
    }
    printf("\n");
}

void ion_dump_binary_line2(int offset, BYTE *buffer, SIZE line_length)
{
    int ii, c;

    offset;

    printf("        ");
    for (ii=0; ii<line_length; ii++) {
        c = *(buffer+ii);
        if (c < 32 || c > 126) {
            printf("   ");
        }
        else {
            printf("%2c ", c);
        }
    }
    printf("\n");
}

void ion_dump_binary_line3(int offset, BYTE *buffer, SIZE line_length)
{
    buffer;
    line_length;

    printf("\n");

#if ION_BINARY_DUMP_LINES_PER_BLOCK > 1
    if ((offset % ION_BINARY_DUMP_LINES_PER_BLOCK) == 0) {
        printf("\n");
    }
#else
    offset;
#endif

}

iERR expect_type(ION_TYPE actual, ION_TYPE expected)
{
    iENTER;

    if (actual != expected) {
        char buffer[ION_ERROR_MESSAGE_MAX_LENGTH];
        snprintf(buffer, ION_ERROR_MESSAGE_MAX_LENGTH,
            "Expected %s but was %s",
            ion_test_get_type_name(expected),
            ion_test_get_type_name(actual));
        FAILWITHMSG(IERR_INVALID_ARG, buffer);
    }

    iRETURN;
}

iERR expect_string(char* expected, hREADER reader)
{
    iENTER;

    ION_STRING actual;
    IONCHECK(ion_reader_read_string(reader, &actual))
    char* actual_cstring = test_make_cstr(&actual);
    if (strcmp(expected, actual_cstring)) {
        char buffer[ION_ERROR_MESSAGE_MAX_LENGTH];
        snprintf(buffer, ION_ERROR_MESSAGE_MAX_LENGTH,
            "Expected \"%s\" but was \"%s\"",
            expected,
            actual_cstring);
        free(actual_cstring);
        FAILWITHMSG(IERR_INVALID_ARG, buffer);
    }

    iRETURN;
}
