#include "ion_cookbook_test.h"

#include "ion_assert.h"
#include <ion_reader.h>
#include <ion_string.h>
#include <ion_types.h>
#include "ion_unit_test.h"
#include <ion_writer.h>
#include <stdbool.h>
#include <string.h>

iERR ion_cookbook_test() {
    iENTER;

    run_unit_test(test_ion_cookbook_hello_world);
    run_unit_test(test_ion_cookbook_write_hello_world_text);
    run_unit_test(test_ion_cookbook_write_hello_world_binary);

    iRETURN;
}

iERR test_ion_cookbook_hello_world() {
    iENTER;

    char* ion_text = "{ hello:\"world\" }";

    hREADER reader;
    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(options));

    IONCHECK(ion_reader_open_buffer(&reader, (BYTE*) ion_text, strlen(ion_text), &options));

    ION_TYPE type;

    ion_reader_next(reader, &type);
    ASSERT_EQUALS_ION_TYPE(tid_STRUCT, type, NULL);

    ion_reader_step_in(reader);
    ion_reader_next(reader, &type);
    ASSERT_EQUALS_ION_TYPE(tid_STRING, type, NULL);

    ION_STRING field_name;
    ion_reader_get_field_name(reader, &field_name);
    ASSERT_EQUALS_C_STRING_ION_STRING("hello", &field_name, NULL);

    ION_STRING field_value;
    ion_reader_read_string(reader, &field_value);
    ASSERT_EQUALS_C_STRING_ION_STRING("world", &field_value, NULL);

    ion_reader_step_out(reader);
    ion_reader_next(reader, &type);
    ASSERT_EQUALS_ION_TYPE(tid_EOF, type, NULL);

    IONCHECK(ion_reader_close(reader));

    iRETURN;
}

iERR test_ion_cookbook_write_hello_world_text() {
    iENTER;

    size_t buffer_size = 1024;
    BYTE* buffer = calloc(buffer_size, sizeof(BYTE));

    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));

    ION_WRITER* writer;
    IONCHECK(ion_writer_open_buffer(&writer, buffer, buffer_size, &options));

    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));

    char* field_name = "hello";
    ION_STRING ion_field_name;
    ion_string_assign_cstr(&ion_field_name, field_name, strlen(field_name));
    IONCHECK(ion_writer_write_field_name(writer, &ion_field_name));

    char* field_value = "world";
    ION_STRING ion_field_value;
    ion_string_assign_cstr(&ion_field_value, field_value, strlen(field_value));
    IONCHECK(ion_writer_write_string(writer, &ion_field_value));

    IONCHECK(ion_writer_finish_container(writer));

    SIZE bytes_written;
    IONCHECK(ion_writer_flush(writer, &bytes_written));
    ASSERT_EQUALS_INT(15, (int) bytes_written, NULL);

    IONCHECK(ion_writer_close(writer));

    ASSERT_EQUALS_C_STRING("{hello:\"world\"}", (char*) buffer, NULL);

    iRETURN;
}

iERR test_ion_cookbook_write_hello_world_binary() {
    iENTER;

    size_t buffer_size = 1024;
    BYTE* buffer = calloc(buffer_size, sizeof(BYTE));

    ION_WRITER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    options.output_as_binary = true;

    ION_WRITER* writer;
    IONCHECK(ion_writer_open_buffer(&writer, buffer, buffer_size, &options));

    IONCHECK(ion_writer_start_container(writer, tid_STRUCT));

    char* field_name = "hello";
    ION_STRING ion_field_name;
    ion_string_assign_cstr(&ion_field_name, field_name, strlen(field_name));
    IONCHECK(ion_writer_write_field_name(writer, &ion_field_name));

    char* field_value = "world";
    ION_STRING ion_field_value;
    ion_string_assign_cstr(&ion_field_value, field_value, strlen(field_value));
    IONCHECK(ion_writer_write_string(writer, &ion_field_value));

    IONCHECK(ion_writer_finish_container(writer));

    SIZE bytes_written;
    IONCHECK(ion_writer_flush(writer, &bytes_written));
    ASSERT_EQUALS_INT(24, (int) bytes_written, "Wrong number of bytes written");

    IONCHECK(ion_writer_close(writer));

    BYTE* expected = NULL;
    size_t expected_length = 0;
    hex_string_to_bytes("e00100eaeb8183d887b68568656c6c6fd78a85776f726c64", &expected, &expected_length);
    ASSERT_EQUALS_BYTES(
        expected,
        expected_length,
        buffer,
        bytes_written,
        NULL);

    fail:
    free(buffer);
    if (expected != NULL) {
        free(expected);
    }

    return err;
}
