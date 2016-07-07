#include "ion_cookbook_test.h"

#include "ion_assert.h"
#include <ion_reader.h>
#include <ion_string.h>
#include <ion_types.h>
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
    IONCHECK(assert_equals_ion_type(tid_STRUCT, type));

    ion_reader_step_in(reader);
    ion_reader_next(reader, &type);
    IONCHECK(assert_equals_ion_type(tid_STRING, type));

    ION_STRING field_name;
    ion_reader_get_field_name(reader, &field_name);
    IONCHECK(assert_equals_c_string_ion_string("hello", &field_name));

    ION_STRING field_value;
    ion_reader_read_string(reader, &field_value);
    IONCHECK(assert_equals_c_string_ion_string("world", &field_value));

    ion_reader_step_out(reader);
    ion_reader_next(reader, &type);
    IONCHECK(assert_equals_ion_type(tid_EOF, type));

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
    IONCHECK(assert_equals_int(15, (int) bytes_written));

    IONCHECK(ion_writer_close(writer));

    IONCHECK(assert_equals_c_string("{hello:\"world\"}", (char*) buffer));

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
    IONCHECK(assert_equals_int(24, (int) bytes_written));

    IONCHECK(ion_writer_close(writer));

    IONCHECK(
        assert_equals_bytes(
            hex_string_to_bytes("e00100eaeb8183d887b68568656c6c6fd78a85776f726c64"),
            buffer,
            bytes_written));

    iRETURN;
}
