#include "ion_cookbook_test.h"

#include "ion_assert.h"
#include <ion_reader.h>
#include <ion_string.h>
#include <ion_types.h>
#include <string.h>

iERR ion_cookbook_test() {
    iENTER;

    run_unit_test(test_ion_cookbook_hello_world);

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
