add_library(ionc
  decQuadHelpers.c
  ion_allocation.c
  ion_binary.c
  ion_catalog.c
  ion_collection.c
  ion_debug.c
  ion_errors.c
  ion_helpers.c
  ion_index.c
  ion_initialize.c
  ion_int.c
  ion_reader_binary.c
  ion_reader.c
  ion_reader_text.c
  ion_scanner.c
  ion_stream.c
  ion_string.c
  ion_symbol_table.c
  ion_timestamp.c
  ion_writer_binary.c
  ion_writer.c
  ion_writer_text.c
  ion_decimal.c
  ion_float.c
)
set_property(TARGET ionc PROPERTY C_STANDARD 99)
target_include_directories(ionc PUBLIC
  .
  inc/

)
target_link_libraries(ionc decNumber m)
