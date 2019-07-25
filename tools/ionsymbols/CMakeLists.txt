cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

add_executable(ionsymbols
  ionsymbols_args.c
  ionsymbols.c
  options.c
)
set_property(TARGET ionsymbols PROPERTY C_STANDARD 99)

target_include_directories(ionsymbols
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../decNumber/include
)

target_link_libraries(ionsymbols ionc)

