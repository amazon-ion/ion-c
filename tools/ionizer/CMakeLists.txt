cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

add_executable(ionizer
  ionizer_args.c
  ionizer.c
  ionizer_stream.c
  options.c
)
set_property(TARGET ionizer PROPERTY C_STANDARD 99)
target_include_directories(ionizer
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../decNumber/include
)

if (MSVC)
    target_link_libraries(ionizer ionc)
else()
    # Unix requires linking against lib m explicitly.
    target_link_libraries(ionizer ionc m)
endif()

