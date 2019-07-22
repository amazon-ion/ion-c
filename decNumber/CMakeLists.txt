cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

set(declibsrc
        decContext.c
        decimal128.c
        decimal64.c
        decimal32.c
        decNumber.c
        decPacked.c
        decQuad.c
        )

# this is the "object library" target: compiles the sources only once
add_library(decobjlib OBJECT ${declibsrc})

# shared libraries need PIC
set_property(TARGET decobjlib PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(decNumber SHARED $<TARGET_OBJECTS:decobjlib>)
add_library(decNumber_static STATIC $<TARGET_OBJECTS:decobjlib>)

target_include_directories(decobjlib
        PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

install(TARGETS
            decNumber
            decNumber_static
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/decNumber
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h")