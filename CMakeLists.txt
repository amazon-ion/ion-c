cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

project(IonC
        VERSION 1.0.3
        LANGUAGES CXX C)

# we default to 'Release' build type
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
message(STATUS "Build type set to ${CMAKE_BUILD_TYPE}")

if (MSVC)
else()
    set(BUILD_SHARED_LIBS ON)
    add_compile_options("$<$<CONFIG:DEBUG>:-O0;-g3;-ggdb3;-Wall>")
endif()

# NOTE: DECNUMDIGITS must be set across all compilation units to at least DECQUAD_Pmax (34), so that the value is
# guaranteed to be consistent between ionc and decNumber. This is required for conversions between decQuad and
# decNumber. This is NOT the limit on decimal precision; ION_DECIMAL can handle arbitrarily large precision.
add_definitions(-DDECNUMDIGITS=34)
message(STATUS "Setting DECNUMBER max digits to 34")

add_subdirectory(decNumber)
add_subdirectory(ionc)
add_subdirectory(test)
add_subdirectory(tools)

export(PACKAGE IonC)
