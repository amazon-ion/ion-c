cmake_minimum_required(VERSION 3.5)

project(IonC)

# NOTE: DECNUMDIGITS must be set across all compilation units to at least DECQUAD_Pmax (34), so that the value is
# guaranteed to be consistent between ionc and decNumber. This is required for conversions between decQuad and
# decNumber. This is NOT the limit on decimal precision; ION_DECIMAL can handle arbitrarily large precision.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDECNUMDIGITS=34")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DDECNUMDIGITS=34")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall -DDECNUMDIGITS=34")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -Wall -DDECNUMDIGITS=34")
set(BUILD_SHARED_LIBS ON)

add_subdirectory(decNumber)
add_subdirectory(ionc)
add_subdirectory(test)
add_subdirectory(tools)

export(PACKAGE IonC)
