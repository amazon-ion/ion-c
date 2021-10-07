cmake_minimum_required(VERSION 3.6 FATAL_ERROR)

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

include(GNUInstallDirs)

# NOTE: DECNUMDIGITS must be set across all compilation units to at least DECQUAD_Pmax (34), so that the value is
# guaranteed to be consistent between ionc and decNumber. This is required for conversions between decQuad and
# decNumber. This is NOT the limit on decimal precision; ION_DECIMAL can handle arbitrarily large precision.
add_definitions(-DDECNUMDIGITS=34)
message(STATUS "Setting DECNUMBER max digits to 34")

set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_subdirectory(decNumber)
add_subdirectory(ionc)
add_subdirectory(test)
add_subdirectory(tools)

export(PACKAGE IonC)

include(cmake/CMakeCPack.cmake)

## Convenience target to build and install 
add_custom_target(release 
  COMMENT "Build and install the library"
  COMMENT "Binary Dir : ${CMAKE_BINARY_DIR}" 
  COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --target install
  USES_TERMINAL)

####
## Create cmake uninstall target
####

#  First, create the cmake script that will do the actual uninstall.

configure_file( "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
                "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake" @ONLY )

#  Define an uninstall target that will run the above script.

add_custom_target(uninstall
                  COMMAND ${CMAKE_COMMAND} -P
                  "${CMAKE_CURRENT_BINARY_DIR}/cmake/cmake_uninstall.cmake" )
