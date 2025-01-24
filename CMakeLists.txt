cmake_minimum_required(VERSION 3.6 FATAL_ERROR)
project(IonC
        VERSION 1.1.3
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

set(IONC_FORCE_SANITIZERS OFF CACHE BOOL "Force use of compiler sanitizers")
set(IONC_ENABLE_VERBOSE_DEBUG OFF CACHE BOOL "Enable verbose logging for ion-c")

## Build Type Settings
if (CMAKE_BUILD_TYPE STREQUAL "Release")
   add_compile_definitions(NDEBUG=1)
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
   include(CheckCXXCompilerFlag)
   # check_cxx_compiler_flag doesn't just check compiler flags, it also attempts to link the program, which
   # in this case, also requires sanitizer flags. So we have to abuse CMAKE_REQUIRED_LINK_OPTIONS since that's
   # the only lever we're given afaict. This does not work with cmake prior to 3.14.
   list(INSERT CMAKE_REQUIRED_LINK_OPTIONS 0 "-fsanitize=address,undefined")
   check_cxx_compiler_flag("-g -fsanitize=address,undefined -fno-omit-frame-pointer" UBISAN_OK)
   list(REMOVE_AT CMAKE_REQUIRED_LINK_OPTIONS 0)
   if (UBISAN_OK OR IONC_FORCE_SANITIZERS)
      add_compile_options(
         -fsanitize=address,undefined
         -fsanitize-recover=address
      )
      add_link_options(-fsanitize=address,undefined -fsanitize-recover=address)
   endif()
   add_compile_options(-g -fno-omit-frame-pointer -fno-optimize-sibling-calls)
   if (IONC_ENABLE_VERBOSE_DEBUG)
       add_compile_definitions(DEBUG=1)
   endif()
elseif (CMAKE_BUILD_TYPE STREQUAL "Profiling")
   add_compile_options(
      -O3 -march=native
      -fno-omit-frame-pointer
      -fno-optimize-sibling-calls
      -g
   )
   add_compile_definitions(NDEBUG=1)
   set(IONC_BENCHMARKING_ENABLED ON CACHE BOOL "Enable Benchmarking")
endif()

## ion-c Build Version
set(IONC_FULL_VERSION "v${CMAKE_PROJECT_VERSION}-0-g000000") # Format cmake project version to match git describe output
find_program(GIT_EXECUTABLE "git")
add_custom_target(
   version
   ${CMAKE_COMMAND} -D SRC=${CMAKE_CURRENT_SOURCE_DIR}/build_version.h.in
                    -D DST=${CMAKE_CURRENT_BINARY_DIR}/build_version.h
                    -D GIT_EXECUTABLE=${GIT_EXECUTABLE}
                    -D IONC_FULL_VERSION=${IONC_FULL_VERSION}
                    -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/VersionHeader.cmake
)

include(GNUInstallDirs)

set(IONC_DECIMAL_NUM_DIGITS "34" CACHE STRING "Number of digits supported without added allocation")
option(IONC_BUILD_TESTS "Enable or Disable building of ion-c tests" ON)

# NOTE: DECNUMDIGITS must be set across all compilation units to at least DECQUAD_Pmax (34), so that the value is
# guaranteed to be consistent between ionc and decNumber. This is required for conversions between decQuad and
# decNumber. This is NOT the limit on decimal precision; ION_DECIMAL can handle arbitrarily large precision.
add_definitions(-DDECNUMDIGITS=${IONC_DECIMAL_NUM_DIGITS})
message(STATUS "Setting DECNUMBER max digits to ${IONC_DECIMAL_NUM_DIGITS}")

set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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


add_subdirectory(decNumber)
add_subdirectory(ionc)
add_subdirectory(tools)
if (IONC_BUILD_TESTS)
   if (NOT TARGET gtest_main)
      message("Using included google-test")
      # Prevent googletest from linking against different versions of the C Runtime Library on
      # Windows.
      set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
      add_subdirectory(deps/google-test EXCLUDE_FROM_ALL)
   endif()
   add_subdirectory(test)
endif()
