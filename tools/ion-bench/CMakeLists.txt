cmake_minimum_required(VERSION 3.10)

option(BUILD_TESTING "" OFF)
set(BUILD_SHARED_LIBS OFF)
option(MSGPACK_BUILD_TESTS "" OFF)

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF)

# We currently need std::filesystem to build IonCBench.
Include(CheckCXXSourceCompiles)
check_cxx_source_compiles(
   "#include <filesystem>\nint main(int argc, char **argv) { return 0; }"
   CXX_FILESYSTEM_SUPPORTED
)

if (CXX_FILESYSTEM_SUPPORTED)
   add_subdirectory(deps/google-benchmark EXCLUDE_FROM_ALL)
   add_subdirectory(deps/json-c EXCLUDE_FROM_ALL)
   add_subdirectory(deps/libcbor EXCLUDE_FROM_ALL)
   add_subdirectory(deps/msgpack-c EXCLUDE_FROM_ALL)
   add_subdirectory(deps/yyjson EXCLUDE_FROM_ALL)
   add_subdirectory(src)
else()
   message("NOTE: C++17, or at least std::filesystem not supported, cannot build IonCBench")
endif()
