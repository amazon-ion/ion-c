cmake_minimum_required(VERSION 3.10)

option(BUILD_TESTING "" OFF)
set(BUILD_SHARED_LIBS OFF)
option(MSGPACK_BUILD_TESTS "" OFF)

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF)

add_subdirectory(deps/google-benchmark EXCLUDE_FROM_ALL)
add_subdirectory(deps/json-c EXCLUDE_FROM_ALL)
add_subdirectory(deps/libcbor EXCLUDE_FROM_ALL)
add_subdirectory(deps/msgpack-c EXCLUDE_FROM_ALL)
add_subdirectory(deps/yyjson EXCLUDE_FROM_ALL)
add_subdirectory(src)
