
# Avoid macro definition collisions between ionc and googletest.
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DGTEST_DONT_DEFINE_SUCCEED=1 -DGTEST_DONT_DEFINE_FAIL=1")

# Disable verbose parameterized test names in debug configuration. These don't always integrate well with IDEs
# (e.g. CLion), but are nice to have from the command line.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -Wall")

add_subdirectory(googletest)

add_executable(all_tests
    gather_vectors.cpp
    ion_event_stream.cpp
    ion_assert.cpp
    test_vectors.cpp
    test_ion_binary.cpp
    test_ion_text.cpp
)

include_directories(
    .
    googletest/googletest/include/gtest
)

# Linking against gtest_main provides a basic main() method, which detects all tests, for free.
target_link_libraries(all_tests ionc gtest_main)
