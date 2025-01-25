set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if (NOT CMAKE_CXX_FLAGS OR CMAKE_CXX_STANDARD LESS 14)
   set(CMAKE_CXX_STANDARD 14)
endif()

# Avoid macro definition collisions between ionc and googletest.
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DGTEST_DONT_DEFINE_SUCCEED=1 -DGTEST_DONT_DEFINE_FAIL=1")
# Verbose parameterized names are disabled in debug configuration and on Windows. These don't
# always integrate well with IDEs (e.g. CLion), but are nice to have from the command line.

add_executable(all_tests
    gather_vectors.cpp
    ion_assert.cpp
    ion_test_util.cpp
    test_vectors.cpp
    test_ion_binary.cpp
    test_ion_writer.cpp
    test_ion_decimal.cpp
    test_ion_symbol.cpp
    test_ion_text.cpp
    test_ion_timestamp.cpp
    test_ion_values.cpp
    test_ion_extractor.cpp
    test_ion_integer.cpp
    test_ion_cli.cpp
    test_ion_stream.cpp
    test_ion_reader_seek.cpp
)

target_include_directories(all_tests
        PRIVATE
            .
            ../tools/cli
            ../tools/events
            ../tools/events/inc
            ../ionc/
            ../ionc/include
)

# Linking against gtest_main provides a basic main() method, which detects all tests, for free.
target_link_libraries(all_tests ionc ion_events ion_cli gtest_main)
