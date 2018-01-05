
# Avoid macro definition collisions between ionc and googletest.
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DGTEST_DONT_DEFINE_SUCCEED=1 -DGTEST_DONT_DEFINE_FAIL=1")
# Verbose parameterized names are disabled in debug configuration and on Windows. These don't
# always integrate well with IDEs (e.g. CLion), but are nice to have from the command line.
if (MSVC)
    # Prevent googletest from linking against different versions of the C Runtime Library on
    # Windows.
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    # Prevent deprecation errors for std::tr1 in googletest
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -Wall")
endif()

add_executable(all_tests
    gather_vectors.cpp
    ion_event_stream.cpp
    ion_assert.cpp
    ion_test_util.cpp
    test_vectors.cpp
    test_ion_binary.cpp
    test_ion_decimal.cpp
    test_ion_symbol.cpp
    test_ion_text.cpp
    test_ion_values.cpp
    test_ion_extractor.cpp
)

add_subdirectory(googletest)

include_directories(
    .
    googletest/googletest/include/gtest
)

# Linking against gtest_main provides a basic main() method, which detects all tests, for free.
target_link_libraries(all_tests ionc gtest_main)
