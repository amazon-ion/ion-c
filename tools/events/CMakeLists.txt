include_directories(./)
# The library to use in normal applications, like the CLI.
add_library(ion_events ion_event_util.cpp ion_event_stream.cpp ion_event_equivalence.cpp)
target_link_libraries(ion_events ionc)

# The library to use in tests. Links against gtest and performs gtest assertions when equivalence assertions fail.
add_library(ion_events_gtest ion_event_util.cpp ion_event_stream.cpp ion_event_equivalence.cpp)
target_compile_definitions(ion_events_gtest PUBLIC ION_ASSERT_ENABLE_GTEST GTEST_DONT_DEFINE_SUCCEED=1 GTEST_DONT_DEFINE_FAIL=1)
target_link_libraries(ion_events_gtest ionc gtest)
