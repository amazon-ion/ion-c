include_directories(./)
# A library for testing.
add_library(ion_events ion_event_util.cpp ion_event_stream.cpp)
target_link_libraries(ion_events ionc)
