include_directories(./ inc/)
add_library(ion_events ion_event_util.cpp ion_event_stream.cpp ion_event_equivalence.cpp)
target_link_libraries(ion_events ionc)
