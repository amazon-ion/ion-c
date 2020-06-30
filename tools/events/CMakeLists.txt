add_library(ion_events 
        ion_event_util.cpp
        ion_event_stream.cpp
        ion_event_equivalence.cpp)

target_include_directories(ion_events
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/inc
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
)

target_link_libraries(ion_events ionc)

add_library(ion_events_static STATIC
        ion_event_util.cpp
        ion_event_stream.cpp
        ion_event_equivalence.cpp)

target_include_directories(ion_events_static
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/inc
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
)

target_link_libraries(ion_events_static ionc)
