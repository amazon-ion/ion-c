cmake_minimum_required(VERSION 3.5 FATAL_ERROR)


set(eventslibsrc
        ion_event_util.cpp
        ion_event_stream.cpp
        ion_event_equivalence.cpp)

add_library(eventsobjlib OBJECT ${eventslibsrc})
# shared libraries need PIC
set_property(TARGET eventsobjlib PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(ion_events SHARED $<TARGET_OBJECTS:eventsobjlib>)
add_library(ion_events_static STATIC $<TARGET_OBJECTS:eventsobjlib>)


target_include_directories(eventsobjlib
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/inc>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
            ${CMAKE_CURRENT_SOURCE_DIR}/../../decNumber/include
)



target_link_libraries(ion_events ionc)
target_link_libraries(ion_events_static ionc_static)
