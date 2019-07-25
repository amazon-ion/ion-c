cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

add_subdirectory(docopt)

# C++ standard
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_CXX_STANDARD OR CMAKE_CXX_STANDARD LESS 11)
    set(CMAKE_CXX_STANDARD 11)
endif()


# The public executable.
add_executable(ion main.cpp cli.cpp)

target_include_directories(ion
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/docopt
        ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
        ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../events
        ${CMAKE_CURRENT_SOURCE_DIR}/../events/inc
        ${CMAKE_CURRENT_SOURCE_DIR}/../../decNumber/include
        )

target_link_libraries(ion -static ionc_static ion_events_static docopt_s)


install(TARGETS
        ion
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})


# A library for testing.
add_library(ion_cli cli.cpp)
target_include_directories(ion_cli
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/docopt
        ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc
        ${CMAKE_CURRENT_SOURCE_DIR}/../../ionc/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../events
        ${CMAKE_CURRENT_SOURCE_DIR}/../events/inc
        ${CMAKE_CURRENT_SOURCE_DIR}/../../decNumber/include
        )

target_link_libraries(ion_cli ionc ion_events)
