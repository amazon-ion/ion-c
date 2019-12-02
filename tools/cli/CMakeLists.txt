cmake_minimum_required(VERSION 3.14 FATAL_ERROR)

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
            ./
            ../events
            ../events/inc
            ../../ionc
            ../../ionc/include
            docopt)

target_link_libraries(ion ionc ion_events docopt)

# A library for testing.
add_library(ion_cli cli.cpp)
target_include_directories(ion_cli
        PRIVATE
        ./
        ../events
        ../../ionc
        ../../ionc/include
        ../events/inc)
target_link_libraries(ion_cli ionc ion_events)
