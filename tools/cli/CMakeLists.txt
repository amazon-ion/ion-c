
# C++ standard
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_CXX_STANDARD OR CMAKE_CXX_STANDARD LESS 11)
    set(CMAKE_CXX_STANDARD 11)
endif()


# The public executable.
add_executable(ion main.cpp argtable/argtable3.c cli.cpp)

target_include_directories(ion
        PRIVATE
            ./
            ../events
            ../events/inc
            ../../ionc
            ../../ionc/include
            argtable)

target_link_libraries(ion ionc ion_events)

# A static library version of `ion` that does not include a main()
# function, allowing other programs to wrap it.
add_library(ion_cli_main STATIC main.cpp argtable/argtable3.c cli.cpp)
target_compile_definitions(ion_cli_main PUBLIC EXTERNAL_DRIVER)
target_include_directories(ion_cli_main
        PRIVATE
            ./
            ../events
            ../events/inc
            ../../ionc
            ../../ionc/include
            argtable)
target_link_libraries(ion_cli_main ionc ion_events)


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
