cmake_minimum_required(VERSION 3.2)

project(IonC)

if (MSVC)
else()
    set(BUILD_SHARED_LIBS ON)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -Wall")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -Wall")
endif()

add_subdirectory(decNumber)
add_subdirectory(ionc)
add_subdirectory(test)
add_subdirectory(tools)

export(PACKAGE IonC)
