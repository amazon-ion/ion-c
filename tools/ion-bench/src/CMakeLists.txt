cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

set(SOURCES main.cpp common.cc ../../cli/argtable/argtable3.c)
if (TRACK_MEMORY)
   list(APPEND SOURCES memory.c)
endif()

add_executable(IonCBench ${SOURCES})
set_property(TARGET IonCBench
   PROPERTY CXX_STANDARD 17
)
set_property(TARGET IonCBench
   PROPERTY CXX_STANDARD_REQUIRED ON
)
target_compile_features(IonCBench PRIVATE cxx_std_17)
target_include_directories(IonCBench PRIVATE ${libcbor_BINARY_DIR} ../../cli/argtable/)
target_link_libraries(IonCBench
   benchmark::benchmark
   objlib
   m
   decNumber_static
   yyjson
   msgpack-c
   json-c
   cbor
)
