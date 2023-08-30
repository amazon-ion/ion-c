
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

option(IONC_BENCHMARKING_ENABLED "Enable ion-bench build" OFF)
add_subdirectory(ionizer)
add_subdirectory(ionsymbols)
add_subdirectory(events)
add_subdirectory(cli)
if (IONC_BENCHMARKING_ENABLED)
   add_subdirectory(ion-bench)
endif()
