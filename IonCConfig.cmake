set(IONC_INCLUDE_DIRS ${CMAKE_CURRENT_LIST_DIR}/ionc/include ${CMAKE_CURRENT_LIST_DIR}/decNumber/include)
set(IONC_LIBRARY ${CMAKE_CURRENT_LIST_DIR}/build/release/ionc/libionc${CMAKE_SHARED_LIBRARY_SUFFIX})
set(IONC_LIBRARIES
        ${CMAKE_CURRENT_LIST_DIR}/build/release/ionc/libionc${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_CURRENT_LIST_DIR}/build/release/decNumber/libdecNumber${CMAKE_SHARED_LIBRARY_SUFFIX}
)