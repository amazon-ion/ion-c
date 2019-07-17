cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

add_library(decNumber
        decContext.c
        decimal128.c
        decimal64.c
        decimal32.c
        decNumber.c
        decPacked.c
        decQuad.c
        )

target_include_directories(decNumber
        PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

install(TARGETS decNumber
        DESTINATION lib)

install(FILES include/decNumber/decContext.h
        include/decNumber/decDouble.h
        include/decNumber/decimal32.h
        include/decNumber/decimal64.h
        include/decNumber/decimal128.h
        include/decNumber/decNumber.h
        include/decNumber/decPacked.h
        include/decNumber/decQuad.h
        include/decNumber/decSingle.h
        DESTINATION include/decNumber)
