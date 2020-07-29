
set(declibsrc 
        decContext.c
        decimal128.c
        decimal64.c
        decimal32.c
        decNumber.c
        decPacked.c
        decQuad.c)
        
set(DEC_PUB_HEADERS 
    include/decNumber/decContext.h
    include/decNumber/decDouble.h
    include/decNumber/decDPD.h
    include/decNumber/decimal128.h
    include/decNumber/decimal32.h
    include/decNumber/decimal64.h
    include/decNumber/decNumber.h
    include/decNumber/decNumberLocal.h
    include/decNumber/decPacked.h
    include/decNumber/decQuad.h
    include/decNumber/decSingle.h)

add_library(decNumber ${declibsrc})
set_property(TARGET decNumber PROPERTY POSITION_INDEPENDENT_CODE 1)

target_include_directories(decNumber
        PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

set_target_properties(decNumber
    PROPERTIES
        PUBLIC_HEADER "${DEC_PUB_HEADERS}")

# ---

add_library(decNumber_static STATIC ${declibsrc})
set_property(TARGET decNumber_static PROPERTY POSITION_INDEPENDENT_CODE 1)

target_include_directories(decNumber_static
        PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

set_target_properties(decNumber_static
    PROPERTIES
        PUBLIC_HEADER "${DEC_PUB_HEADERS}")

# ---

install(TARGETS decNumber decNumber_static
        EXPORT IonCTargets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/decNumber)


install(DIRECTORY include/decNumber
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h")
