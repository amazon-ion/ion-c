
set(libsrc
        decQuadHelpers.c
        ion_allocation.c
        ion_binary.c
        ion_catalog.c
        ion_collection.c
        ion_debug.c
        ion_errors.c
        ion_helpers.c
        ion_index.c
        ion_initialize.c
        ion_int.c
        ion_reader_binary.c
        ion_reader.c
        ion_reader_text.c
        ion_scanner.c
        ion_stream.c
        ion_string.c
        ion_symbol_table.c
        ion_timestamp.c
        ion_writer_binary.c
        ion_writer.c
        ion_writer_text.c
        ion_decimal.c
        ion_float.c
        ion_extractor.c)

set(LIB_PUB_HEADERS 
    include/ionc/ion_catalog.h
    include/ionc/ion_collection.h
    include/ionc/ion_debug.h
    include/ionc/ion_decimal.h
    include/ionc/ion_error_codes.h
    include/ionc/ion_errors.h
    include/ionc/ion_extractor.h
    include/ionc/ion_float.h
    include/ionc/ion.h
    include/ionc/ion_int.h
    include/ionc/ion_platform_config.h
    include/ionc/ion_reader.h
    include/ionc/ion_stream.h
    include/ionc/ion_string.h
    include/ionc/ion_symbol_table.h
    include/ionc/ion_timestamp.h
    include/ionc/ion_types.h
    include/ionc/ion_writer.h)



# this is the "object library" target: compiles the sources only once
add_library(objlib OBJECT ${libsrc})

# shared libraries need PIC
set_target_properties(objlib
        PROPERTIES
            POSITION_INDEPENDENT_CODE 1
            C_STANDARD 99)

target_include_directories(objlib
        PUBLIC
            $<INSTALL_INTERFACE:include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../decNumber/include>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}

)


if (MSVC)
  add_library(ionc $<TARGET_OBJECTS:objlib>) 
else()
  add_library(ionc SHARED $<TARGET_OBJECTS:objlib>)
  target_include_directories(ionc
      PUBLIC
            $<INSTALL_INTERFACE:include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../decNumber/include>
      PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}

)
  set_target_properties(ionc
        PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION}
        PUBLIC_HEADER "${LIB_PUB_HEADERS}")
endif()

add_library(ionc_static STATIC $<TARGET_OBJECTS:objlib>)


if (MSVC)
    target_link_libraries(ionc decNumber)
else()
    # Unix requires linking against lib m explicitly.
    target_link_libraries(ionc PUBLIC decNumber m)
endif()

set(INSTALL_CONFIGDIR ${CMAKE_INSTALL_LIBDIR}/cmake/IonC)

install(TARGETS ionc ionc_static
        EXPORT IonCTargets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ionc)

install(EXPORT IonCTargets
        FILE IonCTargets.cmake
        NAMESPACE IonC::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/IonC)

install(DIRECTORY include/ionc
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING PATTERN "*.h")

####
## CMake packaging. Allows other CMake projects to detect and use IonC.
####
include(CMakePackageConfigHelpers)

message("CURRENT_SOURCE_DIR : ${CMAKE_CURRENT_SOURCE_DIR}")
message("SOURCE_DIR : ${CMAKE_SOURCE_DIR}")
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/IonCConfig.cmake.in
  ${CMAKE_BINARY_DIR}/cmake/IonCConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/IonC)


write_basic_package_version_file(
        ${CMAKE_BINARY_DIR}/cmake/IonCConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion)

install(
        FILES
        ${CMAKE_BINARY_DIR}/cmake/IonCConfig.cmake
        ${CMAKE_BINARY_DIR}/cmake/IonCConfigVersion.cmake
        DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/IonC)
