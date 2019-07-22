cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

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


# this is the "object library" target: compiles the sources only once
add_library(objlib OBJECT ${libsrc})

# shared libraries need PIC
set_property(TARGET objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
set_property(TARGET objlib
             PROPERTY C_STANDARD 99)

add_library(ionc SHARED $<TARGET_OBJECTS:objlib>)
add_library(ionc_static STATIC $<TARGET_OBJECTS:objlib>)

target_include_directories(objlib
        PUBLIC
            $<INSTALL_INTERFACE:include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../decNumber/include>
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}

)

if (MSVC)
    target_link_libraries(ionc decNumber)
    target_link_libraries(ionc_static decNumber_static)
else()
    # Unix requires linking against lib m explicitly.
    target_link_libraries(ionc decNumber m)
    target_link_libraries(ionc_static decNumber_static m)
endif()


set(INSTALL_CONFIGDIR ${CMAKE_INSTALL_LIBDIR}/cmake/IonC)

install(TARGETS
            ionc
            ionc_static
            decNumber
            decNumber_static
        EXPORT
            IonCTargets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

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


configure_package_config_file(
  ${CMAKE_SOURCE_DIR}/cmake/IonCConfig.cmake.in
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