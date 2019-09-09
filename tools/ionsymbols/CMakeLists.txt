cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

add_executable(ionsymbols
  ionsymbols_args.c
  ionsymbols.c
  options.c
)
set_property(TARGET ionsymbols PROPERTY C_STANDARD 99)
target_include_directories(ionsymbols
        PRIVATE
            .
            ../../ionc
            ../../ionc/include
)
target_link_libraries(ionsymbols ionc)

