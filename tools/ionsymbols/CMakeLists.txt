add_executable(ionsymbols
  ionsymbols_args.c
  ionsymbols.c
  options.c
)
set_property(TARGET ionsymbols PROPERTY C_STANDARD 99)
target_include_directories(ionsymbols PRIVATE
  .
)
target_link_libraries(ionsymbols ionc)

