add_executable(tester
  tester.c
  testfiles.c
  test_internal.c
)
set_property(TARGET tester PROPERTY C_STANDARD 99)
target_include_directories(tester PRIVATE
  .
)
target_link_libraries(tester ionc)
