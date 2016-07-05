add_executable(tester
  ion_binary_test.c
  ion_cookbook_test.c
  ion_unit_test.c
  test_internal.c
  tester.c
  testfiles.c
)
set_property(TARGET tester PROPERTY C_STANDARD 99)
target_include_directories(tester PRIVATE
  .
)
target_link_libraries(tester ionc)
