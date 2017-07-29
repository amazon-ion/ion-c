add_library(decNumber
  decContext.c
  decimal128.c
  decimal64.c
  decNumber.c
  decPacked.c
  decQuad.c
)
target_include_directories(decNumber PUBLIC
  .
)
