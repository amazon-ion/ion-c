add_library(decNumber
  decContext.c
  decDouble.c
  decimal128.c
  decimal32.c
  decimal64.c
  decNumber.c
  decPacked.c
  decQuad.c
  decSingle.c
)
target_include_directories(decNumber PUBLIC
  .
)
