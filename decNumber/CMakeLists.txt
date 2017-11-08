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

install(TARGETS decNumber
        DESTINATION lib)

install(FILES decContext.h decDouble.h decimal32.h decimal64.h decimal128.h
        decNumber.h decPacked.h decQuad.h decSingle.h
        DESTINATION include/decNumber)
