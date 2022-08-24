#!/bin/sh --

set -x

DEFAULT_CFLAGS="-fsanitize=address,undefined -fsanitize-recover=address -fno-omit-frame-pointer -fno-optimize-sibling-calls"
DEFAULT_CXXFLAGS="${DEFAULT_CFLAGS}"
DEFAULT_LDFLAGS="-fsanitize=address,undefined"

export LD="$CXX"

mkdir -p build/debug && cd build/debug
cmake \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_FLAGS_DEBUG="${DEBUG_CFLAGS-${DEFAULT_CFLAGS}}" \
  -DCMAKE_CXX_FLAGS_DEBUG="${DEBUG_CXXFLAGS-${DEFAULT_CXXFLAGS}}" \
  ../..
make clean && make LDFLAGS="${DEBUG_LDFLAGS-${DEFAULT_LDFLAGS}}" -j"$(nproc || sysctl -n hw.ncpu)"
