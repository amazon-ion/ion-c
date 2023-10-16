#!/bin/sh --

set -x

export LD="$CXX"

if ! [ -x "$(command -v cmake)" ]; then
  echo 'Error: cmake is not installed.' >&2
  exit 1
fi

mkdir -p build/debug && cd build/debug
cmake \
  -DCMAKE_BUILD_TYPE=Debug \
  ${CMAKE_FLAGS} \
  ../..
make clean && make -j"$(nproc || sysctl -n hw.ncpu)"
