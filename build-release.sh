#!/bin/sh --

set -x

export LD="$CXX"

mkdir -p build/release
cd build/release

if ! [ -x "$(command -v cmake)" ]; then
  echo 'Error: cmake is not installed.' >&2
  exit 1
fi

cmake \
   -DCMAKE_BUILD_TYPE=Release \
   ${CMAKE_FLAGS} \
   ../..

make clean && make -j"$(nproc || sysctl -n hw.ncpu)"
