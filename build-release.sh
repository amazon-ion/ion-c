#!/bin/sh --

mkdir -p build/release
cd build/release

if ! [ -x "$(command -v cmake)" ]; then
  echo 'Error: cmake is not installed.' >&2
  exit 1
fi

cmake -DCMAKE_BUILD_TYPE=Release ../..
make clean && make
