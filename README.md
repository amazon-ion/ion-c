# Amazon Ion C
A C implementation of the [Ion data notation](https://amazon-ion.github.io/ion-docs).

[![Build Status](https://travis-ci.org/amazon-ion/ion-c.svg?branch=master)](https://travis-ci.org/amazon-ion/ion-c)
[![Build status](https://ci.appveyor.com/api/projects/status/x6xfom3x3hs3y945/branch/master?svg=true)](https://ci.appveyor.com/project/tgregg/ion-c-3akm7/branch/master)
<a title="docs" href="https://amazon-ion.github.io/ion-c"><img src="https://img.shields.io/badge/docs-api-green.svg"/></a>

## Setup
This repository contains a [git submodule](https://git-scm.com/docs/git-submodule)
called `ion-tests`, which holds test data used by `ion-c`'s unit tests.

The easiest way to clone the `ion-c` repository and initialize its `ion-tests`
submodule is to run the following command.

```
$ git clone --recursive https://github.com/amazon-ion/ion-c.git ion-c
```

Alternatively, the submodule may be initialized independently from the clone
by running the following commands.

```
$ git submodule init
$ git submodule update
```

The submodule points to the tip of the branch of the `ion-tests` repository
specified in `ion-c`'s `.gitmodules` file.

### Pulling in Upstream Changes
To pull upstream changes into `ion-c`, start with a simple `git pull`.
This will pull in any changes to `ion-c` itself (including any changes
to its `.gitmodules` file), but not any changes to the `ion-tests`
submodule. To make sure the submodule is up-to-date, use the following
command.

```
$ git submodule update --remote
```

This will fetch and update the ion-tests submodule from the `ion-tests` branch
currently specified in the `.gitmodules` file.

For detailed walkthroughs of git submodule usage, see the
[Git Tools documentation](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

## Building the Library
Use the provided scripts `build-release.sh` and `build-debug.sh`. Ensure that `cmake` is installed first.

### On macOS
`cmake` can be installed using [Homebrew](https://brew.sh/): `brew install cmake`

## Using the Library
A great way to get started is to use the [Ion cookbook](https://amazon-ion.github.io/ion-docs/guides/cookbook.html).
