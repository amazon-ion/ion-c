# Amazon Ion C
A C implementation of the [Ion data notation](http://amzn.github.io/ion-docs).

[![Build Status](https://travis-ci.org/amzn/ion-c.svg?branch=master)](https://travis-ci.org/amzn/ion-c)

## Setup
This repository contains a [git submodule](https://git-scm.com/docs/git-submodule)
called `ion-tests`, which holds test data used by `ion-c`'s unit tests.

The easiest way to clone the `ion-c` repository and initialize its `ion-tests`
submodule is to run the following command.

```
$ git clone --recursive https://github.com/amzn/ion-c.git ion-c
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

## Using the Library
Ion cookbook for C coming soon!
