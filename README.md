[![license](http://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/kostya-sh/parquet-go/master/LICENSE)
[![Build Status](https://travis-ci.org/kostya-sh/parquet-go.svg?branch=master)](https://travis-ci.org/kostya-sh/parquet-go)

# parquet-go

Library to work with Parquet file format in Go.

## The current state

A low level API for reading parquet files provides methods to read everything
that a parquet file consist of:
- file metadata
- page headers
- schema
- values along with definition and repetition levels

All encodings and all parquet types are supported.

GZIP and SNAPPY compression codecs are supported.

While I took considerable effort to test the implementation I am sure there are
some bugs that are still lurking around. This library has never been used in any
production system.

The library should be pretty fast but there are still quite a few known missing
performance optimisations.

Writing files and assembling records are not implemented.

## Usage

For examples how to use the library check out the source code of parqueteur
command. `csv.go` is the best starting point.

## Future

Currently I do not plan to work on adding new features. I will try to fix any
reported bugs though.
