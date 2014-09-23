#lmdb [![Build Status](https://travis-ci.org/bmatsuo/lmdb.exp.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb.exp)

Go bindings to the OpenLDAP Lightning Memory-Mapped Database (LMDB).

This is an incompatible fork of "github.com/szferi/gomdb".

This package is experimental and the API signature or its behavior may change
without notice.

## Differences from gomdb

- Zero-copy by default
- Some function signatures have been changed to be easier to use.
- TODO Serialized transactions safe for concurrent use.

#Build

`go get github.com/bmatsuo/lmdb.exp`

There is no dependency on LMDB dynamic library.

On FreeBSD 10, you must explicitly set `CC` (otherwise it will fail with a cryptic error), for example:

`CC=clang go test -v`

#Documentation

The best source of documentation is the official LMDB C API documentation
reachable through the LMDB [homepage](http://symas.com/mdb/)

Documentation specific to the Go bindings and how methods differ from their
underlying C counterparts can be found on
[godoc.org](http://godoc.org/github.com/bmatsuo/lmdb.exp).

#TODO

- identify missing functions that deserve to be included
- documentation
- tests
