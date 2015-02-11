#lmdb [![Build Status](https://travis-ci.org/bmatsuo/lmdb.exp.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb.exp)

Go bindings to the OpenLDAP Lightning Memory-Mapped Database (LMDB).

## Experimental

This package is experimental and the API signature or its behavior may change
without notice. Several questions remain:

- Should it be easier to initialize/open a database? (edit(bmatsuo): probably safe to punt)
- Should errors be abstracted more to provide better diagnostics?
- Is the `Cursor.PutMulti()` signature the right balance of performance and
  usability?
- Is \[Get\]Val necessary for export? (edit(bmatsuo): probably not initially)

## Key Features

- Zero-copy by default
- API inspired by [BoltDB](https://github.com/boltdb/bolt)
- A full-featured LMDB bindings with comprehensive examples
- Serialized transactions safe for concurrent use

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
