#lmdb [![Build Status](https://travis-ci.org/bmatsuo/lmdb-go.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb-go)

Go bindings to the OpenLDAP Lightning Memory-Mapped Database (LMDB).

## Experimental

This package is experimental and the API signature and behavior may change
without notice. Several questions remain:

- The Reset/Renew methods probably need tests that they are usable. Benchmarks
  would be good as well. They might be removed for now.
- How are runtime panics handled inside transaction goroutines?

## Key Features

- Zero-copy by default
- API inspired by [BoltDB](https://github.com/boltdb/bolt)
- A full-featured LMDB bindings with comprehensive examples
- Serialized transactions safe for concurrent use

#Build

`go get github.com/bmatsuo/lmdb-go/lmdb`

There is no dependency on LMDB dynamic library.

On FreeBSD 10, you must explicitly set `CC` (otherwise it will fail with a cryptic error), for example:

`CC=clang go test -v`

#Documentation

The best source of documentation is the official LMDB C API documentation
reachable through the LMDB [homepage](http://symas.com/mdb/)

Documentation specific to the Go bindings and how methods differ from their
underlying C counterparts can be found on
[godoc.org](http://godoc.org/github.com/bmatsuo/lmdb.exp).
