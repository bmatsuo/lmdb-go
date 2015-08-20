#lmdb-go [![Build Status](https://travis-ci.org/bmatsuo/lmdb-go.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb-go)

Go bindings to the OpenLDAP Lightning Memory-Mapped Database (LMDB).

## Key Features

- Fast zero-copy reads for applications with high performance requirements.

- API inspired by [BoltDB](https://github.com/boltdb/bolt).  The goal of
  lmdb-go is to provide idiomatic interaction without compromising the
  flexibility of the C API.

- Full-featured bindings to the LMDB library with comprehensive examples.

#Build

`go get github.com/bmatsuo/lmdb-go/lmdb`

There is no dependency on LMDB dynamic library.

On FreeBSD 10, you must explicitly set `CC` (otherwise it will fail with a cryptic error), for example:

    CC=clang go test -v ./...

#Documentation

The best source of documentation is the official LMDB C API documentation
reachable through the LMDB [homepage](http://symas.com/mdb/)

Documentation specific to the Go bindings and how methods differ from their
underlying C counterparts can be found on
[godoc.org](http://godoc.org/github.com/bmatsuo/lmdb-go/lmdb).
