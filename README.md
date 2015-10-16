#lmdb-go [![Build Status](https://travis-ci.org/bmatsuo/lmdb-go.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb-go) [![GoDoc](https://godoc.org/github.com/bmatsuo/lmdb-go?status.svg)](https://godoc.org/github.com/bmatsuo/lmdb-go)

Go bindings to the OpenLDAP Lightning Memory-Mapped Database (LMDB).

## Key Features

###Idiomatic API

API inspired by [BoltDB](https://github.com/boltdb/bolt) with automatic
commit/rollback of transactions.  The goal of lmdb-go is to provide idiomatic,
safe database interactions without compromising the flexibility of the C API.

###API coverage

The lmdb-go project aims for *complete* feature coverage for LMDB.  Some
notable features:

- Subtransactions are fully supported.

- Batch IO on databases utilizing the `MDB_DUPSORT` and `MDB_DUPFIXED` flags.

- Reserved "put" for reduced memory copies.

For tracking purposes a list of unsupported features is kept in an
[issue](https://github.com/bmatsuo/lmdb-go/issues/1).

###Zero-copy reads

Applications with high performance requirements can opt-in to fast, zero-copy
reads at the cost of runtime safety.  Zero-copy behavior is specified at the
transaction level to reduce instrumentation overhead.

```
err := lmdb.View(func(txn *lmdb.Txn) error {
    // RawRead enables zero-copy behavior with some serious caveats.
    // Read the documentation carefully before using.
    txn.RawRead = true

    val, err := txn.Get(dbi, []byte("largevalue"), 0)
    // ...
})
```

###Documentation

Comprehensive documentation and examples are provided to demonstrate safe usage
of lmdb.  In addition to [godoc](https://godoc.org/github.com/bmatsuo/lmdb-go)
documentation, implementations of the standand LMDB commands (`mdb_stat`, etc)
can be found in the [cmd/](cmd/) directory and some simple experimental
commands can be found in the [exp/cmd/](exp/cmd) directory.  Aside from
providing minor utility these programs are provided as examples of lmdb in
practice.

#Build

There is no dependency on shared libraries.  So most users can simply install
using `go get`.

`go get github.com/bmatsuo/lmdb-go/lmdb`

On FreeBSD 10, you must explicitly set `CC` (otherwise it will fail with a
cryptic error), for example:

    CC=clang go test -v ./...

#Documentation

The best source of documentation is the official LMDB C API documentation
reachable through the LMDB [homepage](http://symas.com/mdb/).

Documentation specific to the Go bindings and how methods differ from their
underlying C counterparts can be found on
[godoc.org](https://godoc.org/github.com/bmatsuo/lmdb-go).
