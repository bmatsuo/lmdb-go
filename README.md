#lmdb-go [![releases/v1.2.0](https://img.shields.io/badge/release-v1.2.0-375eab.svg)](CHANGES.md) [![C/v0.9.16](https://img.shields.io/badge/C-v0.9.16-555555.svg)](https://github.com/LMDB/lmdb/blob/mdb.RE/0.9/libraries/liblmdb/CHANGES) [![Build Status](https://travis-ci.org/bmatsuo/lmdb-go.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb-go)

Go bindings to the OpenLDAP Lightning Memory-Mapped Database (LMDB).

## Packages

Functionality is logically divided into several packages.  Applications will
usually need to import **lmdb** but may import other packages on an as needed
basis.

Packages in the `exp/` directory are not stable and may change without warning.
That said, they are generally usable if application dependencies are managed
and pinned by tag/commit.

Developers concerned with package stability should consult the documentation.

####lmdb [![GoDoc](https://godoc.org/github.com/bmatsuo/lmdb-go/lmdb?status.svg)](https://godoc.org/github.com/bmatsuo/lmdb-go/lmdb) [![stable](https://img.shields.io/badge/stability-stable-brightgreen.svg)](#user-content-versioning-and-stability) [![GoCover](http://gocover.io/_badge/github.com/bmatsuo/lmdb-go/lmdb)](http://gocover.io/github.com/bmatsuo/lmdb-go/lmdb)

```go
import "github.com/bmatsuo/lmdb-go/lmdb"
```

Core bindings allowing low-level access to LMDB.

####exp/lmdbscan [![GoDoc](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbscan?status.svg)](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbscan) [![unstable](https://img.shields.io/badge/stability-unstable-orange.svg)](#user-content-versioning-and-stability) [![GoCover](http://gocover.io/_badge/github.com/bmatsuo/lmdb-go/exp/lmdbscan)](http://gocover.io/github.com/bmatsuo/lmdb-go/exp/lmdbscan)

```go
import "github.com/bmatsuo/lmdb-go/exp/lmdbscan"
```

A utility package for scanning database ranges with an API inspired by
[bufio.Scanner](https://godoc.org/bufio#Scanner).

The **lmdbscan** package is unstable. The API is properly scoped and adequately
tested.  And no features that exist now will be removed without a similar
substitute.  See the versioning documentation for more information.

####exp/lmdbsync [![GoDoc](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbsync?status.svg)](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbsync) [![experimental](https://img.shields.io/badge/stability-experimental-red.svg)](#user-content-versioning-and-stability) [![GoCover](http://gocover.io/_badge/github.com/bmatsuo/lmdb-go/exp/lmdbsync)](http://gocover.io/github.com/bmatsuo/lmdb-go/exp/lmdbsync)


```go
import "github.com/bmatsuo/lmdb-go/exp/lmdbsync"
```

An experimental utility package that provides synchronization necessary to
change an environment's map size after initialization.

The **lmdbsync** package is usable for synchronization but its resizing
behavior should be considered highly unstable and may change without notice
between releases.  Its use case is real but somewhat niche and requires much
more feedback driven development before it can be considered stable.

## Key Features

###Idiomatic API

API inspired by [BoltDB](https://github.com/boltdb/bolt) with automatic
commit/rollback of transactions.  The goal of lmdb-go is to provide idiomatic,
safe database interactions without compromising the flexibility of the C API.

###API coverage

The lmdb-go project aims for complete coverage of the LMDB C API (within
reason).  Some notable features and optimizations that are supported:

- Idiomatic subtransactions ("sub-updates") that do not disrupt thread locking.

- Batch IO on databases utilizing the `MDB_DUPSORT` and `MDB_DUPFIXED` flags.

- Reserved writes than can save in memory copies coverting/buffering into `[]byte`.

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

Building commands and running tests can be done with `go` or with `make`

    make bin
    make test
    make check
    make all

#Documentation

##LMDB

The best source of documentation regarding the low-level usage of LMDB
environments is the official LMDB C API documentation reachable through the
LMDB [homepage](http://symas.com/mdb/).

##Godoc

The "godoc" documentation available on
[godoc.org](https://godoc.org/github.com/bmatsuo/lmdb-go) has all remaining
developer documentation for lmdb-go.  Godoc documentation covers specifics to
the Go bindings, how methods differ from their underlying C counterparts, and
lots of usage examples.

##Versioning and Stability

The lmdb-go project makes regular releases with IDs `X.Y.Z`.  All packages
outside of the `exp/` directory are considered stable and adhere to the
guidelines of [semantic versioning](http://semver.org/).

Experimental packages (those packages in `exp/`) are not required to adhere to
semantic versioning.  However packages specifically declared to merely be
"unstable" can be relied on more for long term use with less concern.

The API of an unstable package may change in subtle ways between minor release
versions.  But all functionality will remain available through some method (at
least until the next major release version).

#Links

####[github.com/bmatsuo/raft-mdb](https://github.com/bmatsuo/raft-mdb) ([godoc](https://godoc.org/github.com/bmatsuo/raft-mdb))

An experimental backend for
[github.com/hashicorp/raft](https://github.com/hashicorp/raft) forked from
[github.com/hashicorp/raft-mdb](github.com/hashicorp/raft-mdb).

####[github.com/bmatsuo/cayley/graph/lmdb](https://github.com/bmatsuo/cayley/tree/master/graph/lmdb) ([godoc](https://godoc.org/github.com/bmatsuo/cayley/graph/lmdb))

Experimental backend quad-store for
[github.com/google/cayley](https://github.com/google/cayley) based off of the
BoltDB
[implementation](https://github.com/google/cayley/tree/master/graph/bolt).
