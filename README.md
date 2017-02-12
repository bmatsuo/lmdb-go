#lmdb-go [![releases/v1.8.0](https://img.shields.io/badge/release-v1.8.0-375eab.svg)](releases) [![C/v0.9.19](https://img.shields.io/badge/C-v0.9.19-555555.svg)](https://github.com/LMDB/lmdb/blob/mdb.RE/0.9/libraries/liblmdb/CHANGES) [![Build Status](https://travis-ci.org/bmatsuo/lmdb-go.svg?branch=master)](https://travis-ci.org/bmatsuo/lmdb-go)

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

####lmdbscan [![GoDoc](https://godoc.org/github.com/bmatsuo/lmdb-go/lmdbscan?status.svg)](https://godoc.org/github.com/bmatsuo/lmdb-go/lmdbscan) [![stable](https://img.shields.io/badge/stability-stable-brightgreen.svg)](#user-content-versioning-and-stability) [![GoCover](http://gocover.io/_badge/github.com/bmatsuo/lmdb-go/lmdbscan)](http://gocover.io/github.com/bmatsuo/lmdb-go/lmdbscan)

```go
import "github.com/bmatsuo/lmdb-go/lmdbscan"
```

A utility package for scanning database ranges. The API is inspired by
[bufio.Scanner](https://godoc.org/bufio#Scanner) and the python cursor
[implementation](https://lmdb.readthedocs.org/en/release/#cursor-class).

####exp/lmdbsync [![GoDoc](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbsync?status.svg)](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbsync) [![experimental](https://img.shields.io/badge/stability-experimental-red.svg)](#user-content-versioning-and-stability) [![GoCover](http://gocover.io/_badge/github.com/bmatsuo/lmdb-go/exp/lmdbsync)](http://gocover.io/github.com/bmatsuo/lmdb-go/exp/lmdbsync)


```go
import "github.com/bmatsuo/lmdb-go/exp/lmdbsync"
```

An experimental utility package that provides synchronization necessary to
change an environment's map size after initialization.  The package provides
error handlers to automatically manage database size and retry failed
transactions.

The **lmdbsync** package is usable but the implementation of Handlers are
unstable and may change in incompatible ways without notice.  The use cases of
dynamic map sizes and multiprocessing are niche and the package requires much
more development driven by practical feedback before the Handler API and the
provided implementations can be considered stable.

## Key Features

###Idiomatic API

API inspired by [BoltDB](https://github.com/boltdb/bolt) with automatic
commit/rollback of transactions.  The goal of lmdb-go is to provide idiomatic
database interactions without compromising the flexibility of the C API.

**NOTE:** While the lmdb package tries hard to make LMDB as easy to use as
possible there are compromises, gotchas, and caveats that application
developers must be aware of when relying on LMDB to store their data.  All
users are encouraged to fully read the
[documentation](https://godoc.org/github.com/bmatsuo/lmdb-go/lmdb) so they are
aware of these caveats.

Where the lmdb package and its implementation decisions do not meet the needs
of application developers in terms of safety or operational use the lmdbsync
package has been designed to wrap lmdb and safely fill in additional
functionality.  Consult the
[documentation](https://godoc.org/github.com/bmatsuo/lmdb-go/exp/lmdbsync) for
more information about the lmdbsync package.

###API coverage

The lmdb-go project aims for complete coverage of the LMDB C API (within
reason).  Some notable features and optimizations that are supported:

- Idiomatic subtransactions ("sub-updates") that allow the batching of updates.

- Batch IO on databases utilizing the `MDB_DUPSORT` and `MDB_DUPFIXED` flags.

- Reserved writes than can save in memory copies converting/buffering into
  `[]byte`.

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

##LMDB compared to BoltDB

BoltDB is a quality database with a design similar to LMDB.  Both store
key-value data in a file and provide ACID transactions.  So there are often
questions of why to use one database or the other.

###Advantages of BoltDB

- Nested databases allow for hierarchical data organization.

- Far more databases can be accessed concurrently.

- Operating systems that do not support sparse files do not use up excessive
  space due to a large pre-allocation of file space.  The exp/lmdbsync package
  is intended to resolve this problem with LMDB but it is not ready.

- As a pure Go package bolt can be easily cross-compiled using the `go`
  toolchain and `GOOS`/`GOARCH` variables.

- Its simpler design and implementation in pure Go mean it is free of many
  caveats and gotchas which are present using the lmdb package.  For more
  information about caveats with the lmdb package, consult its
  [documentation](https://godoc.org/github.com/bmatsuo/lmdb-go/lmdb).

###Advantages of LMDB

- Keys can contain multiple values using the DupSort flag.

- Updates can have sub-updates for atomic batching of changes.

- Databases typically remain open for the application lifetime.  This limits
  the number of concurrently accessible databases.  But, this minimizes the
  overhead of database accesses and typically produces cleaner code than
  an equivalent BoltDB implementation.

- Significantly faster than BoltDB.  The raw speed of LMDB easily surpasses
  BoltDB.  Additionally, LMDB provides optimizations ranging from safe,
  feature-specific optimizations to generally unsafe, extremely situational
  ones.  Applications are free to enable any optimizations that fit their data,
  access, and reliability models.

- LMDB allows multiple applications to access a database simultaneously.
  Updates from concurrent processes are synchronized using a database lock
  file.

- As a C library, applications in any language can interact with LMDB
  databases.  Mission critical Go applications can use a database while Python
  scripts perform analysis on the side.

##Build

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

On Linux, you can specify the `pwritev` build tag to reduce the number of syscalls
required when committing a transaction. In your own package you can then do

    go build -tags pwritev .

to enable the optimisation.

##Documentation

###Go doc

The `go doc` documentation available on
[godoc.org](https://godoc.org/github.com/bmatsuo/lmdb-go) is the primary source
of developer documentation for lmdb-go.  It provides an overview of the API
with a lot of usage examples.  Where necessary the documentation points out
differences between the semantics of methods and their C counterparts.

###LMDB

The LMDB [homepage](http://symas.com/mdb/) and mailing list
([archives](http://www.openldap.org/lists/openldap-technical/)) are the
official source of documentation regarding low-level LMDB operation and
internals.

Along with an API reference LMDB provides a high-level
[summary](http://symas.com/mdb/doc/starting.html) of the library.  While
lmdb-go abstracts many of the thread and transaction details by default the
rest of the guide is still useful to compare with `go doc`.

###Versioning and Stability

The lmdb-go project makes regular releases with IDs `X.Y.Z`.  All packages
outside of the `exp/` directory are considered stable and adhere to the
guidelines of [semantic versioning](http://semver.org/).

Experimental packages (those packages in `exp/`) are not required to adhere to
semantic versioning.  However packages specifically declared to merely be
"unstable" can be relied on more for long term use with less concern.

The API of an unstable package may change in subtle ways between minor release
versions.  But deprecations will be indicated at least one release in advance
and all functionality will remain available through some method.

##License

Except where otherwise noted files in the lmdb-go project are licensed under
the MIT open source license.

The LMDB C source is licensed under the OpenLDAP Public License.

##Links

####[github.com/bmatsuo/raft-mdb](https://github.com/bmatsuo/raft-mdb) ([godoc](https://godoc.org/github.com/bmatsuo/raft-mdb))

An experimental backend for
[github.com/hashicorp/raft](https://github.com/hashicorp/raft) forked from
[github.com/hashicorp/raft-mdb](https://github.com/hashicorp/raft-mdb).

####[github.com/bmatsuo/cayley/graph/lmdb](https://github.com/bmatsuo/cayley/tree/master/graph/lmdb) ([godoc](https://godoc.org/github.com/bmatsuo/cayley/graph/lmdb))

Experimental backend quad-store for
[github.com/google/cayley](https://github.com/google/cayley) based off of the
BoltDB
[implementation](https://github.com/google/cayley/tree/master/graph/bolt).
