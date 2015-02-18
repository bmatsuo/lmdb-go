/*
Package lmdb provides bindings to the lmdb C API.  The package bindings are
fairly low level and are designed to provide a minimal interface that prevents
misuse to a reasonable extent.  When in doubt refer to the C documentation as a
reference.

	http://symas.com/mdb/doc/group__mdb.html

Experimental

This package is experimental and the API is in flux while core development is
being done and the spartan but volumnous LMDB docs are being completly
deciphered.

Environment

An LMDB environment holds named databases (key-value stores).  An environment
is represented as one file on the filesystem (though often a corresponding lock
file exists).

LMDB recommends setting an environment's size as lange as possible when it is
created.  On filesystems that support sparse file this should not be a problem.
Resizing an environment is possible but is difficult to manage with concurrent
assess patterns, especially with if long-running transactions are involved.

Databases

A database in an LMDB environment is an ordered key-value store for data blobs.
Typically the keys are unique but duplicate keys may be allowed (DupSort), in
which case the values for each duplicate key are ordered.

There is a 'root' (unnamed) database that can be used to store data.  Use
caution storing data in the root database when named database are in use.  The
root database serves as an index for named databases.

A database is referenced by an opaque handle known as its DBI.  A single LMDB
environment can have multiple named databases.

DBIs may be closed but it is not required.  Typically, applications aquire
handles for all their databases immediately after opening an environment and
retain them for the lifetime of the process.

Transactions

View (readonly) transactions in LMDB operate on a snapshot of the database at
the time the transaction began.  The number of simultaneously active view
transactions is bounded and configured when the environment is initialized.

LMDB allows only one update (read-write) transaction to be active at a time.
Attempts to create write transactions will block until no other write
transactions are active.

LMDB allows update transactions to have subtransactions which may be aborted
and rolled back without aborting their parent.  Transactions cannot be used
while they have an active subtransaction.

The lmdb package supplies managed, and unmanaged transaction types. Managed
transactions do not require explicit calling of Abort/Commit and are provided
through Env methods Update, View, and RunTxn.  The BeginTxn method on Env
creates an unmanaged transaction but its use is strongly advised against in
most applications.
*/
package lmdb

/*
#cgo CFLAGS: -pthread -W -Wall -Wno-unused-parameter -Wbad-function-cast -O2 -g
#cgo freebsd CFLAGS: -DMDB_DSYNC=O_SYNC
#cgo openbsd CFLAGS: -DMDB_DSYNC=O_SYNC
#cgo netbsd CFLAGS: -DMDB_DSYNC=O_SYNC

#include "lmdb.h"
*/
import "C"

import (
	"fmt"
	"syscall"
)

// Errno represents the class of error defined by LMDB.  The only values of
// type Errno returned by the API are those declared as constants in this
// package.  Other errors resulting from non-zero errno values will be of type
// syscall.Errno.
type Errno C.int

// minimum and maximum values produced for the Errno type. syscall.Errnos of
// other values may still be produced.
const minErrno, maxErrno C.int = C.MDB_KEYEXIST, C.MDB_LAST_ERRCODE

func (e Errno) Error() string {
	s := C.GoString(C.mdb_strerror(C.int(e)))
	if s == "" {
		return fmt.Sprint("mdb errno:", int(e))
	}
	return s
}

// _errno is for use by tests that can't import C
func _errno(ret int) error {
	return errno(C.int(ret))
}

// errno transforms an integer returned by the LMDB C API into an error value,
// which may be nil.
func errno(ret C.int) error {
	if ret == C.MDB_SUCCESS {
		return nil
	}
	if minErrno <= ret && ret <= maxErrno {
		return Errno(ret)
	}
	return syscall.Errno(ret)
}

// The set of error codes defined by LMDB are typed constants.
// See the list of LMDB return codes for more information
//
//		http://symas.com/mdb/doc/group__errors.html
const (
	ErrKeyExist        Errno = C.MDB_KEYEXIST
	ErrNotFound        Errno = C.MDB_NOTFOUND
	ErrPageNotFound    Errno = C.MDB_PAGE_NOTFOUND
	ErrCorrupted       Errno = C.MDB_CORRUPTED
	ErrPanic           Errno = C.MDB_PANIC
	ErrVersionMismatch Errno = C.MDB_VERSION_MISMATCH
	ErrInvalid         Errno = C.MDB_INVALID
	ErrMapFull         Errno = C.MDB_MAP_FULL
	ErrDBsFull         Errno = C.MDB_DBS_FULL
	ErrReadersFull     Errno = C.MDB_READERS_FULL
	ErrTLSFull         Errno = C.MDB_TLS_FULL
	ErrTxnFull         Errno = C.MDB_TXN_FULL
	ErrCursorFull      Errno = C.MDB_CURSOR_FULL
	ErrPageFull        Errno = C.MDB_PAGE_FULL
	ErrMapResized      Errno = C.MDB_MAP_RESIZED
	ErrIncompatibile   Errno = C.MDB_INCOMPATIBLE
)

// Version return the major, minor, and patch version numbers of the LMDB C
// library and a string representation of the version.
//
// See mdb_version.
func Version() (major, minor, patch int, s string) {
	var maj, min, pat C.int
	ver_str := C.mdb_version(&maj, &min, &pat)
	return int(maj), int(min), int(pat), C.GoString(ver_str)
}

// VersionString returns a string representation of the LMDB C library version.
//
// See mdb_version.
func VersionString() string {
	var maj, min, pat C.int
	ver_str := C.mdb_version(&maj, &min, &pat)
	return C.GoString(ver_str)
}

func cbool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}
