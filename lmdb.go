/*
Package lmdb provides bindings to the lmdb C API.  The package bindings are
fairly low level and are designed to provide a minimal interface that prevents
misuse up to a reasonable extent.  In general the C documentation should be
used as reference.

	http://symas.com/mdb/doc/group__mdb.html

Experimental

This package is experimental and the API is in flux while core development is
being done and the spartan but volumnous LMDB docs are being completly
deciphered.

Transactions

Readonly transactions in LMDB operate on a snapshot of the database at the time
the transaction began.  The number of simultaneously active read transaction is
bounded and configured when the environment is initialized.

LMDB allows only one read-write transaction to be active at a time.  Attempts
to create write transactions will block until no others write transactions are
active.

LMDB allows read-write transactions to have subtransactions which may be
aborted and rolled back without aborting their parent.  Transactions must not
be used while they have an active subtransaction.

Errors

The errors returned by the package API will with few exceptions have type Errno
or syscall.Errno.  The only errors of type Errno returned are those defined in
lmdb.h, those which have constants defined in this package.  Other errno values
like EINVAL will by of type syscall.Errno.
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

// Version returns a string representation of the LMDB version.
//
// See mdb_version.
func Version() string {
	var major, minor, patch *C.int
	ver_str := C.mdb_version(major, minor, patch)
	return C.GoString(ver_str)
}

func cbool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}
