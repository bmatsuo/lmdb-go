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
