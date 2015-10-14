/*
Package lmdb provides bindings to the lmdb C API.  The package bindings are
fairly low level and are designed to provide a minimal interface that prevents
misuse to a reasonable extent.  When in doubt refer to the C documentation as a
reference.

	http://symas.com/mdb/doc/group__mdb.html

Environment

An LMDB environment holds named databases (key-value stores).  An environment
is represented as one file on the filesystem (though often a corresponding lock
file exists).

LMDB recommends setting an environment's size as large as possible at the time
of creation.  On filesystems that support sparse files this should not
adversely affect disk usage.  Resizing an environment is possible but must be
handled with care when concurrent access is involved.

Databases

A database in an LMDB environment is an ordered key-value store that holds
arbitrary binary data.  Typically the keys are unique but duplicate keys may be
allowed (DupSort), in which case the values for each duplicate key are ordered.

A single LMDB environment can have multiple named databases.  But there is also
a 'root' (unnamed) database that can be used to store data.  Use caution
storing data in the root database when named databases are in use.  The root
database serves as an index for named databases.

A database is referenced by an opaque handle known as its DBI which must be
opened inside a transaction with the OpenDBI or OpenRoot methods.  DBIs may be
closed but it is not required.  Typically, applications aquire handles for all
their databases immediately after opening an environment and retain them for
the lifetime of the process.

Transactions

View (readonly) transactions in LMDB operate on a snapshot of the database at
the time the transaction began.  The number of simultaneously active view
transactions is bounded and configured when the environment is initialized.

Update (read-write) transactions are serialized in LMDB.  Attempts to create
update transactions block until a lock may be obtained.  Update transactions
can create subtransactions which may be rolled back independently from their
parent.

The lmdb package supplies managed and unmanaged transactions. Managed
transactions do not require explicit calling of Abort/Commit and are provided
through the Env methods Update, View, and RunTxn.  The BeginTxn method on Env
creates an unmanaged transaction but its use is not advised in most
applications.
*/
package lmdb

/*
#cgo CFLAGS: -pthread -W -Wall -Wno-unused-parameter -Wbad-function-cast -O2 -g
#cgo freebsd CFLAGS: -DMDB_DSYNC=O_SYNC
#cgo openbsd CFLAGS: -DMDB_DSYNC=O_SYNC
#cgo netbsd CFLAGS: -DMDB_DSYNC=O_SYNC

#include "lmdb.h"
#include "lmdbgo.h"
*/
import "C"
import "unsafe"

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

type msgCtx struct {
	fn  msgfunc
	err error
}
type msgfunc func(string) error

func newMsgCtx(fn msgfunc) *msgCtx { return &msgCtx{fn: fn} }

//export lmdbgo_mdb_msg_func_bridge
func lmdbgo_mdb_msg_func_bridge(msg C.lmdbgo_ConstCString, _ctx unsafe.Pointer) C.int {
	ctx := (*msgCtx)(_ctx)
	fn := ctx.fn
	if fn == nil {
		return 0
	}

	err := fn(C.GoString(msg.p))
	if err != nil {
		ctx.err = err
		return -1
	}
	return 0
}
