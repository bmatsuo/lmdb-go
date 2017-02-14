/*
Package lmdb provides bindings to the lmdb C API.  The package bindings are
fairly low level and are designed to provide a minimal interface that prevents
misuse to a reasonable extent.  When in doubt refer to the C documentation as a
reference.

	http://www.lmdb.tech/doc/
	http://www.lmdb.tech/doc/starting.html
	http://www.lmdb.tech/doc/modules.html


Environment

An LMDB environment holds named databases (key-value stores).  An environment
is represented as one file on the filesystem (though often a corresponding lock
file exists).

LMDB recommends setting an environment's size as large as possible at the time
of creation.  On filesystems that support sparse files this should not
adversely affect disk usage.  Resizing an environment is possible but must be
handled with care when concurrent access is involved.

Note that the package lmdb forces all Env objects to be opened with the NoTLS
(MDB_NOTLS) flag.  Without this flag LMDB would not be practically usable in Go
(in the author's opinion).  However, even for environments opened with this
flag there are caveats regarding how transactions are used (see Caveats below).


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
closed but it is not required.  Typically, applications acquire handles for all
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


Caveats

Write transactions (those created without the Readonly flag) must be created in
a goroutine that has been locked to its thread by calling the function
runtime.LockOSThread.  Futhermore, all methods on such transactions must be
called from the goroutine which created them.  This is a fundamental limitation
of LMDB even when using the NoTLS flag (which the package always uses).  The
Env.Update method assists the programmer by calling runtime.LockOSThread
automatically but it cannot sufficiently abstract write transactions to make
them completely safe in Go.

A goroutine must never create a write transaction if the application programmer
cannot determine whether the goroutine is locked to an OS thread.  This is a
consequence of goroutine restrictions on write transactions and limitations in
the runtime's thread locking implementation.  In such situations updates
desired by the goroutine in question must be proxied by a goroutine with a
known state (i.e.  "locked" or "unlocked").  See the included examples for more
details about dealing with such situations.

Integer Values

The IntegerKey and IntegerDup flags on databases allow LMDB to store C.uint and
C.size_t data directly, sorted using standard integer comparison.  This is a
performance optimization that avoids otherwise necessary serialization overhead
storing unsigned integer data to ensure that keys, and values for duplicate
keys in the case of DupSort, retain the correct ordering when using byte-wise
comparison.

Before committing your application to the use of integer values there are
downsides to consider, particularly concerning application portability.  The
acceptable values for the C.uint and C.size_t types are platform dependent.  As
such, when using these flags it is easy to write application code which
contains built-in assumptions about the target architecture.  Where an
application which serializes unsigned integer data itself will deal with
explicitly sized types like uint32 and uint64 and more naturally write their
application in a way which is portable across architectures.

Applications that have been written to serialize unsigned integer data as
big-endian byte slices should consider benchmarking there applications before
committing to the use of integer values for their application.  It is likely
that applications will not notice a significant performance change unless
operating on database with a large number of entries.

When retrieving integer data from a database the package lmdb does not know
what type of value you are retrieving.  Because of this everything is returned
as a raw []byte.  In order to extract integer values package lmdb provides the
developer with safe conversion functions which are listed below for reference.

	ValueU
	ValueX
	ValueZ
	ValueBU
	ValueBX
	ValueBZ
	ValueUB
	ValueUU
	ValueUX
	ValueUZ
	ValueXB
	ValueXU
	ValueXX
	ValueXZ
	ValueZB
	ValueZU
	ValueZX
	ValueZZ

The suffix on the conversion function denotes the types of value(s) extracted
from []byte data and the length of the suffix denotes the number of []byte
arguments accepted converted.  The character 'U' in a suffix means the
conversion function extracts a C.uint value as a uint.  The character 'Z' in a
suffix means the function extracts a C.size_t value as a uintptr.  The
character 'X' in a suffix means that the function will extract either C.uint or
C.size_t based on the size of the input []byte.

Conversion functions with a single-letter suffix (e.g.  ValueU), take a single
[]byte value with an error and can safely extract integers from the result of
Txn.Get.  So, ValueU will take ([]byte, error) arguments and return (uint,
error) results.

Conversion functions with a two-letter suffix (e.g.  ValueZU) take two []byte
values with an error and can safely extract integers the result of Cursor.Get.
So, ValueZU will take ([]byte, []byte, error) arguments and return (uintptr,
uint, error) results.  These three-argument conversion functions also have
special variants using the character 'B' in their suffix which signifies that
the corresponding []byte argument will be returned as it is given.  So an
application that with a database using only the flag IntegerKey might call
Cursor.Get and pass its results to ValueZB (or ValueUB).

	id, data, err := lmdb.ValueZB(cursor.Get(nil, nil, lmdb.First))

The id variable above will have type uintptr while data will be the bytes
returned by cursor.Get, unmodified.
*/
package lmdb

/*
#cgo CFLAGS: -pthread -W -Wall -Wno-unused-parameter -Wno-format-extra-args -Wbad-function-cast -Wno-missing-field-initializers -O2 -g
#cgo linux,pwritev CFLAGS: -DMDB_USE_PWRITEV

#include "lmdb.h"
*/
import "C"

// Version return the major, minor, and patch version numbers of the LMDB C
// library and a string representation of the version.
//
// See mdb_version.
func Version() (major, minor, patch int, s string) {
	var maj, min, pat C.int
	verstr := C.mdb_version(&maj, &min, &pat)
	return int(maj), int(min), int(pat), C.GoString(verstr)
}

// VersionString returns a string representation of the LMDB C library version.
//
// See mdb_version.
func VersionString() string {
	var maj, min, pat C.int
	verstr := C.mdb_version(&maj, &min, &pat)
	return C.GoString(verstr)
}

func cbool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}
