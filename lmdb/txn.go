package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
#include "lmdbgo.h"
*/
import "C"

import (
	"log"
	"runtime"
	"unsafe"
)

// This flags are used exclusively for Txn.OpenDBI and Txn.OpenRoot.  The
// Create flag must always be supplied when opening a non-root DBI for the
// first time.
//
// BUG(bmatsuo):
// MDB_INTEGERKEY and MDB_INTEGERDUP aren't usable. I'm not sure they would be
// faster with the cgo bridge.  They need to be tested and benchmarked.
const (
	// Flags for Txn.OpenDBI.

	ReverseKey = C.MDB_REVERSEKEY // Use reverse string keys.
	DupSort    = C.MDB_DUPSORT    // Use sorted duplicates.
	DupFixed   = C.MDB_DUPFIXED   // Duplicate items have a fixed size (DupSort).
	ReverseDup = C.MDB_REVERSEDUP // Reverse duplicate values (DupSort).
	Create     = C.MDB_CREATE     // Create DB if not already existing.
)

// Txn is a database transaction in an environment.
//
// WARNING: A writable Txn is not threadsafe and may only be used in the
// goroutine that created it.
//
// See MDB_txn.
type Txn struct {
	// If RawRead is true []byte values retrieved from Get() calls on the Txn
	// and its cursors will point directly into the memory-mapped structure.
	// Such slices will be readonly and must only be referenced wthin the
	// transaction's lifetime.
	RawRead bool

	// Pooled may be set to true while a Txn is stored in a sync.Pool, after
	// Txn.Reset reset has been called and before Txn.Renew.  This will keep
	// the Txn finalizer from unnecessarily warning the application about
	// finalizations.
	Pooled bool

	managed  bool
	readonly bool
	env      *Env
	_txn     *C.MDB_txn
	errLogf  func(format string, v ...interface{})
	key      *C.MDB_val
	val      *C.MDB_val
}

// beginTxn does not lock the OS thread which is a prerequisite for creating a
// write transaction.
func beginTxn(env *Env, parent *Txn, flags uint) (*Txn, error) {
	txn := &Txn{
		readonly: (flags&Readonly != 0),
		env:      env,
	}

	var ptxn *C.MDB_txn
	if parent == nil {
		if flags&Readonly == 0 {
			// In a write Txn we can use the shared, C-allocated key and value
			// allocated by env, and freed when it is closed.
			txn.key = env.ckey
			txn.val = env.cval
		} else {
			// It is not easy to share C.MDB_val values in this scenario unless
			// there is a synchronized pool involved, which will increase
			// overhead.  Further, allocating these values with C will add
			// overhead both here and when the values are freed.
			txn.key = new(C.MDB_val)
			txn.val = new(C.MDB_val)
		}
	} else {
		// Because parent Txn objects cannot be used while a sub-Txn is active
		// it is OK for them to share their C.MDB_val objects.
		ptxn = parent._txn
		txn.key = parent.key
		txn.val = parent.val
	}
	ret := C.mdb_txn_begin(env._env, ptxn, C.uint(flags), &txn._txn)
	if ret != success {
		return nil, operrno("mdb_txn_begin", ret)
	}
	return txn, nil
}

// ID returns the identifier for txn.  A view transaction identifier
// corresponds to the Env snapshot being viewed and may be shared with other
// view transactions.
//
// See mdb_txn_id.
func (txn *Txn) ID() uintptr {
	return uintptr(C.mdb_txn_id(txn._txn))
}

// RunOp executs fn with txn as an argument.  During the execution of fn no
// goroutine may call the Commit, Abort, Reset, and Renew methods on txn.
// RunOp returns the result of fn without any further action.  RunOp will not
// about txn if fn returns an error.
func (txn *Txn) RunOp(fn TxnOp, commit bool) error {
	if txn.managed {
		if commit {
			defer txn.abort()
		}
	} else {
		txn.managed = true
		defer func() {
			// Restoring txn.managed must be done in a deferred call otherwise
			// the caller may not be able to abort the transaction if a runtime
			// panic occurs (attempting to do so would cause another panic).
			txn.managed = false

			// It is significantly faster to abort in the same deferred call
			// that resets txn.managed, despite being less clean conceptually.
			if commit {
				txn.abort()
				return
			}
		}()
	}

	err := fn(txn)
	if commit && err == nil {
		return txn.commit()
	}
	return err
}

// Commit persists all transaction operations to the database and clears the
// finalizer on txn.  A Txn cannot be used again after Commit is called.
//
// See mdb_txn_commit.
func (txn *Txn) Commit() error {
	if txn.managed {
		panic("managed transaction cannot be committed directly")
	}

	runtime.SetFinalizer(txn, nil)
	return txn.commit()
}

func (txn *Txn) commit() error {
	ret := C.mdb_txn_commit(txn._txn)
	txn._txn = nil
	return operrno("mdb_txn_commit", ret)
}

// Abort discards pending writes in the transaction and clears the finalizer on
// txn.  A Txn cannot be used again after Abort is called.
//
// See mdb_txn_abort.
func (txn *Txn) Abort() {
	if txn.managed {
		panic("managed transaction cannot be aborted directly")
	}

	runtime.SetFinalizer(txn, nil)
	txn.abort()
}

func (txn *Txn) abort() {
	if txn._txn == nil {
		return
	}

	// Get a read-lock on the environment so we can abort txn if needed.
	// txn.env **should** terminate all readers otherwise when it closes.
	txn.env.closeLock.RLock()
	if txn.env._env != nil {
		C.mdb_txn_abort(txn._txn)
	}
	txn.env.closeLock.RUnlock()

	// Clear the C object to prevent any potential future use of the freed
	// pointer.
	txn._txn = nil
}

// Reset aborts the transaction clears internal state so the transaction may be
// reused by calling Renew.  If txn is not going to be reused txn.Abort() must
// be called to release its slot in the lock table and free its memory.  Reset
// panics if txn is managed by Update, View, etc.
//
// See mdb_txn_reset.
func (txn *Txn) Reset() {
	if txn.managed {
		panic("managed transaction cannot be reset directly")
	}

	txn.reset()
}

func (txn *Txn) reset() {
	C.mdb_txn_reset(txn._txn)
}

// Renew reuses a transaction that was previously reset by calling txn.Reset().
// Renew panics if txn is managed by Update, View, etc.
//
// See mdb_txn_renew.
func (txn *Txn) Renew() error {
	if txn.managed {
		panic("managed transaction cannot be renewed directly")
	}

	return txn.renew()
}

func (txn *Txn) renew() error {
	ret := C.mdb_txn_renew(txn._txn)
	return operrno("mdb_txn_renew", ret)
}

// OpenDBI opens a named database in the environment.  An error is returned if
// name is empty.  The DBI returned by OpenDBI can be used in other
// transactions but not before Txn has terminated.
//
// OpenDBI can only be called after env.SetMaxDBs() has been called to set the
// maximum number of named databases.
//
// The C API uses null terminated strings for database names.  A consequence is
// that names cannot contain null bytes themselves. OpenDBI does not check for
// null bytes in the name argument.
//
// See mdb_dbi_open.
func (txn *Txn) OpenDBI(name string, flags uint) (DBI, error) {
	cname := C.CString(name)
	dbi, err := txn.openDBI(cname, flags)
	C.free(unsafe.Pointer(cname))
	return dbi, err
}

// CreateDBI is a shorthand for OpenDBI that passed the flag lmdb.Create.
func (txn *Txn) CreateDBI(name string) (DBI, error) {
	return txn.OpenDBI(name, Create)
}

// Flags returns the database flags for handle dbi.
func (txn *Txn) Flags(dbi DBI) (uint, error) {
	var cflags C.uint
	ret := C.mdb_dbi_flags(txn._txn, C.MDB_dbi(dbi), (*C.uint)(&cflags))
	return uint(cflags), operrno("mdb_dbi_flags", ret)
}

// OpenRoot opens the root database.  OpenRoot behaves similarly to OpenDBI but
// does not require env.SetMaxDBs() to be called beforehand.  And, OpenRoot can
// be called without flags in a View transaction.
func (txn *Txn) OpenRoot(flags uint) (DBI, error) {
	return txn.openDBI(nil, flags)
}

// openDBI returns returns whatever DBI value was set by mdb_open_dbi.  In an
// error case, LMDB does not currently set DBI in case of failure, so zero is
// returned in those cases.  This is not a big deal for now because
// applications are expected to handle any error encountered opening a
// database.
func (txn *Txn) openDBI(cname *C.char, flags uint) (DBI, error) {
	var dbi C.MDB_dbi
	ret := C.mdb_dbi_open(txn._txn, cname, C.uint(flags), &dbi)
	return DBI(dbi), operrno("mdb_dbi_open", ret)
}

// Stat returns a Stat describing the database dbi.
//
// See mdb_stat.
func (txn *Txn) Stat(dbi DBI) (*Stat, error) {
	var _stat C.MDB_stat
	ret := C.mdb_stat(txn._txn, C.MDB_dbi(dbi), &_stat)
	if ret != success {
		return nil, operrno("mdb_stat", ret)
	}
	stat := Stat{PSize: uint(_stat.ms_psize),
		Depth:         uint(_stat.ms_depth),
		BranchPages:   uint64(_stat.ms_branch_pages),
		LeafPages:     uint64(_stat.ms_leaf_pages),
		OverflowPages: uint64(_stat.ms_overflow_pages),
		Entries:       uint64(_stat.ms_entries)}
	return &stat, nil
}

// Drop empties the database if del is false.  Drop deletes and closes the
// database if del is true.
//
// See mdb_drop.
func (txn *Txn) Drop(dbi DBI, del bool) error {
	ret := C.mdb_drop(txn._txn, C.MDB_dbi(dbi), cbool(del))
	return operrno("mdb_drop", ret)
}

// Sub executes fn in a subtransaction.  Sub commits the subtransaction iff a
// nil error is returned by fn and otherwise aborts it.  Sub returns any error
// it encounters.
//
// Sub may only be called on an Update Txn (one created without the Readonly
// flag).  Calling Sub on a View transaction will return an error.  Sub assumes
// the calling goroutine is locked to an OS thread and will not call
// runtime.LockOSThread.
//
// Any call to Abort, Commit, Renew, or Reset on a Txn created by Sub will
// panic.
func (txn *Txn) Sub(fn TxnOp) error {
	// As of 0.9.14 Readonly is the only Txn flag and readonly subtransactions
	// don't make sense.
	return txn.subFlag(0, fn)
}

func (txn *Txn) subFlag(flags uint, fn TxnOp) error {
	sub, err := beginTxn(txn.env, txn, flags)
	if err != nil {
		return err
	}
	sub.managed = true
	defer sub.abort()
	err = fn(sub)
	if err != nil {
		return err
	}
	return sub.commit()
}

func (txn *Txn) bytes(val *C.MDB_val) []byte {
	if txn.RawRead {
		return getBytes(val)
	}
	return getBytesCopy(val)
}

// Get retrieves items from database dbi.  If txn.RawRead is true the slice
// returned by Get references a readonly section of memory that must not be
// accessed after txn has terminated.
//
// Returns ErrNotFound if the key is not present in dbi.
//
// See mdb_get.
func (txn *Txn) Get(dbi DBI, key []byte) ([]byte, error) {
	kdata, kn := valBytes(key)
	ret := C.lmdbgo_mdb_get(
		txn._txn, C.MDB_dbi(dbi),
		(*C.char)(unsafe.Pointer(&kdata[0])), C.size_t(kn),
		txn.val,
	)

	if ret == C.MDB_NOTFOUND {
		*txn.val = C.MDB_val{}
		return nil, ErrNotFound
	}

	err := operrno("mdb_get", ret)
	if err != nil {
		*txn.val = C.MDB_val{}
		return nil, err
	}
	b := txn.bytes(txn.val)
	*txn.val = C.MDB_val{}
	return b, nil
}

func (txn *Txn) putNilKey(dbi DBI, flags uint) error {
	// mdb_put with an empty key will always fail
	ret := C.lmdbgo_mdb_put2(txn._txn, C.MDB_dbi(dbi), nil, 0, nil, 0, C.uint(flags))
	return operrno("mdb_put", ret)
}

// Put stores an item in database dbi.
//
// See mdb_put.
func (txn *Txn) Put(dbi DBI, key []byte, val []byte, flags uint) error {
	kn := len(key)
	if kn == 0 {
		return txn.putNilKey(dbi, flags)
	}
	vn := len(val)
	if vn == 0 {
		val = []byte{0}
	}

	ret := C.lmdbgo_mdb_put2(
		txn._txn, C.MDB_dbi(dbi),
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(kn),
		(*C.char)(unsafe.Pointer(&val[0])), C.size_t(vn),
		C.uint(flags),
	)
	return operrno("mdb_put", ret)
}

// PutReserve returns a []byte of length n that can be written to, potentially
// avoiding a memcopy.  The returned byte slice is only valid in txn's thread,
// before it has terminated.
func (txn *Txn) PutReserve(dbi DBI, key []byte, n int, flags uint) ([]byte, error) {
	if len(key) == 0 {
		return nil, txn.putNilKey(dbi, flags)
	}
	txn.val.mv_size = C.size_t(n)
	ret := C.lmdbgo_mdb_put1(
		txn._txn, C.MDB_dbi(dbi),
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		txn.val,
		C.uint(flags|C.MDB_RESERVE),
	)
	err := operrno("mdb_put", ret)
	if err != nil {
		*txn.val = C.MDB_val{}
		return nil, err
	}
	b := getBytes(txn.val)
	*txn.val = C.MDB_val{}
	return b, nil
}

// Del deletes an item from database dbi.  Del ignores val unless dbi has the
// DupSort flag.
//
// Returns ErrNotFound if the key is not present in dbi.
//
// See mdb_del.
func (txn *Txn) Del(dbi DBI, key, val []byte) error {
	kdata, kn := valBytes(key)
	vdata, vn := valBytes(val)
	ret := C.lmdbgo_mdb_del(
		txn._txn, C.MDB_dbi(dbi),
		(*C.char)(unsafe.Pointer(&kdata[0])), C.size_t(kn),
		(*C.char)(unsafe.Pointer(&vdata[0])), C.size_t(vn),
	)

	if ret == C.MDB_NOTFOUND {
		return ErrNotFound
	}

	return operrno("mdb_del", ret)
}

// OpenCursor allocates and initializes a Cursor to database dbi.
//
// See mdb_cursor_open.
func (txn *Txn) OpenCursor(dbi DBI) (*Cursor, error) {
	cur, err := openCursor(txn, dbi)
	if cur != nil && txn.readonly {
		runtime.SetFinalizer(cur, (*Cursor).close)
	}
	return cur, err
}

func (txn *Txn) errf(format string, v ...interface{}) {
	if txn.errLogf != nil {
		txn.errLogf(format, v...)
		return
	}
	log.Printf(format, v...)
}

func (txn *Txn) finalize() {
	if txn._txn != nil {
		if !txn.Pooled {
			txn.errf("lmdb: aborting unreachable transaction %#x", uintptr(unsafe.Pointer(txn)))
		}

		txn.abort()
	}
}

// TxnOp is an operation applied to a managed transaction.  The Txn passed to a
// TxnOp is managed and the operation must not call Commit, Abort, Renew, or
// Reset on it.
//
// IMPORTANT:
// TxnOps that write to the database (those passed to Env.Update or Txn.Sub)
// must not use the Txn in another goroutine (passing it directly or otherwise
// through closure).  Doing so has undefined results.
type TxnOp func(txn *Txn) error
