package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"math"
	"runtime"
	"unsafe"
)

// Database Flags for OpenDBI.
const (
	ReverseKey = C.MDB_REVERSEKEY // use reverse string keys
	DupSort    = C.MDB_DUPSORT    // use sorted duplicates
	IntegerKey = C.MDB_INTEGERKEY // numeric keys in native byte order. The keys must all be of the same size.
	DupFixed   = C.MDB_DUPFIXED   // with DUPSORT, sorted dup items have fixed size
	IntegerDup = C.MDB_INTEGERDUP // with DUPSORT, dups are numeric in native byte order
	ReverseDup = C.MDB_REVERSEDUP // with DUPSORT, use reverse string dups
	Create     = C.MDB_CREATE     // create DB if not already existing
)

// Txn is a database transaction in an environment.
//
// BUG: Using write transactions multiple goroutines has undefined results.
//
// See MDB_txn.
type Txn struct {
	iswrite bool
	unsafe  *unsafeTxn
}

func beginTxn(env *Env, parent *Txn, flags uint) (*Txn, error) {
	var _txn *C.MDB_txn
	var ptxn *C.MDB_txn
	if parent == nil {
		ptxn = nil
	} else {
		ptxn = parent._txn
	}
	if flags&Readonly == 0 {
		runtime.LockOSThread()
	}
	ret := C.mdb_txn_begin(env._env, ptxn, C.uint(flags), &_txn)
	if ret != success {
		runtime.UnlockOSThread()
		return nil, errno(ret)
	}
	return &Txn{_txn}, nil
}

// Commit commits all operations of the transaction to the database.
//
// See mdb_txn_commit.
func (txn *Txn) Commit() error {
	ret := C.mdb_txn_commit(txn._txn)
	runtime.UnlockOSThread()
	// The transaction handle is freed if there was no error
	if ret == success {
		txn._txn = nil
	}
	return errno(ret)
}

// Abort abandons operations of a transaction and does not persist them.
//
// See mdb_txn_abort.
func (txn *Txn) Abort() {
	if txn._txn == nil {
		return
	}
	C.mdb_txn_abort(txn._txn)
	runtime.UnlockOSThread()
	// The transaction handle is always freed.
	txn._txn = nil
}

// Reset aborts the transaction clears internal state so the transaction may be
// reused by calling Renew.
//
// See mdb_txn_reset.
func (txn *Txn) Reset() {
	C.mdb_txn_reset(txn._txn)
}

// Renew reuses a transaction that was previously reset.
//
// See mdb_txn_renew.
func (txn *Txn) Renew() error {
	ret := C.mdb_txn_renew(txn._txn)
	return errno(ret)
}

// Open opens a database in the environment.  If name is empty Open opens the
// default database.
//
// TODO: Add an example showing potential problems using the default database.
//
// See mdb_dbi_open.
func (txn *Txn) OpenDBI(name string, flags uint) (DBI, error) {
	var _dbi C.MDB_dbi
	var cname *C.char
	if name != "" {
		cname = C.CString(name)
		defer C.free(unsafe.Pointer(cname))
	}
	ret := C.mdb_dbi_open(txn._txn, cname, C.uint(flags), &_dbi)
	if ret != success {
		return DBI(math.NaN()), errno(ret)
	}
	return DBI(_dbi), nil
}

// Stat returns statistics for database handle dbi.
//
// See mdb_stat.
func (txn *Txn) Stat(dbi DBI) (*Stat, error) {
	var _stat C.MDB_stat
	ret := C.mdb_stat(txn._txn, C.MDB_dbi(dbi), &_stat)
	if ret != success {
		return nil, errno(ret)
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
	return errno(ret)
}

// Get retrieves items from database dbi.  The slice returned by Get references
// a readonly section of memory and attempts to mutate region the memory will
// result in a runtime panic.
//
// See mdb_get.
func (txn *Txn) Get(dbi DBI, key []byte) ([]byte, error) {
	val, err := txn.GetVal(dbi, key)
	if err != nil {
		return nil, err
	}
	return val.Bytes(), nil
}

// GetVal retrieves items from database dbi as a Val.
//
// See mdb_get.
func (txn *Txn) GetVal(dbi DBI, key []byte) (Val, error) {
	ckey := Wrap(key)
	var cval Val
	ret := C.mdb_get(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval))
	return cval, errno(ret)
}

// Put stores an item in database dbi.
//
// See mdb_put.
func (txn *Txn) Put(dbi DBI, key []byte, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_put(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval), C.uint(flags))
	return errno(ret)
}

// Del deletes items from database dbi.
//
// See mdb_del.
func (txn *Txn) Del(dbi DBI, key, val []byte) error {
	ckey := Wrap(key)
	if val == nil {
		ret := C.mdb_del(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), nil)
		return errno(ret)
	}
	cval := Wrap(val)
	ret := C.mdb_del(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval))
	return errno(ret)
}

// OpenCursor allocates and initializes a Cursor to database dbi.
//
// See mdb_cursor_open.
func (txn *Txn) OpenCursor(dbi DBI) (*Cursor, error) {
	return openCursor(txn, dbi)
}

// unsafeTxn is a database transaction in an environment.
//
// BUG: Using write transactions multiple goroutines has undefined results.
//
// See MDB_txn.
type unsafeTxn struct {
	_txn *C.MDB_txn
}

func beginUnsafeTxn(env *Env, parent *unsafeTxn, flags uint) (*unsafeTxn, error) {
	var _txn *C.MDB_txn
	var ptxn *C.MDB_txn
	if parent == nil {
		ptxn = nil
	} else {
		ptxn = parent._txn
	}
	if flags&Readonly == 0 {
		runtime.LockOSThread()
	}
	ret := C.mdb_txn_begin(env._env, ptxn, C.uint(flags), &_txn)
	if ret != success {
		runtime.UnlockOSThread()
		return nil, errno(ret)
	}
	return &unsafeTxn{_txn}, nil
}

// Commit commits all operations of the transaction to the database.
//
// See mdb_txn_commit.
func (txn *unsafeTxn) Commit() error {
	ret := C.mdb_txn_commit(txn._txn)
	runtime.UnlockOSThread()
	// The transaction handle is freed if there was no error
	if ret == success {
		txn._txn = nil
	}
	return errno(ret)
}

// Abort abandons operations of a transaction and does not persist them.
//
// See mdb_txn_abort.
func (txn *unsafeTxn) Abort() {
	if txn._txn == nil {
		return
	}
	C.mdb_txn_abort(txn._txn)
	runtime.UnlockOSThread()
	// The transaction handle is always freed.
	txn._txn = nil
}

// Reset aborts the transaction clears internal state so the transaction may be
// reused by calling Renew.
//
// See mdb_txn_reset.
func (txn *unsafeTxn) Reset() {
	C.mdb_txn_reset(txn._txn)
}

// Renew reuses a transaction that was previously reset.
//
// See mdb_txn_renew.
func (txn *unsafeTxn) Renew() error {
	ret := C.mdb_txn_renew(txn._txn)
	return errno(ret)
}

// Open opens a database in the environment.  If name is empty Open opens the
// default database.
//
// TODO: Add an example showing potential problems using the default database.
//
// See mdb_dbi_open.
func (txn *unsafeTxn) OpenDBI(name string, flags uint) (DBI, error) {
	var _dbi C.MDB_dbi
	var cname *C.char
	if name != "" {
		cname = C.CString(name)
		defer C.free(unsafe.Pointer(cname))
	}
	ret := C.mdb_dbi_open(txn._txn, cname, C.uint(flags), &_dbi)
	if ret != success {
		return DBI(math.NaN()), errno(ret)
	}
	return DBI(_dbi), nil
}

// Stat returns statistics for database handle dbi.
//
// See mdb_stat.
func (txn *unsafeTxn) Stat(dbi DBI) (*Stat, error) {
	var _stat C.MDB_stat
	ret := C.mdb_stat(txn._txn, C.MDB_dbi(dbi), &_stat)
	if ret != success {
		return nil, errno(ret)
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
func (txn *unsafeTxn) Drop(dbi DBI, del bool) error {
	ret := C.mdb_drop(txn._txn, C.MDB_dbi(dbi), cbool(del))
	return errno(ret)
}

// Get retrieves items from database dbi.  The slice returned by Get references
// a readonly section of memory and attempts to mutate region the memory will
// result in a runtime panic.
//
// See mdb_get.
func (txn *unsafeTxn) Get(dbi DBI, key []byte) ([]byte, error) {
	val, err := txn.GetVal(dbi, key)
	if err != nil {
		return nil, err
	}
	return val.Bytes(), nil
}

// GetVal retrieves items from database dbi as a Val.
//
// See mdb_get.
func (txn *unsafeTxn) GetVal(dbi DBI, key []byte) (*Val, error) {
	ckey := Wrap(key)
	var cval Val
	ret := C.mdb_get(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval))
	err := errno(ret)
	if err != nil {
		return nil, err
	}
	return &cval, nil
}

// Put stores an item in database dbi.
//
// See mdb_put.
func (txn *unsafeTxn) Put(dbi DBI, key []byte, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_put(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval), C.uint(flags))
	return errno(ret)
}

// Del deletes items from database dbi.
//
// See mdb_del.
func (txn *unsafeTxn) Del(dbi DBI, key, val []byte) error {
	ckey := Wrap(key)
	if val == nil {
		ret := C.mdb_del(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), nil)
		return errno(ret)
	}
	cval := Wrap(val)
	ret := C.mdb_del(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval))
	return errno(ret)
}

// OpenCursor allocates and initializes a Cursor to database dbi.
//
// See mdb_cursor_open.
func (txn *unsafeTxn) OpenCursor(dbi DBI) (*unsafeCursor, error) {
	return openUnsafeCursor(txn, dbi)
}
