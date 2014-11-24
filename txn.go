package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"fmt"
	"math"
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
// WARNING: A writable Txn is not threadsafe and may only be used in the
// goroutine that created it.  To serialize concurrent access to a read-write
// transaction use a WriteTxn.
//
// See MDB_txn.
type Txn struct {
	env     *Env
	_txn    *C.MDB_txn
	managed bool
}

// beginTxn does not lock the OS thread which is a prerequisite for creating a
// write transaction.
func beginTxn(env *Env, parent *Txn, flags uint) (*Txn, error) {
	var _txn *C.MDB_txn
	var ptxn *C.MDB_txn
	if parent == nil {
		ptxn = nil
	} else {
		ptxn = parent._txn
	}
	ret := C.mdb_txn_begin(env._env, ptxn, C.uint(flags), &_txn)
	if ret != success {
		return nil, errno(ret)
	}
	txn := &Txn{
		env:  env,
		_txn: _txn,
	}
	return txn, nil
}

// Commit commits all operations of the transaction to the database.  A Txn
// cannot be used again after Commit is called.
//
// See mdb_txn_commit.
func (txn *Txn) Commit() error {
	if txn.managed {
		panic("managed transaction cannot be comitted directly")
	}
	return txn.commit()
}

func (txn *Txn) commit() error {
	ret := C.mdb_txn_commit(txn._txn)
	// The transaction handle is freed if there was no error
	if ret == success {
		txn._txn = nil
	}
	return errno(ret)
}

// Abort discards pending writes in the transaction.  A Txn cannot be used
// again after Abort is called.
//
// See mdb_txn_abort.
func (txn *Txn) Abort() {
	if txn.managed {
		panic("managed transaction cannot be aborted directly")
	}

	txn.abort()
}

func (txn *Txn) abort() {
	if txn._txn == nil {
		return
	}
	C.mdb_txn_abort(txn._txn)
	// The transaction handle is always freed.
	txn._txn = nil
}

// Reset aborts the transaction clears internal state so the transaction may be
// reused by calling Renew.  Reset panics if the transaction is managed by
// Update, View, etc.
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

// Renew reuses a transaction that was previously reset.
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
	return errno(ret)
}

// OpenDBI opens a database in the environment.  An error is returned if name is empty.
//
// BUG:
// DBI(math.NaN()) is returned on error which seems really wrong.
//
// See mdb_dbi_open.
func (txn *Txn) OpenDBI(name string, flags uint) (DBI, error) {
	if name == "" {
		return 0, fmt.Errorf("database name cannot be empty")
	}

	cname := C.CString(name)
	dbi, err := txn.openDBI(cname, flags)
	C.free(unsafe.Pointer(cname))
	return dbi, err
}

// CreateDBI is a shorthand for OpenDBI that passed the flag lmdb.Create.
func (txn *Txn) CreateDBI(name string) (DBI, error) {
	return txn.OpenDBI(name, Create)
}

// OpenRoot opens the root database.  Applications should not write to the root
// database if also using named databases as LMDB stores metadata in the root
// database.
func (txn *Txn) OpenRoot(flags uint) (DBI, error) {
	return txn.openDBI(nil, flags)
}

func (txn *Txn) openDBI(cname *C.char, flags uint) (DBI, error) {
	var dbi C.MDB_dbi
	ret := C.mdb_dbi_open(txn._txn, cname, C.uint(flags), &dbi)
	if ret != success {
		return DBI(math.NaN()), errno(ret)
	}
	return DBI(dbi), nil
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

// Sub executes fn in a subtransaction.  Sub commits the subtransaction iff no
// error is returned and otherwise aborts it.  Sub returns any error it
// encounters.
//
// Any call to Abort, Commit, Renew, or Reset on a Txn created by Sub will
// panic.
func (txn *Txn) Sub(fn ...TxnOp) error {
	// As of 0.9.14 Readonly is the only Txn flag and readonly subtransactions
	// don't make sense.
	return txn.subFlag(0, fn)
}

func (txn *Txn) subFlag(flags uint, fn []TxnOp) error {
	sub, err := beginTxn(txn.env, txn, flags)
	if err != nil {
		return err
	}
	sub.managed = true
	defer func() {
		if e := recover(); e != nil {
			sub.abort()
			panic(e)
		}
	}()
	for _, fn := range fn {
		err = fn(sub)
		if err != nil {
			sub.abort()
			return err
		}
	}
	return sub.commit()
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
func (txn *Txn) GetVal(dbi DBI, key []byte) (*Val, error) {
	ckey := Wrap(key)
	var cval Val
	ret := C.mdb_get(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(ckey), (*C.MDB_val)(&cval))
	err := errno(ret)
	if err != nil {
		return nil, err
	}
	return &cval, nil
}

// Put stores an item in database dbi.
//
// See mdb_put.
func (txn *Txn) Put(dbi DBI, key []byte, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_put(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(ckey), (*C.MDB_val)(cval), C.uint(flags))
	return errno(ret)
}

// Del deletes items from database dbi.
//
// See mdb_del.
func (txn *Txn) Del(dbi DBI, key, val []byte) error {
	ckey := Wrap(key)
	if val == nil {
		ret := C.mdb_del(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(ckey), nil)
		return errno(ret)
	}
	cval := Wrap(val)
	ret := C.mdb_del(txn._txn, C.MDB_dbi(dbi), (*C.MDB_val)(ckey), (*C.MDB_val)(cval))
	return errno(ret)
}

// OpenCursor allocates and initializes a Cursor to database dbi.
//
// See mdb_cursor_open.
func (txn *Txn) OpenCursor(dbi DBI) (*Cursor, error) {
	return openCursor(txn, dbi)
}

// TxnOp is an operation applied to a transaction.  The Txn passed to a TxnOp
// is managed and the operation must not call Commit, Abort, Renew, or Reset on
// it.
//
// IMPORTANT:
// TxnOps that write to the database (those passed to Update or BeginUpdate)
// must not use the Txn in another goroutine (passing it directory otherwise
// through closure). Doing so has undefined results.
type TxnOp func(txn *Txn) error
