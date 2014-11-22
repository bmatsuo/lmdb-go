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
// WARNING: A writable Txn is not threadsafe and may only be used in the
// goroutine that created it.  To serialize concurrent access to a read-write
// transaction use a WriteTxn.
//
// See MDB_txn.
type Txn struct {
	env  *Env
	_txn *C.MDB_txn
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
	return &Txn{env, _txn}, nil
}

// Commit commits all operations of the transaction to the database.  A Txn
// cannot be used again after Commit is called.
//
// See mdb_txn_commit.
func (txn *Txn) Commit() error {
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
	if txn._txn == nil {
		return
	}
	C.mdb_txn_abort(txn._txn)
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

// Sub executes fn in a subtransaction.  Sub commits the subtransaction iff no
// error is returned.  Sub returns any error it encounters.
func (txn *Txn) Sub(fn ...TxnOp) error {
	return txn.subFlag(0, fn)
}

func (txn *Txn) subFlag(flags uint, fn []TxnOp) error {
	sub, err := beginTxn(txn.env, txn, flags)
	if err != nil {
		return err
	}
	defer func() {
		if e := recover(); e != nil {
			sub.Abort()
			panic(e)
		}
	}()
	for _, fn := range fn {
		err = fn(sub)
		if err != nil {
			sub.Abort()
			return err
		}
	}
	return sub.Commit()
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

// WriteTxn is a safe writable transaction.  All operations to the transaction
// are serialized a single goroutine (and single OS thread) for reliable
// execution behavior.
type WriteTxn struct {
	id      int
	env     *Env
	subchan chan writeNewSub
	opchan  chan writeOp
	commit  chan writeCommit
	abort   chan int
	closed  chan struct{}
}

func beginWriteTxn(env *Env, flags uint) (*WriteTxn, error) {
	wtxn := &WriteTxn{
		env:    env,
		opchan: make(chan writeOp),
		commit: make(chan writeCommit),
		abort:  make(chan int),
		closed: make(chan struct{}),
	}
	errc := make(chan error)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		txn, err := beginTxn(env, nil, flags)
		if err != nil {
			errc <- err
			return
		}
		close(errc)
		wtxn.loop(txn)
	}()
	err := <-errc
	if err != nil {
		return nil, err
	}
	return wtxn, nil
}

func (w *WriteTxn) loop(txn *Txn) {
	type IDTxn struct {
		id  int
		txn *Txn
	}
	txns := []IDTxn{{0, txn}}
	nextid := 1
	getTxn := func(id int) (*Txn, error) {
		current := txns[len(txns)-1]
		if current.id != id {
			for _, t := range txns {
				if t.id == id {
					return nil, fmt.Errorf("attempt to use transaction while a subtransaction is active")
				}
			}
			return nil, fmt.Errorf("attempt to use transaction after it was closed")
		}
		return current.txn, nil
	}
	defer close(w.closed)
	for {
		select {
		case sub := <-w.subchan:
			txn, err := getTxn(sub.id)
			if err != nil {
				sub.Err(err)
				sub.Close()
				continue
			}
			child, err := beginTxn(w.env, txn, sub.flags)
			if err != nil {
				sub.Err(err)
				sub.Close()
				continue
			}
			id := nextid
			nextid++
			txns = append(txns, IDTxn{id, child})
			sub.OK(id)
			sub.Close()
		case op := <-w.opchan:
			txn, err := getTxn(op.id)
			if err != nil {
				op.errc <- err
				close(op.errc)
				continue
			}
			if op.sub {
				op.errc <- w.doSub(txn, 0, op.fn)
				close(op.errc)
				continue
			}
			func() {
				defer close(op.errc)
				defer func() {
					if e := recover(); e != nil {
						txn.Abort()
						panic(e)
					}
				}()
				err = op.fn(txn)
				if err != nil {
					op.errc <- err
				}
			}()
		case comm := <-w.commit:
			txn, err := getTxn(comm.id)
			if err != nil {
				comm.errc <- err
			} else {
				txns = txns[:len(txns)-1]
				err = txn.Commit()
				if err != nil {
					comm.errc <- err
				}
			}
			close(comm.errc)
			if len(txns) == 0 {
				return
			}
		case id := <-w.abort:
			txn, err := getTxn(id)
			if err != nil {
				panic(fmt.Errorf("abort: %v", err))
			} else {
				txns = txns[:len(txns)-1]
				txn.Abort()
			}
			if len(txns) == 0 {
				return
			}
		}
	}
}

// doSub executes fn in order within a subtransaction.  if fn returns a
// non-nil error the subtransaction doSub aborts returns the error.  if no
// error is returned by fn then the subtransaction is committed.
func (w *WriteTxn) doSub(txn *Txn, flags uint, fn TxnOp) error {
	sub, err := w.beginSub(txn, flags)
	if err != nil {
		return err
	}
	defer func() {
		if e := recover(); e != nil {
			sub.Abort()
			panic(e)
		}
	}()
	err = fn(sub)
	if err != nil {
		sub.Abort()
		return err
	}
	return sub.Commit()
}

// beginSub is a convenience method to begin a subtransaction of txn.
func (w *WriteTxn) beginSub(txn *Txn, flags uint) (*Txn, error) {
	return beginTxn(w.env, txn, flags)
}

// TxnOp is an operation applied to a transaction.  If a TxnOp returns an error
// or panics the transaction will be aborted.
//
// IMPORTANT:
// TxnOps that write to the database (those passed to Update or BeginUpdate)
// must not use the Txn in another goroutine (passing it directory otherwise
// through closure). Doing so has undefined results.
type TxnOp func(txn *Txn) error

// Do executes fn within a transaction.  Do serializes execution of
// functions within a single goroutine (and thread).
func (w *WriteTxn) Do(fn TxnOp) error {
	return w.send(false, fn)
}

// Sub executes fn in order within a subtransaction of w committing iff no
// error is encountered.
func (w *WriteTxn) Sub(fn TxnOp) error {
	return w.send(true, fn)
}

// BeginSub starts a subtransaction of w and returns it as a new WriteTxn.
// While the returned transaction it is active the receiver w may not be used.
func (w *WriteTxn) BeginSub(fn TxnOp) (*WriteTxn, error) {
	subc := make(chan struct {
		int
		error
	})
	w.subchan <- writeNewSub{
		id:    w.id,
		flags: 0,
		c:     subc,
	}
	res := <-subc
	if res.error != nil {
		return nil, res.error
	}
	w2 := w.sub(res.int)
	return w2, nil
}

// sub returns a WriteTxn identical to w mod the id which is replaced.
func (w *WriteTxn) sub(id int) *WriteTxn {
	w2 := new(WriteTxn)
	*w2 = *w
	w2.id = id
	return w2
}

// send sends a writeOp to the primary transaction goroutine so execution of fn
// may be serialized.
func (w *WriteTxn) send(sub bool, fn TxnOp) error {
	// like Do but the operation is marked subtransactional
	errc := make(chan error)
	op := writeOp{w.id, sub, fn, errc}
	select {
	case <-w.closed:
		return fmt.Errorf("send: attempted after transaction was closed")
	case w.opchan <- op:
		return <-errc
	}
}

// Commit terminates the transaction and commits changes to the environment.
// An error is returned if any the transaction count not be committed or if the
// transaction was terminated previously.
func (w *WriteTxn) Commit() error {
	errc := make(chan error)
	comm := writeCommit{
		id:   w.id,
		errc: errc,
	}
	select {
	case <-w.closed:
		return fmt.Errorf("commit: attempted after transaction was closed")
	case w.commit <- comm:
		return <-errc
	}
}

// Abort terminates the transaction and discards changes.  Abort is idempotent.
func (w *WriteTxn) Abort() {
	select {
	case <-w.closed:
		panic("abort: attempted after transaction was closed")
	case w.abort <- w.id:
	}
}

type writeNewSub struct {
	id    int
	flags uint
	c     chan<- struct {
		int
		error
	}
}

func (w *writeNewSub) OK(newid int) {
	w.c <- struct {
		int
		error
	}{newid, nil}
}

func (w *writeNewSub) Err(err error) {
	if err == nil {
		panic("no error provided")
	}
	w.c <- struct {
		int
		error
	}{0, err}
}

func (w *writeNewSub) Close() {
	close(w.c)
}

type writeOp struct {
	id   int
	sub  bool
	fn   TxnOp
	errc chan<- error
}

type writeCommit struct {
	id   int
	errc chan<- error
}
