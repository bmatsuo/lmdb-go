package lmdbpool

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// TxnPool is a pool for reusing transactions through their Reset and Renew
// methods.  However, even though TxnPool can only reuse lmdb.Readonly
// transactions it this way it should be used to create and terminate all Txns
// if it is used at all.  Update transactions can cause LMDB to use excessive
// numbers of pages when there are long-lived lmdb.Readonly transactions in a
// TxnPool.  Executing all transactions using the TxnPool allows it to track
// updates and prevent long-lived updates from causing excessive disk
// utilization.
type TxnPool struct {
	env    *lmdb.Env
	lastid uintptr
	pool   sync.Pool
}

// NewTxnPool initializes returns a new TxnPool.
func NewTxnPool(env *lmdb.Env) *TxnPool {
	p := &TxnPool{
		env: env,
	}
	return p
}

// Close flushes the pool of transactions and aborts them to free resources so
// that the pool Env may be closed.
func (p *TxnPool) Close() {
	txn, ok := (*lmdb.Txn)(nil), true
	for ok {
		txn, ok = p.pool.Get().(*lmdb.Txn)
		if ok {
			txn.Abort()
		}
	}
}

// BeginTxn is analogous to the BeginTxn method on lmdb.Env but may only be
// used to create lmdb.Readonly transactions.  Any call to BeginTxn that does
// not include lmdb.Readonly will return an error
func (p *TxnPool) BeginTxn(flags uint) (*lmdb.Txn, error) {
	// We can only re-use transactions with exactly the same flags.  So
	// instead of masking flags with lmdb.Readonly an equality comparison
	// is necessary.
	if flags != lmdb.Readonly {
		return nil, fmt.Errorf("flag lmdb.Readonly not provided")
	}

	return p.beginReadonly()
}

func (p *TxnPool) beginReadonly() (*lmdb.Txn, error) {
	txn, ok := p.pool.Get().(*lmdb.Txn)
	if !ok {
		return p.env.BeginTxn(nil, lmdb.Readonly)
	}

	// Abort the pooled transaction if it is causing LMDB to hold onto old
	// pages.
	id := txn.ID()
	lastid := atomic.LoadUintptr(&p.lastid)
	if id < lastid {
		txn.Abort()

		// Beginning a new transaction without continuing to read from the pool
		// is lazy.  But it is likely that remaining Txn objects in the pool
		// are holding stale pages and polling them out would be slow.
		// Instead, we hope the Txn finalizer will pick them up before any
		// other caller would.
		return p.env.BeginTxn(nil, lmdb.Readonly)
	}

	err := txn.Renew()
	if err != nil {
		txn.Abort()
		log.Printf("lmdbpool: failed to renew transaction: %v", err)

		// It's not clear for now what better handling of a renew error would
		// be so we just try to create a new transaction.  Presumably it will
		// fail with the same error.
		return p.env.BeginTxn(nil, lmdb.Readonly)
	}

	// Clear txn.Pooled to let a warning be emitted from the Txn finalizer
	// again.  And, make sure to clear RawRead to make the Txn appear like it
	// was just allocated.
	txn.RawRead = false
	txn.Pooled = false

	return txn, nil
}

func (p *TxnPool) abortReadonly(txn *lmdb.Txn) {
	// Don't waste cycles resetting RawRead here -- the cost be paid when the
	// Txn is reused (if at all).  All we need to do is set txn.Pooled to avoid
	// any warning emitted from the Txn finalizer.
	txn.Pooled = true

	txn.Reset()
	if returnTxnToPool {
		p.pool.Put(txn)
	} else {
		// If the pool is disabled from race detection then we just abort the
		// Txn instead of waiting for the finalizer.  See the files put.go and
		// putrace.go for more information.
		txn.Abort()
	}
}

// CommitID sets the TxnPool's last-known transaction id to invalidate
// previously created lmdb.Readonly transactions and prevent their reuse.
//
// CommitID should only be called if p is not used to create/commit update
// transactions.
func (p *TxnPool) CommitID(id uintptr) {
	// As long as we think we are holding a newer id than lastid we keep trying
	// to CAS until we see a newer id or the CAS succeeds.
	lastid := atomic.LoadUintptr(&p.lastid)
	for lastid < id && !atomic.CompareAndSwapUintptr(&p.lastid, lastid, id) {
		lastid = atomic.LoadUintptr(&p.lastid)
	}
}

// Abort aborts the txn and allows it to be reused if possible.  Abort must
// only be passed lmdb.Txn objects which it returned from a call to BeginTxn.
// Aborting a transaction created through other means will have undefined
// results.
func (p *TxnPool) Abort(txn *lmdb.Txn) {
	p.abortReadonly(txn)
}

// Update is analogous to the Update method on lmdb.Env.
func (p *TxnPool) Update(fn lmdb.TxnOp) error {
	var id uintptr
	err := p.env.Update(func(txn *lmdb.Txn) (err error) {
		err = fn(txn)
		if err != nil {
			return err
		}

		// Save the transaction identifier once we know fn succeeded so that
		// the p.lastid field can be set appropriately once the txn has
		// committed successfully.
		id = txn.ID()

		return nil
	})
	if err != nil {
		return err
	}

	p.CommitID(id)

	return nil
}

// View is analogous to the View method on lmdb.Env.
func (p *TxnPool) View(fn lmdb.TxnOp) error {
	txn, err := p.beginReadonly()
	if err != nil {
		return err
	}
	defer p.abortReadonly(txn)
	return txn.RunOp(fn, false)
}
