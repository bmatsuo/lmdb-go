package lmdbpool

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// UpdateHandling describes how a TxnPool handles existing lmdb.Readonly
// transactions when an environment update occurs.  Applications with a high
// rate of large updates may need to choose non-default settings to reduce
// their storage requirements at the cost of read throughput.
type UpdateHandling uint

const (
	// HandleOutstanding causes a TxnPool to abort any lmdb.Readonly
	// transactions that are being returned to the pool after an update.
	HandleOutstanding UpdateHandling = 1 << iota

	// HandleIdle causes a TxnPool to actively attempt aborting idle
	// transactions in the sync.Pool after an update has been committed.  There
	// is no guarantee when using AbortIdle that all idle readers will be
	// aborted.
	HandleIdle

	// HandleRenew modifies how other UpdateHandling flags are interpretted and
	// causes a TxnPool to renew transactions and put them back in the pool
	// instead of aborting them.
	HandleRenew
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
	UpdateHandling UpdateHandling
	env            *lmdb.Env
	lastid         uintptr
	pool           sync.Pool
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

	// If txn was holding stale pages the call to txn.Renew() should release
	// them when txn aquires a new lock (this is an implication made by the
	// LMDB documentation as of 0.9.19).
	err := txn.Renew()
	if err != nil {
		p.renewError(err)

		// Nothing we can do with txn now other than destroy it.
		txn.Abort()

		// For now it's not clear what better handling of a renew error would
		// entail so we just try to create a new transaction.  It is assumed
		// that it will fail with the same error... But maybe not?
		return p.env.BeginTxn(nil, lmdb.Readonly)
	}

	// Clear txn.Pooled to let a warning be emitted from the Txn finalizer
	// again.  And, make sure to clear RawRead to make the Txn appear like it
	// was just allocated.
	txn.RawRead = false
	txn.Pooled = false

	return txn, nil
}

func (p *TxnPool) renewError(err error) {
	// TODO:
	// When this is integrated directly in the lmdb package this can use
	// the same logging functionality that the Txn finalizer uses.
	log.Printf("lmdbpool: failed to renew transaction: %v", err)
}

func (p *TxnPool) abortReadonly(txn *lmdb.Txn) {
	if !returnTxnToPool {
		// If the pool is disabled from race detection then we just abort the
		// Txn instead of waiting for the finalizer.  See the files put.go and
		// putrace.go for more information.
		txn.Abort()
		return
	}

	if txn.ID() < p.getLastID() {
		ok, err := p.handleReadonly(txn, HandleOutstanding)
		if err != nil {
			// We attempted to renew the transaction but failed and the
			// transaction was automatically aborted.
			p.renewError(err)
			return
		}
		if !ok {
			// The transaction was aborted instead of being renewed.
			return
		}
	}

	// Don't waste cycles resetting RawRead here -- the cost be paid when the
	// Txn is reused (if at all).  All we need to do is set txn.Pooled to avoid
	// any warning emitted from the Txn finalizer.
	txn.Pooled = true
	txn.Reset()
	p.pool.Put(txn)
}

func (p *TxnPool) handleReadonly(txn *lmdb.Txn, condition UpdateHandling) (renewed bool, err error) {
	if p.UpdateHandling&condition == 0 {
		return
	}

	if p.UpdateHandling&HandleRenew != 0 {
		err = txn.Renew()
		if err != nil {
			// There is not much to do with txn other than abort it.
			txn.Abort()
		}
		return true, err
	}
	txn.Abort()
	return false, nil
}

// getLastID safely retrieves the value of p.lastid so routines operating on
// the sync.Pool know if a transaction can continue to be used without bloating
// the database.
func (p *TxnPool) getLastID() uintptr {
	return atomic.LoadUintptr(&p.lastid)
}

// CommitID sets the TxnPool's last-known transaction id to invalidate
// previously created lmdb.Readonly transactions and prevent their reuse.
//
// CommitID should only be called if p is not used to create/commit update
// transactions.
//
// BUG:
// HandleIdle is not checked.
func (p *TxnPool) CommitID(id uintptr) {
	// As long as we think we are holding a newer id than lastid we keep trying
	// to CAS until we see a newer id or the CAS succeeds.
	lastid := atomic.LoadUintptr(&p.lastid)
	for lastid < id && !atomic.CompareAndSwapUintptr(&p.lastid, lastid, id) {
		lastid = atomic.LoadUintptr(&p.lastid)
	}

	// TODO:
	// Presuming the CAS succeeded, do we now try to asynchronously terminate
	// transactions in the pool?  Alternative to terminating the transactions,
	// we could go through the pool and renew/reset any stale txns we find?
	//
	// In the case where a single transaction enters and exits the pool
	// repeatedly we are actually doing a disservice to the application because
	// it will need to allocate more Txns than it would otherwise if we were to
	// terminate them. Renewing them preemptively runs the risk of wasting
	// resources.
	//
	// The questions surrounding this require more benchmarks and real world
	// experimentation.
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
