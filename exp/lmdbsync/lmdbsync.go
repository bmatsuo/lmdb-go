/*
Package lmdbsync provides advanced synchronization for LMDB environments at the
cost of performance.  The package provides a drop-in replacement for *lmdb.Env
that can be used in situations where the database may be resized or where the
flag lmdb.NoLock is used.

Bypassing an Env's methods to access the underlying lmdb.Env is not safe.  The
severity of such usage depends such behavior should be strictly avoided as it
may produce undefined behavior from the LMDB C library.

Resizing the environment

The Env type synchronizes all calls to Env.SetMapSize so that it may, with some
caveats, be safely called in the presence of concurrent transactions after an
environment has been opened.  All running transactions must complete before the
method will be called on the underlying lmdb.Env.

If an open transaction depends on a call to Env.SetMapSize then the Env will
deadlock and block all future transactions.  When using a Handler to
automatically call Env.SetMapSize this implies the restriction that
transactions must terminate independently of the creation/termination of other
transactions to avoid deadlock.

In the simplest example, a view transaction that attempts an update on the
underlying Env will deadlock the environment if the map is full and a Handler
attempts to resize the map so the update may be retried.

	env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(db, key)
		if err != nil {
			return err
		}
		err = env.Update(func(txn *lmdb.Txn) (err error) { // deadlock on lmdb.MapFull!
			txn.Put(dbi, key, append(v, b...))
		})
		return err
	}

The update should instead be prepared inside the view and then executed
following its termination.  This removes the implicit dependence of the view on
calls to Env.SetMapSize().

	var v []byte
	env.View(func(txn *lmdb.Txn) (err error) {
		// RawRead isn't used because the value will be used outside the
		// transaction.
		v, err = txn.Get(db, key)
		if err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		// ...
	}
	err = env.Update(func(txn *lmdb.Txn) (err error) { // no deadlock, even if env is resized!
		txn.Put(dbi, key, append(v, b...))
	})

The developers of LMDB officially recommend against applications changing the
memory map size for an open database.  It requires careful synchronization by
all processes accessing the database file.  And, a large memory map will not
affect disk usage on operating systems that support sparse files (e.g. Linux,
not OS X).

See mdb_env_set_mapsize.

MapFull

The MapFullHandler function configures an Env to automatically call increase
the map size with Env.SetMapSize and retry transactions when a lmdb.MapFull
error prevents an update from being committed.

Because updates may need to execute multiple times in the presence of
lmdb.MapFull it is important to make sure their TxnOp functions are idempotent
and do not cause unwanted additive change to the program state.

See mdb_txn_commit and MDB_MAP_FULL.

MapResized

When multiple processes access and resize an environment it is not uncommon to
encounter a MapResized error which prevents the TxnOp from being executed and
requires a synchronized call to Env.SetMapSize before continuing normal
operation.

The MapResizedHandler function configures an Env to automatically adopt a new
map size when a lmdb.MapResized error is encountered and retry execution of the
TxnOp.

See mdb_txn_begin and MDB_MAP_RESIZED.

NoLock

When the lmdb.NoLock flag is set on an environment Env handles all transaction
synchronization using Go structures and is an experimental feature.  It is
unclear what benefits this provides.

Usage of lmdb.NoLock requires that update transactions acquire an exclusive
lock on the environment.  In such cases it is required that view transactions
execute independently of update transactions, a requirement more strict than
that from handling MapFull.

See mdb_env_open and MDB_NOLOCK.
*/
package lmdbsync

import (
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Env wraps an *lmdb.Env, receiving all the same methods and proxying some to
// provide transaction management.  Transactions run by an Env handle
// lmdb.MapResized error transparently through additional synchronization.
// Additionally, Env is safe to use on environments setting the lmdb.NoLock
// flag.  When in NoLock mode write transactions block all read transactions
// from running (in addition to blocking other write transactions like a normal
// lmdb.Env would).
//
// Env proxies several methods to provide synchronization required for safe
// operation in some scenarios.  It is important not byprass proxies and call
// the methods directly on the underlying lmdb.Env or synchronization may be
// interfered with.  Calling proxied methods directly on the lmdb.Env may
// result in poor transaction performance or unspecified behavior in from the C
// library.
type Env struct {
	*lmdb.Env
	Handlers HandlerChain
	ctx      context.Context
	noLock   bool
	txnlock  sync.RWMutex
}

// NewEnv returns an newly allocated Env that wraps env.  If env is nil then
// lmdb.NewEnv() will be called to allocate an lmdb.Env.
func NewEnv(env *lmdb.Env, h ...Handler) (*Env, error) {
	var err error
	if env == nil {
		env, err = lmdb.NewEnv()
		if err != nil {
			return nil, err
		}
	}

	flags, err := env.Flags()
	if err != nil {
		return nil, err
	}
	noLock := flags&lmdb.NoLock != 0

	chain := append(HandlerChain(nil), h...)

	_env := &Env{
		Env:      env,
		Handlers: chain,
		noLock:   noLock,
		ctx:      context.Background(),
	}
	return _env, nil
}

// Open is a proxy for r.Env.Open() that detects the lmdb.NoLock flag to
// properly manage transaction synchronization.
func (r *Env) Open(path string, flags uint, mode os.FileMode) error {
	err := r.Env.Open(path, flags, mode)
	if err != nil {
		// no update to flags occurred
		return err
	}

	if flags&lmdb.NoLock != 0 {
		r.noLock = true
	}

	return nil
}

// SetMapSize is a proxy for r.Env.SetMapSize() that blocks while concurrent
// transactions are in progress.
func (r *Env) SetMapSize(size int64) error {
	return r.setMapSize(size, 0)
}

func (r *Env) setMapSize(size int64, delay time.Duration) error {
	r.txnlock.Lock()
	if delay > 0 {
		// wait before adopting a map size set from another process. hold on to
		// the transaction lock so that other transactions don't attempt to
		// begin while waiting.
		time.Sleep(delay)
	}
	err := r.Env.SetMapSize(size)
	r.txnlock.Unlock()
	return err
}

// BeginTxn overrides the r.Env.BeginTxn and always returns an error.  An
// unmanaged transaction.
func (r *Env) BeginTxn(parent *lmdb.Txn, flags uint) (*lmdb.Txn, error) {
	return nil, fmt.Errorf("lmdbsync: unmanaged transactions are not supported")
}

// RunTxn is a proxy for r.Env.RunTxn().
//
// If lmdb.NoLock is set on r.Env then RunTxn will block while other updates
// are in progress, regardless of flags.
func (r *Env) RunTxn(flags uint, op lmdb.TxnOp) (err error) {
	readonly := flags&lmdb.Readonly != 0
	return r.runHandler(readonly, func() error { return r.Env.RunTxn(flags, op) }, r.Handlers)
}

// View is a proxy for r.Env.View().
//
// If lmdb.NoLock is set on r.Env then View will block until any running update
// completes.
func (r *Env) View(op lmdb.TxnOp) error {
	return r.runHandler(true, func() error { return r.Env.View(op) }, r.Handlers)
}

// Update is a proxy for r.Env.Update().
//
// If lmdb.NoLock is set on r.Env then Update blocks until all other
// transactions have terminated and blocks all other transactions from running
// while in progress (including readonly transactions).
func (r *Env) Update(op lmdb.TxnOp) error {
	return r.runHandler(false, func() error { return r.Env.Update(op) }, r.Handlers)
}

// UpdateLocked is a proxy for r.Env.UpdateLocked().
//
// If lmdb.NoLock is set on r.Env then UpdateLocked blocks until all other
// transactions have terminated and blocks all other transactions from running
// while in progress (including readonly transactions).
func (r *Env) UpdateLocked(op lmdb.TxnOp) error {
	return r.runHandler(false, func() error { return r.Env.UpdateLocked(op) }, r.Handlers)
}

// WithHandler returns a TxnRunner than handles transaction errors r.Handlers
// chained with h.
func (r *Env) WithHandler(h Handler) TxnRunner {
	return &handlerRunner{
		env: r,
		h:   r.Handlers.Append(h),
	}
}

func (r *Env) runHandler(readonly bool, fn func() error, h Handler) error {
	ctx := r.ctx
	for {
		err := r.run(readonly, fn)
		ctx, err = h.HandleTxnErr(ctx, r, err)
		if err != ErrTxnRetry {
			return err
		}
	}
}
func (r *Env) run(readonly bool, fn func() error) error {
	var err error
	if r.noLock && !readonly {
		r.txnlock.Lock()
		err = fn()
		r.txnlock.Unlock()
	} else {
		r.txnlock.RLock()
		err = fn()
		r.txnlock.RUnlock()
	}
	return err
}
