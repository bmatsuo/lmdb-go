package lmdb

import (
	"fmt"
	"runtime"
)

// WriteTxn is a safe writable transaction.  All operations to the transaction
// are serialized a single goroutine (and single OS thread) for reliable
// execution behavior.  As a consequence the WriteTxn methods Do and Sub may be
// called concurrently from multiple goroutines.
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
	txn.managed = true
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
			child.managed = true
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
			func() {
				defer close(op.errc)
				defer func() {
					if e := recover(); e != nil {
						txn.abort()
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
				err = txn.commit()
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
				txn.abort()
			}
			if len(txns) == 0 {
				return
			}
		}
	}
}

// beginSub is a convenience method to begin a subtransaction of txn.
func (w *WriteTxn) beginSub(txn *Txn, flags uint) (*Txn, error) {
	return beginTxn(w.env, txn, flags)
}

// SubTxn returns a TxnOp that performs op in a subtransaction.  SubTxn
// simplifies executing subtransaction operations on WriteTxn types.
//		txn.Do(lmdb.SubTxn(op))
// The above statement is semantically equavalent to
//		txn.Do(func(txn *Txn) error {
//			return txn.Sub(op)
//		})
func SubTxn(op TxnOp) TxnOp {
	return func(txn *Txn) error { return txn.Sub(op) }
}

// Do executes fn within a transaction.  Do serializes execution of
// functions within a single goroutine (and thread).
func (w *WriteTxn) Do(fn TxnOp) error {
	return w.send(fn)
}

// BeginSub starts an unmanaged subtransaction of w and returns it as a new
// WriteTxn.  While the returned transaction is active the parent WriteTxn may
// not be used.
func (w *WriteTxn) BeginSub() (*WriteTxn, error) {
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
func (w *WriteTxn) send(fn TxnOp) error {
	// like Do but the operation is marked subtransactional
	errc := make(chan error)
	op := writeOp{w.id, fn, errc}
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
	fn   TxnOp
	errc chan<- error
}

type writeCommit struct {
	id   int
	errc chan<- error
}
