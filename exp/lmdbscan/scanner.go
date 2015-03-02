/*
Package lmdbscan provides a wrapper for lmdb.Cursor to simplify iteration.
This package is experimental and it's API may change.
*/
package lmdbscan

import (
	"fmt"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Stop is returned by a Func to signal that Scanner.Scan should terminate and
// return false.
var Stop = fmt.Errorf("stop")

// Skip is returned by a Func to signal that Scanner.Scan should not yield the
// current (k, v) pair.
var Skip = fmt.Errorf("skip")

// A Func is used to scan (k, v) pairs in an lmdb database.  A Func can be used
// either as a filter or as a handler depending on context.
type Func func(k, v []byte) error

// Ignore returns a Func that calls fn(k, v) on pairs and returns Skip
// whenever any error is returned.
func Ignore(fn Func) Func {
	return func(k, v []byte) error {
		err := fn(k, v)
		if err != nil {
			return Skip
		}
		return nil
	}
}

// While returns a Func  that returns nil for all (k, v) pairs that fn(k,v)
// returns true and Stop otherwise.
func While(fn func(k, v []byte) bool) Func {
	return func(k, v []byte) error {
		if fn(k, v) {
			return nil
		}
		return Stop
	}
}

// Select returns a Func that returns nil for all (k, v) pairs that fn(k,v)
// returns true and Stop otherwise.
func Select(fn func(k, v []byte) bool) Func {
	return func(k, v []byte) error {
		if fn(k, v) {
			return nil
		}
		return Skip
	}
}

// Each scans dbi in reverse order and calls fn(k, v) for each (k, v) pair.
func EachReverse(txn *lmdb.Txn, dbi lmdb.DBI, fn Func) error {
	s := New(txn, dbi)
	defer s.Close()
	s.SetNext(nil, nil, lmdb.Last, lmdb.Prev)
	for s.Scan(fn) {
	}
	return s.Err()
}

// Each scans dbi in order and calls fn(k, v) for each (k ,v) pair.
func Each(txn *lmdb.Txn, dbi lmdb.DBI, fn Func) error {
	s := New(txn, dbi)
	defer s.Close()
	for s.Scan(fn) {
	}
	return s.Err()
}

// Scanner is a low level construct for scanning databases inside a
// transaction.
type Scanner struct {
	dbi     lmdb.DBI
	dbflags uint
	txn     *lmdb.Txn
	cur     *lmdb.Cursor
	op      uint
	setop   *uint
	setkey  []byte
	setval  []byte
	key     []byte
	val     []byte
	err     error
}

// New allocates and intializes a Scanner for dbi within txn.
func New(txn *lmdb.Txn, dbi lmdb.DBI) *Scanner {
	s := &Scanner{
		dbi: dbi,
		txn: txn,
	}
	s.dbflags, s.err = txn.Flags(dbi)
	if s.err != nil {
		return s
	}
	s.op = lmdb.Next

	s.cur, s.err = txn.OpenCursor(dbi)
	return s
}

// Key returns the key read during the last call to Scan.
func (s *Scanner) Key() []byte {
	return s.key
}

// Val returns the value read during the last call to Scan.
func (s *Scanner) Val() []byte {
	return s.val
}

// Set marks the starting position for iteration.  On the next call to s.Scan()
// the underlying cursor will be moved as
//		c.Get(k, v, op)
func (s *Scanner) Set(k, v []byte, op uint) {
	if s.err != nil {
		return
	}
	s.setop = new(uint)
	*s.setop = op
	s.setkey = k
	s.setval = v
}

// Set determines the cursor behavior for subsequent calls to s.Scan().  The
// immediately following follow to s.Scan() behaves as if s.Set(k,v,opset) was
// called.  Subsequent calls move the cursor as
//		c.Get(nil, nil, opnext)
func (s *Scanner) SetNext(k, v []byte, opset, opnext uint) {
	s.Set(k, v, opset)
	s.op = opnext
}

// Scan gets key-value successive pairs with the underlying cursor until one
// matches the supplied filters.  If all filters return a nil error for the
// current pair, true is returned.  Scan automatically gets the next pair if
// any filter returns Skip.  Scan returns false if all key-value pairs where
// exhausted or another non-nil error was returned by a filter.
func (s *Scanner) Scan(filter ...Func) bool {
move:
	if s.setop == nil {
		s.key, s.val, s.err = s.cur.Get(nil, nil, s.op)
	} else {
		s.key, s.val, s.err = s.cur.Get(s.setkey, s.setval, *s.setop)
		s.setkey = nil
		s.setval = nil
		s.setop = nil
	}
	if s.err != nil {
		return false
	}

	for _, fn := range filter {
		s.err = fn(s.key, s.val)
		if s.err == Skip {
			goto move
		}
		if s.err != nil {
			return false
		}
	}

	return true
}

// Err returns a non-nil error if and only if the previous call to s.Scan()
// resulted in an error other than Stop or lmdb.ErrNotFound.
func (s *Scanner) Err() error {
	if lmdb.IsNotFound(s.err) {
		return nil
	}
	if s.err == Stop {
		return nil
	}
	return s.err
}

// Close clears internal structures.  Close does not attempt to terminate the
// enclosing transaction.
//
// Scan must not be called after Close.
func (s *Scanner) Close() {
	s.txn = nil
	if s.cur != nil {
		s.cur.Close()
		s.cur = nil
	}
}
