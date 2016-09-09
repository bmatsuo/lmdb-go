/*
Package lmdbscan provides a wrapper for lmdb.Cursor to simplify iteration.
*/
package lmdbscan

import (
	"fmt"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// errClosed is an error returned to the user when attempting to operate on a
// closed Scanner.
var errClosed = fmt.Errorf("scanner is closed")

// Scanner is a low level construct for scanning databases inside a
// transaction.
type Scanner struct {
	dbi lmdb.DBI
	cur *lmdb.Cursor
	op  uint
	key []byte
	val []byte
	err error
	set bool
}

// New allocates and intializes a Scanner for dbi within txn.  When the Scanner
// returned by New is no longer needed its Close method must be called.
func New(txn *lmdb.Txn, dbi lmdb.DBI) *Scanner {
	s := &Scanner{
		dbi: dbi,
		op:  lmdb.Next,
	}

	s.cur, s.err = txn.OpenCursor(dbi)
	return s
}

// Cursor returns the lmdb.Cursor underlying s.  Cursor returns nil if s is
// closed.
func (s *Scanner) Cursor() *lmdb.Cursor {
	return s.cur
}

// Del will delete the key at the current cursor location.
//
// Del is deprecated.  Instead use s.Cursor().Del(flags).
func (s *Scanner) Del(flags uint) error {
	if s.cur == nil {
		return errClosed
	}
	return s.cur.Del(flags)
}

// Key returns the key read during the last call to Scan.
func (s *Scanner) Key() []byte {
	return s.key
}

// Val returns the value read during the last call to Scan.
func (s *Scanner) Val() []byte {
	return s.val
}

// Set moves the cursor with s.Cursor().Get(k, v, opset), and sets s.Key(),
// s.Val(), and s.Err() accordingly.  The cursor will not move in the next call
// to Scan.
func (s *Scanner) Set(k, v []byte, opset uint) bool {
	if !s.checkOpen() {
		return false
	}
	s.set = true
	s.key, s.val, s.err = s.cur.Get(k, v, opset)
	return s.err == nil
}

// SetNext moves the cursor like s.Set(k, v, opset) for the next call to
// s.Scan().  Subsequent calls to s.Scan() move the cursor as c.Get(nil, nil,
// opnext)
func (s *Scanner) SetNext(k, v []byte, opset, opnext uint) bool {
	if !s.checkOpen() {
		return false
	}
	ok := s.Set(k, v, opset)
	s.op = opnext
	return ok
}

// Scan gets successive key-value pairs using the underlying cursor.  Scan
// returns false when key-value pairs are exhausted or another error is
// encountered.
func (s *Scanner) Scan() bool {
	if !s.checkOpen() {
		return false
	}
	if s.set {
		s.set = false
	} else {
		s.key, s.val, s.err = s.cur.Get(nil, nil, s.op)
	}
	return s.err == nil
}

func (s *Scanner) checkOpen() bool {
	if s.cur != nil {
		return true
	}
	if s.err == nil {
		s.err = errClosed
	}
	return false
}

// Err returns a non-nil error if and only if the previous call to s.Scan()
// resulted in an error other than lmdb.ErrNotFound.
func (s *Scanner) Err() error {
	if lmdb.IsNotFound(s.err) {
		return nil
	}
	return s.err
}

// Close closes the cursor underlying s and clears its ows internal structures.
// Close does not attempt to terminate the enclosing transaction.
//
// Scan must not be called after Close.
func (s *Scanner) Close() {
	if s.cur != nil {
		s.cur.Close()
		s.cur = nil
	}
}
