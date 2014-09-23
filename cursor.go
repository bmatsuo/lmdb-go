package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"errors"
)

// MDB_cursor_op
const (
	First = iota
	FirstDup
	GetBoth
	GetRange
	GetCurrent
	GetMultiple
	Last
	LastDup
	Next
	NextDup
	NextMultiple
	NextNoDup
	Prev
	PrevDup
	PrevNoDup
	Set
	SetKey
	SetRange
)

// Cursor operates on data inside a transaction and holds a position in the
// database.
//
// See MDB_cursor.
type Cursor struct {
	_cursor *C.MDB_cursor
}

func openCursor(txn *Txn, db DBI) (*Cursor, error) {
	var _cursor *C.MDB_cursor
	ret := C.mdb_cursor_open(txn._txn, C.MDB_dbi(db), &_cursor)
	if ret != Success {
		return nil, errno(ret)
	}
	return &Cursor{_cursor}, nil
}

// Renew associates readonly cursor with txn.
//
// See mdb_cursor_renew.
func (cursor *Cursor) Renew(txn *Txn) error {
	ret := C.mdb_cursor_renew(txn._txn, cursor._cursor)
	return errno(ret)
}

// Close the cursor handle.  The cursor must not be used after Close returns.
// Cursors in write transactions must be closed before their transaction is
// terminated.
//
// See mdb_cursor_close.
func (cursor *Cursor) Close() error {
	if cursor._cursor == nil {
		return errors.New("Cursor already closed")
	}
	C.mdb_cursor_close(cursor._cursor)
	cursor._cursor = nil
	return nil
}

// Txn returns the cursors transaction.
//
// See mdb_cursor_txn.
func (cursor *Cursor) Txn() *Txn {
	var _txn *C.MDB_txn
	_txn = C.mdb_cursor_txn(cursor._cursor)
	if _txn != nil {
		return &Txn{_txn}
	}
	return nil
}

// DBI returns the cursors database.
func (cursor *Cursor) DBI() DBI {
	var _dbi C.MDB_dbi
	_dbi = C.mdb_cursor_dbi(cursor._cursor)
	return DBI(_dbi)
}

// MDBCursor retrieves the low-level MDB cursor.
func (cursor *Cursor) MDBCursor() *C.MDB_cursor {
	return cursor._cursor
}

// Get retrieves items from the database. The slices returned by Get reference
// readonly sections of memory and attempts to mutate the region of memory will
// result in a panic.
//
// See mdb_cursor_get.
func (cursor *Cursor) Get(setkey, setval []byte, op uint) (key, val []byte, err error) {
	k, v, err := cursor.GetVal(setkey, setval, op)
	if err != nil {
		return nil, nil, err
	}
	return k.Bytes(), v.Bytes(), nil
}

// GetVal retrieves items from the database.
//
// See mdb_cursor_get.
func (cursor *Cursor) GetVal(setkey, setval []byte, op uint) (key, val Val, err error) {
	key = Wrap(setkey)
	val = Wrap(setval)
	ret := C.mdb_cursor_get(cursor._cursor, (*C.MDB_val)(&key), (*C.MDB_val)(&val), C.MDB_cursor_op(op))
	return key, val, errno(ret)
}

// Put stores an item in the database.
//
// See mdb_cursor_put.
func (cursor *Cursor) Put(key, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_cursor_put(cursor._cursor, (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval), C.uint(flags))
	return errno(ret)
}

// Del deletes the item referred to by the cursor from the database.
//
// See mdb_cursor_del.
func (cursor *Cursor) Del(flags uint) error {
	ret := C.mdb_cursor_del(cursor._cursor, C.uint(flags))
	return errno(ret)
}

// Count returns the number of duplicates for the current key.
//
// See mdb_cursor_count.
func (cursor *Cursor) Count() (uint64, error) {
	var _size C.size_t
	ret := C.mdb_cursor_count(cursor._cursor, &_size)
	if ret != Success {
		return 0, errno(ret)
	}
	return uint64(_size), nil
}
