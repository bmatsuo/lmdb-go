package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

// Flags for the Get method on Cursors.  These flags modify the cursor position
// and dictate behavior.
//
// See MDB_cursor_op.
const (
	First        = C.MDB_FIRST          // The first item.
	FirstDup     = C.MDB_FIRST_DUP      // The first value of current key (DupSort).
	GetBoth      = C.MDB_GET_BOTH       // Get the key as well as the value (DupSort).
	GetBothRange = C.MDB_GET_BOTH_RANGE // Get the key and the nearsest value (DupSort).
	GetCurrent   = C.MDB_GET_CURRENT    // Get the key and value at the current position.
	GetMultiple  = C.MDB_GET_MULTIPLE   // Get up to a page dup values for key at current position (DupFixed).
	Last         = C.MDB_LAST           // Last item.
	LastDup      = C.MDB_LAST_DUP       // Position at last value of current key (DupSort).
	Next         = C.MDB_NEXT           // Next value.
	NextDup      = C.MDB_NEXT_DUP       // Next value of the current key (DupSort).
	NextMultiple = C.MDB_NEXT_MULTIPLE  // Get key and up to a page of values from the next cursor position (DupFixed)
	NextNoDup    = C.MDB_NEXT_NODUP     // The first value of the next key (DupSort).
	Prev         = C.MDB_PREV           // The previous item.
	PrevDup      = C.MDB_PREV_DUP       // The previous item of the current key (DupSort).
	PrevNoDup    = C.MDB_PREV_NODUP     // The last data item of the previous key (DupSort).
	Set          = C.MDB_SET            // The specified key.
	SetKey       = C.MDB_SET_KEY        // Get key and data at the specified key.
	SetRange     = C.MDB_SET_RANGE      // The first key no less than the specified key.
)

// Flags for the Put method on Cursors.
//
// Importers of the package should not use the Multiple flag directly and
// should instead use the specialized function, PutMulti.  In this area the C
// API is dark and full of terrors.
//
// Note: the MDB_RESERVE flag is somewhat special and does not fit the calling
// pattern of most calls to Put. It requires a special method (TODO).
//
// See mdb_put and mdb_cursor_put.
const (
	Current     = C.MDB_CURRENT     // Replace the item at the current key position (Cursor only).
	NoDupData   = C.MDB_NODUPDATA   // Store the key-value pair only if key is not present (DupSort)
	NoOverwrite = C.MDB_NOOVERWRITE // Store a new key-value pair only if key is not present
	Append      = C.MDB_APPEND      // Append an item to the database.
	AppendDup   = C.MDB_APPENDDUP   // Append an item to the database (DupSort).
	Multiple    = C.MDB_MULTIPLE    // Danger Zone. Store multiple contiguous items (DupSort + DupFixed).
)

// Cursor operates on data inside a transaction and holds a position in the
// database.
//
// See MDB_cursor.
type Cursor struct {
	txn     *Txn
	_cursor *C.MDB_cursor
}

func openCursor(txn *Txn, db DBI) (*Cursor, error) {
	var _cursor *C.MDB_cursor
	ret := C.mdb_cursor_open(txn._txn, C.MDB_dbi(db), &_cursor)
	if ret != success {
		return nil, errno(ret)
	}
	return &Cursor{txn, _cursor}, nil
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
func (cursor *Cursor) Close() {
	C.mdb_cursor_close(cursor._cursor)
	cursor._cursor = nil
}

// Txn returns the cursor's transaction.
func (cursor *Cursor) Txn() *Txn {
	return cursor.txn
}

// DBI returns the cursors database.
func (cursor *Cursor) DBI() DBI {
	var _dbi C.MDB_dbi
	_dbi = C.mdb_cursor_dbi(cursor._cursor)
	return DBI(_dbi)
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
func (cursor *Cursor) GetVal(setkey, setval []byte, op uint) (key, val *Val, err error) {
	key = Wrap(setkey)
	val = Wrap(setval)
	ret := C.mdb_cursor_get(cursor._cursor, (*C.MDB_val)(key), (*C.MDB_val)(val), C.MDB_cursor_op(op))
	return key, val, errno(ret)
}

// Put stores an item in the database.
//
// See mdb_cursor_put.
func (cursor *Cursor) Put(key, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	return cursor.PutVal(ckey, cval, flags)
}

// PutMulti stores a set of contiguous items with stride size under key.
// PutMulti returns an error if len(page) is not a multiple of stride.  The
// cursor's database must be DupFixed and DupSort.
//
// PutMulti implies Multiple and it does not need to be supplied in flags.
//
// See mdb_cursor_put.
func (cursor *Cursor) PutMulti(key []byte, page []byte, stride int, flags uint) error {
	ckey := Wrap(key)
	cval, err := WrapMulti(page, stride)
	if err != nil {
		return err
	}
	return cursor.PutVal(ckey, cval.val(), flags|Multiple)
}

// Put stores an item in the database.
//
// See mdb_cursor_put.
func (cursor *Cursor) PutVal(key, val *Val, flags uint) error {
	ret := C.mdb_cursor_put(cursor._cursor, (*C.MDB_val)(key), (*C.MDB_val)(val), C.uint(flags))
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
	if ret != success {
		return 0, errno(ret)
	}
	return uint64(_size), nil
}
