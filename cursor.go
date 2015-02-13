package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

const (
	// Flags for Cursor.Get
	//
	// See MDB_cursor_op.
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
	NextMultiple = C.MDB_NEXT_MULTIPLE  // Get key and up to a page of values from the next cursor position (DupFixed).
	NextNoDup    = C.MDB_NEXT_NODUP     // The first value of the next key (DupSort).
	Prev         = C.MDB_PREV           // The previous item.
	PrevDup      = C.MDB_PREV_DUP       // The previous item of the current key (DupSort).
	PrevNoDup    = C.MDB_PREV_NODUP     // The last data item of the previous key (DupSort).
	Set          = C.MDB_SET            // The specified key.
	SetKey       = C.MDB_SET_KEY        // Get key and data at the specified key.
	SetRange     = C.MDB_SET_RANGE      // The first key no less than the specified key.
)

// The MDB_MULTIPLE and MDB_RESERVE flags are special and do not fit the
// calling pattern of other calls to Put.  They are not exported because they
// require special methods, PutMultiple and PutReserve in which the flag is
// implied and does not need to be passed.
const (
	// Flags for Txn.Put and Cursor.Put.
	//
	// See mdb_put and mdb_cursor_put.
	Current     = C.MDB_CURRENT     // Replace the item at the current key position (Cursor only)
	NoDupData   = C.MDB_NODUPDATA   // Store the key-value pair only if key is not present (DupSort).
	NoOverwrite = C.MDB_NOOVERWRITE // Store a new key-value pair only if key is not present.
	Append      = C.MDB_APPEND      // Append an item to the database.
	AppendDup   = C.MDB_APPENDDUP   // Append an item to the database (DupSort).
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
	k, v, err := cursor.getVal(setkey, setval, op)
	if err != nil {
		return nil, nil, err
	}
	return k.Bytes(), v.Bytes(), nil
}

// getVal retrieves items from the database.
//
// See mdb_cursor_get.
func (cursor *Cursor) getVal(setkey, setval []byte, op uint) (key, val *mdbVal, err error) {
	key = wrapVal(setkey)
	val = wrapVal(setval)
	ret := C.mdb_cursor_get(cursor._cursor, (*C.MDB_val)(key), (*C.MDB_val)(val), C.MDB_cursor_op(op))
	return key, val, errno(ret)
}

// Put stores an item in the database.
//
// See mdb_cursor_put.
func (cursor *Cursor) Put(key, val []byte, flags uint) error {
	ckey := wrapVal(key)
	cval := wrapVal(val)
	return cursor.putVal(ckey, cval, flags)
}

// PutReserve returns a []byte of length n that can be written to, potentially
// avoiding a memcopy.  The returned byte slice is only valid in txn's thread,
// before it has terminated.
func (cursor *Cursor) PutReserve(key []byte, n int, flags uint) ([]byte, error) {
	ckey := wrapVal(key)
	cval := &mdbVal{mv_size: C.size_t(n)}
	ret := C.mdb_cursor_put(cursor._cursor, (*C.MDB_val)(ckey), (*C.MDB_val)(cval), C.uint(flags|C.MDB_RESERVE))
	err := errno(ret)
	if err != nil {
		return nil, err
	}
	return cval.Bytes(), nil
}

// PutMulti stores a set of contiguous items with stride size under key.
// PutMulti returns an error if len(page) is not a multiple of stride.  The
// cursor's database must be DupFixed and DupSort.
//
// See mdb_cursor_put.
func (cursor *Cursor) PutMulti(key []byte, page []byte, stride int, flags uint) error {
	ckey := wrapVal(key)
	cval, err := WrapMulti(page, stride)
	if err != nil {
		return err
	}
	return cursor.putVal(ckey, cval.val(), flags|C.MDB_MULTIPLE)
}

// putVal stores an item in the database.
//
// See mdb_cursor_put.
func (cursor *Cursor) putVal(key, val *mdbVal, flags uint) error {
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
