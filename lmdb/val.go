package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"reflect"
	"unsafe"
)

// Multi is a wrapper for a contiguous page of sorted, fixed-length values
// passed to Cursor.PutMulti or retrieved using Cursor.Get with the
// GetMultiple/NextMultiple flag.
//
// Multi values are only useful in databases opened with DupSort|DupFixed.
type Multi struct {
	page   []byte
	stride int
}

// WrapMulti converts a page of contiguous values with stride size into a
// Multi.  WrapMulti panics if len(page) is not a multiple of stride.
//
//		_, val, _ := cursor.Get(nil, nil, lmdb.FirstDup)
//		_, page, _ := cursor.Get(nil, nil, lmdb.GetMultiple)
//		multi := lmdb.WrapMulti(page, len(val))
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func WrapMulti(page []byte, stride int) *Multi {
	if len(page)%stride != 0 {
		panic("incongruent arguments")
	}
	return &Multi{page: page, stride: stride}
}

// Vals returns a slice containing the values in m.  The returned slice has
// length m.Len() and each item has length m.Stride().
func (m *Multi) Vals() [][]byte {
	n := m.Len()
	ps := make([][]byte, n)
	for i := 0; i < n; i++ {
		ps[i] = m.Val(i)
	}
	return ps
}

// Val returns the value at index i.  Val panics if i is out of range.
func (m *Multi) Val(i int) []byte {
	off := i * m.stride
	return m.page[off : off+m.stride]
}

// Len returns the number of values in the Multi.
func (m *Multi) Len() int {
	return len(m.page) / m.stride
}

// Stride returns the length of an individual value in the m.
func (m *Multi) Stride() int {
	return m.stride
}

// Size returns the total size of the Multi data and is equal to
//
//		m.Len()*m.Stride()
//
func (m *Multi) Size() int {
	return len(m.page)
}

// Page returns the Multi page data as a raw slice of bytes with length
// m.Size().
func (m *Multi) Page() []byte {
	return m.page[:len(m.page):len(m.page)]
}

func (m *Multi) val() *multiVal {
	return &multiVal{
		mdbVal{
			mv_size: C.size_t(m.stride),
			mv_data: unsafe.Pointer(&m.page[0]),
		},
		mdbVal{
			mv_size: C.size_t(len(m.page) / m.stride),
		},
	}
}

// multiVal is a type to hold a page of values retrieved from a database
// created with DupSort|DupFixed.
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
type multiVal [2]mdbVal

// val converts a Multi into a pointer to mdbVal.  This effectively creates a
// C-style array of the Multi data.
//
// See mdb_cursor_put and MDB_MULTIPLE.
func (val *multiVal) val() *mdbVal {
	return &val[0]
}

// MDB_val
type mdbVal C.MDB_val

// wrapVal creates an mdbVal that points to p's data. the mdbVal's data must
// not be freed manually and C references must not survive the garbage
// collection of p.
func wrapVal(p []byte) *mdbVal {
	if len(p) == 0 {
		return new(mdbVal)
	}
	return &mdbVal{
		mv_size: C.size_t(len(p)),
		mv_data: unsafe.Pointer(&p[0]),
	}
}

var sizetBytes = unsafe.Sizeof(C.size_t(0))
var uintBytes = unsafe.Sizeof(C.uint(0))

func wrapValU(x *C.uint) *mdbVal {
	return &mdbVal{
		mv_size: C.size_t(uintBytes),
		mv_data: unsafe.Pointer(x),
	}
}

func wrapValZ(x *C.size_t) *mdbVal {
	return &mdbVal{
		mv_size: C.size_t(sizetBytes),
		mv_data: unsafe.Pointer(x),
	}
}

func (val *mdbVal) Uint() uint {
	if val.mv_size != C.size_t(uintBytes) {
		panic("val is not uint")
	}
	return uint(*(*C.uint)(val.mv_data))
}

func (val *mdbVal) Uint64() uint64 {
	if val.mv_size != C.size_t(sizetBytes) {
		panic("val is not size_t")
	}
	return uint64(*(*C.size_t)(val.mv_data))
}

// BytesCopy returns a slice copied from the region pointed to by val.
func (val *mdbVal) BytesCopy() []byte {
	return C.GoBytes(val.mv_data, C.int(val.mv_size))
}

// Bytes creates a slice referencing the region referenced by val.
func (val *mdbVal) Bytes() []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val.mv_data)),
		Len:  int(val.mv_size),
		Cap:  int(val.mv_size),
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

// If val is nil, an empty string is returned.
func (val *mdbVal) String() string {
	return C.GoStringN((*C.char)(val.mv_data), C.int(val.mv_size))
}
