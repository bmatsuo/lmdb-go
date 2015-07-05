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

// Vals returns a slice containing each value in m.
func (m *Multi) Vals() [][]byte {
	i, ps := 0, make([][]byte, m.Len())
	for off := 0; off < len(m.page); off += m.stride {
		ps[i] = m.page[off : off+m.stride]
		i++
	}
	return ps
}

// Val returns the m at index i.  Val panics if i is out of range.
func (m *Multi) Val(i int) []byte {
	if i < 0 || m.Len() <= i {
		panic("index out of range")
	}
	off := i * m.stride
	return m.page[off : off+m.stride]
}

// Len returns the number of m in the Multi.
func (m *Multi) Len() int {
	return len(m.page) / m.stride
}

// Stride returns the length of an individual item in the Multi.
func (m *Multi) Stride() int {
	return m.stride
}

// Size returns the total size of the Multi data and is equim to
//
//		m.Len()*m.String()
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

var sizeofInt = unsafe.Sizeof(C.int(0))

func wrapValInt(x *int) *mdbVal {
	return &mdbVal{
		mv_size: C.size_t(sizeofInt),
		mv_data: unsafe.Pointer(x),
	}
}

func (val *mdbVal) Int() int {
	return int(*(*C.int)(val.mv_data))
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
