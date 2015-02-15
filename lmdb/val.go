package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Multi is a type to hold a page of values retrieved from a database open
// with DupSort|DupFixed.
//
// See mdb_cursor_put and MDB_MULTIPLE.
type Multi [2]mdbVal

// WrapMulti converts a page of contiguous values with stride size into a
// Multi.  WrapMulti returns an error if len(page) is not a multiple of
// stride.
func WrapMulti(page []byte, stride int) (*Multi, error) {
	if len(page) == 0 {
		return new(Multi), nil
	}

	if len(page)%stride != 0 {
		return nil, fmt.Errorf("incongruent arguments")
	}

	data := &Multi{
		mdbVal{
			mv_size: C.size_t(stride),
			mv_data: unsafe.Pointer(&page[0]),
		},
		mdbVal{
			mv_size: C.size_t(len(page) / stride),
		},
	}
	return data, nil
}

// val converts a Multi into a pointer to mdbVal.  This effectively creates a
// C-style array of the Multi data.
//
// See mdb_cursor_put and MDB_MULTIPLE.
func (val *Multi) val() *mdbVal {
	return &val[0]
}

// Vals returns a slice containing each value in val.
func (val *Multi) Vals() [][]byte {
	ps := make([][]byte, 0, val.Len())
	stride := val.Stride()
	data := val.Page()
	for off := 0; off < len(data); off += stride {
		ps = append(ps, data[off:off+stride])
	}
	return ps
}

// Val returns the value at index i.
func (val *Multi) Val(i int) []byte {
	if i < 0 {
		panic("index out of range")
	}
	if i >= val.Len() {
		panic("index out of range")
	}
	stride := val.Stride()
	off := i * stride
	return val.Page()[off : off+stride]
}

// Len returns the number of values in the Multi.
func (val *Multi) Len() int {
	return int(val[1].mv_size)
}

// Stride returns the length of an individual item in the Multi.
func (val *Multi) Stride() int {
	return int(val[0].mv_size)
}

// Size returns the total size of the Multi data and is equivalent to
//
//		val.Len()*val.String()
//
// BUG:
// Does not check oveflow.
func (val *Multi) Size() int {
	return val.Len() * val.Stride()
}

// Page returns the Multi page data as a raw slice of bytes with length val.Size().
func (val *Multi) Page() []byte {
	size := val.Size()
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val[0].mv_data)),
		Len:  size,
		Cap:  size,
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
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
