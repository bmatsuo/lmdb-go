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

// MDB_val
type Val C.MDB_val

type MultiVal [2]Val

// Create a Val that points to p's data. the Val's data must not be freed
// manually and C references must not survive the garbage collection of p.
func Wrap(p []byte) *Val {
	if len(p) == 0 {
		return new(Val)
	}
	return &Val{
		mv_size: C.size_t(len(p)),
		mv_data: unsafe.Pointer(&p[0]),
	}
}

// WrapMulti converts a page of contiguous values with stride size into a
// MultiVal.  WrapMulti returns an error if len(page) is not a multiple of
// stride.
func WrapMulti(page []byte, stride int) (*MultiVal, error) {
	if len(page) == 0 {
		return new(MultiVal), nil
	}

	if len(page)%stride != 0 {
		return nil, fmt.Errorf("incongruent arguments")
	}

	data := &MultiVal{
		Val{
			mv_size: C.size_t(stride),
			mv_data: unsafe.Pointer(&page[0]),
		},
		Val{
			mv_size: C.size_t(len(page) / stride),
		},
	}
	return data, nil
}

// val converts a MultiVal into a pointer to Val.  This effectively creates a
// C-style array of the MultiVal data.
//
// See mdb_cursor_put and MDB_MULTIPLE.
func (val *MultiVal) val() *Val {
	return &val[0]
}

func (val *MultiVal) Bytes() [][]byte {
	ps := make([][]byte, val.NumItem())
	itemlen := val.ItemLen()
	data := val.Page()
	for i := range ps {
		ps[i], data = data[:itemlen], data[itemlen:]
	}
	return ps
}

// NumItem returns the number of items in the MultiVal.
func (val *MultiVal) NumItem() int {
	return int(val[1].mv_size)
}

// ItemLen returns the length of an individual item in the MultiVal.
func (val *MultiVal) ItemLen() int {
	return int(val[0].mv_size)
}

// Size returns the total size of the MultiVal data, the product of NumItem and
// ItemLen.
//
// BUG:
// Does not check oveflow.
func (val *MultiVal) Size() int {
	return val.NumItem() * val.ItemLen()
}

// Page returns the MultiVal page data as a raw slice of bytes with length val.Size().
func (val *MultiVal) Page() []byte {
	size := val.Size()
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val[0].mv_data)),
		Len:  size,
		Cap:  size,
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

// BytesCopy returns a slice copied from the region pointed to by val.
func (val *Val) BytesCopy() []byte {
	return C.GoBytes(val.mv_data, C.int(val.mv_size))
}

// Bytes creates a slice referencing the region referenced by val.
func (val *Val) Bytes() []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val.mv_data)),
		Len:  int(val.mv_size),
		Cap:  int(val.mv_size),
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

// If val is nil, an empty string is returned.
func (val *Val) String() string {
	return C.GoStringN((*C.char)(val.mv_data), C.int(val.mv_size))
}
