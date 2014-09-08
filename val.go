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

// MDB_val
type Val C.MDB_val

// Create a Val that points to p's data. the Val's data must not be freed
// manually and C references must not survive the garbage collection of p (and
// the returned Val).
func Wrap(p []byte) Val {
	if len(p) == 0 {
		return Val(C.MDB_val{})
	}
	return Val(C.MDB_val{
		mv_size: C.size_t(len(p)),
		mv_data: unsafe.Pointer(&p[0]),
	})
}

// BytesCopy returns a slice copied from the region pointed to by val.
func (val Val) BytesCopy() []byte {
	return C.GoBytes(val.mv_data, C.int(val.mv_size))
}

// Bytes creates a slice referencing the region referenced by val.
func (val Val) Bytes() []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val.mv_data)),
		Len:  int(val.mv_size),
		Cap:  int(val.mv_size),
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

// If val is nil, an empty string is returned.
func (val Val) String() string {
	return C.GoStringN((*C.char)(val.mv_data), C.int(val.mv_size))
}
