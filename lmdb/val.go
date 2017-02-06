package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"unsafe"

	"github.com/bmatsuo/lmdb-go/internal/lmdbarch"
)

// valSizeBits is the number of bits which constraining the length of the
// single values in an LMDB database, either 32 or 31 depending on the
// platform.  valMaxSize is the largest data size allowed based.  See runtime
// source file malloc.go and the compiler typecheck.go for more information
// about memory limits and array bound limits.
//
//		https://github.com/golang/go/blob/a03bdc3e6bea34abd5077205371e6fb9ef354481/src/runtime/malloc.go#L151-L164
//		https://github.com/golang/go/blob/36a80c5941ec36d9c44d6f3c068d13201e023b5f/src/cmd/compile/internal/gc/typecheck.go#L383
//
// On 64-bit systems, luckily, the value 2^32-1 coincides with the maximum data
// size for LMDB (MAXDATASIZE).
const (
	valSizeBits = lmdbarch.Width64*32 + (1-lmdbarch.Width64)*31
	valMaxSize  = 1<<valSizeBits - 1
)

// Value is a container for data that can be written to an LMDB environment.
//
// Value types are only required when working with databases opened with the
// IntegerKey or IntegerDup flags.  Using Value types in other situations will
// only hurt the performance of your application.
type Value interface {
	tobytes() []byte
}

// FixedPage represents a contiguous sequence of fixed sized data items.
// FixedPage implementations are often mutable and allow construction of data
// for use with the Cursor.PutMulti method.
//
// FixedPage types are only particularly useful when working with databases
// opened with the DupSort|DupFixed combination of flags.
type FixedPage interface {
	// Page returns the raw page data.
	Page() []byte

	// Len returns the number of items in the page
	Len() int

	// Stride returns the side of an indivual page item
	Stride() int

	// Size returns the total size of page data
	Size() int
}

// Multi is a generic FixedPage implementation that can store contiguous
// fixed-width for a configurable width (stride).
//
// Multi values are only useful in databases opened with DupSort|DupFixed.
type Multi struct {
	page   []byte
	stride int
}

// WrapMulti converts a page of contiguous stride-sized values into a Multi.
// WrapMulti panics if len(page) is not a multiple of stride.
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

// Put appends b to the page.  Put panics if len(b) is not equal to m.Stride()
func (m *Multi) Put(b []byte) {
	if len(b) != m.stride {
		panic("bad data size")
	}
	m.page = append(m.page, b...)
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

var eb = []byte{0}

func valBytes(b []byte) ([]byte, int) {
	if len(b) == 0 {
		return eb, 0
	}
	return b, len(b)
}

func wrapVal(b []byte) *C.MDB_val {
	p, n := valBytes(b)
	return &C.MDB_val{
		mv_data: unsafe.Pointer(&p[0]),
		mv_size: C.size_t(n),
	}
}

func getBytes(val *C.MDB_val) []byte {
	return (*[valMaxSize]byte)(unsafe.Pointer(val.mv_data))[:val.mv_size:val.mv_size]
}

func getBytesCopy(val *C.MDB_val) []byte {
	return C.GoBytes(val.mv_data, C.int(val.mv_size))
}

// Bytes returns a Value containg b.  The returned value shares is memory with
// b and b must not be modified while it is use.
func Bytes(b []byte) Value {
	return bytesValue(b)
}

// String returns a Value describing the bytes in s.
//
// BUG(bmatsuo):
// String creates a copy of the bytes in s.  Use the Cursor.PutReserve method
// and explicitly copy string data to avoid unnecessary an allocation/copy.
func String(s string) Value {
	return Bytes([]byte(s))
}

type bytesValue []byte

var _ Value = bytesValue(nil)

func (v bytesValue) tobytes() []byte {
	return []byte(v)
}
