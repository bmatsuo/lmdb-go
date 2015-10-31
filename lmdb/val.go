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

func valBytes(b []byte) (unsafe.Pointer, int) {
	if len(b) == 0 {
		return nil, 0
	}
	return unsafe.Pointer(&b[0]), len(b)
}

func wrapVal(b []byte) *C.MDB_val {
	ptr, n := valBytes(b)
	return &C.MDB_val{
		mv_data: unsafe.Pointer(ptr),
		mv_size: C.size_t(n),
	}
}

func getBytes(val *C.MDB_val) []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val.mv_data)),
		Len:  int(val.mv_size),
		Cap:  int(val.mv_size),
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

func getBytesCopy(val *C.MDB_val) []byte {
	return C.GoBytes(val.mv_data, C.int(val.mv_size))
}

// Value is data that can be written to an LMDB environment.
type Value interface {
	MemAddr() unsafe.Pointer
	MemSize() uintptr
}

// Bytes returns a Value describing the bytes underlying b.
func Bytes(b []byte) Value {
	return bytesValue(b)
}

// String returns a Value describing the bytes in s.
//
// BUG(bmatsuo):
// String creates a copy of the bytes in s.
func String(s string) Value {
	return Bytes([]byte(s))
}

// Uint allocates and returns a new Value that points to a value equal to
// C.uint(x).
func Uint(x uint) Value {
	v := new(uintValue)
	*v = uintValue(x)
	if uint(*v) != x {
		panic("value overflows unsigned int")
	}
	return v
}

// Uintptr allocates and returns a new Value that points to a value equal to
// C.size_t(x).
func Uintptr(x uintptr) Value {
	v := new(sizetValue)
	*v = sizetValue(x)
	return v
}

type bytesValue []byte

var _ Value = bytesValue(nil)

func (v bytesValue) MemAddr() unsafe.Pointer {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Pointer(&v[0])
}

func (v bytesValue) MemSize() uintptr {
	return uintptr(len(v))
}

type uintValue C.uint

var uintSize = unsafe.Sizeof(C.uint(0))

var _ Value = (*uintValue)(nil)

func (v *uintValue) SetUint(x uint) {
	*v = uintValue(x)
}

func (v *uintValue) MemAddr() unsafe.Pointer {
	return unsafe.Pointer(v)
}

func (v *uintValue) MemSize() uintptr {
	return uintSize
}

// UintValue interprets the bytes of b as an unsigned int and returns the value.
//
// BUG(bmatsuo):
// Does not check for overflow.
func UintValue(b []byte) (x uint, ok bool) {
	if uintptr(len(b)) != uintSize {
		return 0, false
	}
	x = uint(*(*C.uint)(unsafe.Pointer(&b[0])))
	return x, true
}

type sizetValue C.size_t

var sizetSize = unsafe.Sizeof(C.size_t(0))

func (v *sizetValue) SetUintptr(x uintptr) {
	*v = sizetValue(x)
}

func (v *sizetValue) MemAddr() unsafe.Pointer {
	return unsafe.Pointer(v)
}

func (v *sizetValue) MemSize() uintptr {
	return sizetSize
}

// UintptrValue interprets the bytes of b as a size_t and returns the value.
func UintptrValue(b []byte) (x uintptr, ok bool) {
	if uintptr(len(b)) != sizetSize {
		return 0, false
	}
	x = uintptr(*(*C.size_t)(unsafe.Pointer(&b[0])))
	return x, true
}
