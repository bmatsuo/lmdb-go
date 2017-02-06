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

const uintSize = unsafe.Sizeof(C.uint(0))
const gouintSize = unsafe.Sizeof(uint(0))
const sizetSize = unsafe.Sizeof(C.size_t(0))

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

// Multi is a wrapper for a contiguous page of sorted, fixed-length values
// passed to Cursor.PutMulti or retrieved using Cursor.Get with the
// GetMultiple/NextMultiple operations.
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

// MultiUint is a wrapper for a contiguous page of sorted uint values retreived
// using the GetMultiple/NextMultiple operations on a Cursor.
type MultiUint struct {
	page []byte
}

// WrapMultiUint converts a page of contiguous uint value into a MultiUint.
// WrapMultiUint panics if len(page) is not a multiple of
// unsife.Sizeof(C.uint(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func WrapMultiUint(page []byte) *MultiUint {
	if len(page)%int(uintSize) != 0 {
		panic("incongruent arguments")
	}
	return &MultiUint{page: page}
}

// Len returns the number of uint values contained.
func (m *MultiUint) Len() int {
	return len(m.page) / int(uintSize)
}

// Val returns the uint and index i.
func (m *MultiUint) Val(i int) uint {
	data := m.page[i*int(uintSize) : (i+1)*int(uintSize)]
	x := uint(*(*C.uint)(unsafe.Pointer(&data[0])))
	if uintSize != gouintSize && C.uint(x) != *(*C.uint)(unsafe.Pointer(&data[0])) {
		panic("value oveflows uint")
	}
	return x
}

// Put appends x to the page.
func (m *MultiUint) Put(x uint) {
	var buf [uintSize]byte
	*(*C.uint)(unsafe.Pointer(&buf[0])) = C.uint(x)
	if uintSize != gouintSize && uint(*(*C.uint)(unsafe.Pointer(&buf[0]))) != x {
		panic("value overflows unsigned int")
	}
	m.page = append(m.page, buf[:]...)
}

// Page returns raw page data which can be passed to Cursor.Put with the
// PutMultiple flag.
func (m *MultiUint) Page() []byte {
	return m.page
}

// Stride returns the size and an individual element in m.Page().
func (m *MultiUint) Stride() int {
	return int(uintSize)
}

// MultiUintptr is a wrapper for a contiguous page of sorted uintptr values
// retreived using the GetMultiple/NextMultiple operations on a Cursor.
type MultiUintptr struct {
	page []byte
}

// WrapMultiUintptr converts a page of contiguous uint value into a MultiUint.
// WrapMultiUint panics if len(page) is not a multiple of
// unsife.Sizeof(C.size_t(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func WrapMultiUintptr(page []byte) *MultiUintptr {
	if len(page)%int(sizetSize) != 0 {
		panic("incongruent arguments")
	}
	return &MultiUintptr{page: page}
}

// Len returns the number of uint values contained.
func (m *MultiUintptr) Len() int {
	return len(m.page) / int(sizetSize)
}

// Val returns the uint and index i.
func (m *MultiUintptr) Val(i int) uint {
	data := m.page[i*int(sizetSize) : (i+1)*int(sizetSize)]
	return uint(*(*C.size_t)(unsafe.Pointer(&data[0])))
}

// Put appends x to the page.
func (m *MultiUintptr) Put(x uintptr) {
	var buf [sizetSize]byte
	*(*C.size_t)(unsafe.Pointer(&buf[0])) = C.size_t(x)
	m.page = append(m.page, buf[:]...)
}

// Page returns raw page data which can be passed to Cursor.Put with the
// PutMultiple flag.
func (m *MultiUintptr) Page() []byte {
	return m.page
}

// Stride returns the size and an individual element in m.Page().
func (m *MultiUintptr) Stride() int {
	return int(sizetSize)
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

// Value is data that can be written to an LMDB environment.
type Value interface {
	tobytes() []byte
	//MemAddr() unsafe.Pointer
	//MemSize() uintptr
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
	return newUintValue(x)
}

// Uintptr allocates and returns a new Value that points to a value equal to
// C.size_t(x).
func Uintptr(x uintptr) Value {
	return newSizetValue(x)
}

type bytesValue []byte

var _ Value = bytesValue(nil)

func (v bytesValue) tobytes() []byte {
	return []byte(v)
}

func (v bytesValue) MemAddr() unsafe.Pointer {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Pointer(&v[0])
}

func (v bytesValue) MemSize() uintptr {
	return uintptr(len(v))
}

type uintValue [uintSize]byte

var _ Value = (*uintValue)(nil)

func newUintValue(x uint) *uintValue {
	v := new(uintValue)
	v.SetUint(x)
	return v
}

func (v *uintValue) SetUint(x uint) {
	*(*C.uint)(unsafe.Pointer(&(*v)[0])) = C.uint(x)
	if uintSize != gouintSize && uint(*(*C.uint)(unsafe.Pointer(&(*v)[0]))) != x {
		panic("value overflows unsigned int")
	}
}

func (v *uintValue) tobytes() []byte {
	return (*v)[:]
}

// UintValue interprets the bytes of b as an unsigned int and returns the uint
// value.
func UintValue(b []byte) (x uint, ok bool) {
	if uintptr(len(b)) != uintSize {
		return 0, false
	}
	x = uint(*(*C.uint)(unsafe.Pointer(&b[0])))
	if uintSize != gouintSize && C.uint(x) != *(*C.uint)(unsafe.Pointer(&b[0])) {
		// overflow
		return 0, false
	}
	return x, true
}

type sizetValue [sizetSize]byte

var _ Value = (*sizetValue)(nil)

func newSizetValue(x uintptr) *sizetValue {
	v := new(sizetValue)
	v.SetUintptr(x)
	return v
}

func (v *sizetValue) SetUintptr(x uintptr) {
	*(*C.size_t)(unsafe.Pointer(&(*v)[0])) = C.size_t(x)
}

func (v *sizetValue) tobytes() []byte {
	return (*v)[:]
}

// UintptrValue interprets the bytes of b as a size_t and returns the uintptr
// value.
func UintptrValue(b []byte) (x uintptr, ok bool) {
	if uintptr(len(b)) != sizetSize {
		return 0, false
	}
	x = uintptr(*(*C.size_t)(unsafe.Pointer(&b[0])))
	return x, true
}
