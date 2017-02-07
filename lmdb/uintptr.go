package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include "lmdb.h"
*/
import "C"

import "unsafe"

// NOTE:
// It seems realy pedantic to check if size_t and uintptr are the same size.
// But assuming they are the same size all of the overflow checks should be
// omitted as an optimization from the compiler.
const sizetSize = unsafe.Sizeof(C.size_t(0))
const uintptrSize = unsafe.Sizeof(uintptr(0))

// Uintptr returns a UintptrData containing the value C.size_t(x).
func Uintptr(x uintptr) *UintptrData {
	return newSizetData(x)
}

// getUintptr interprets the bytes of b as a C.size_t and returns the uintptr
// value.  getUintptr returns false if b is not the size of a C.size_t or
// cannot be decoded into a uintptr.
func getUintptr(b []byte) (x uintptr, ok bool) {
	_ = b[sizetSize-1]
	x = uintptr(*(*C.size_t)(unsafe.Pointer(&b[0])))
	if sizetSize > uintptrSize && C.size_t(x) != *(*C.size_t)(unsafe.Pointer(&b[0])) {
		// overflow
		return 0, false
	}
	return x, true
}

// UintptrMulti is a FixedPage implementation that stores C.size_t-sized data
// values.
type UintptrMulti struct {
	page []byte
}

var _ FixedPage = (*UintptrMulti)(nil)

// WrapUintptrMulti converts a page of contiguous uint value into a MultiUint.
// WrapMultiUint panics if len(page) is not a multiple of
// unsife.Sizeof(C.size_t(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func WrapUintptrMulti(page []byte) *UintptrMulti {
	if len(page)%int(sizetSize) != 0 {
		panic("argument is not a page of C.size_t values")
	}

	return &UintptrMulti{page}
}

// Len implements FixedPage.
func (m *UintptrMulti) Len() int {
	return len(m.page) / int(sizetSize)
}

// Stride implements FixedPage.
func (m *UintptrMulti) Stride() int {
	return int(sizetSize)
}

// Size implements FixedPage.
func (m *UintptrMulti) Size() int {
	return len(m.page)
}

// Page implements FixedPage.
func (m *UintptrMulti) Page() []byte {
	return m.page
}

// Uintptr returns the uintptr value at index i.
func (m *UintptrMulti) Uintptr(i int) uintptr {
	data := m.page[i*int(sizetSize) : (i+1)*int(sizetSize)]
	x := uintptr(*(*C.size_t)(unsafe.Pointer(&data[0])))
	if sizetSize > uintptrSize && C.size_t(x) != *(*C.size_t)(unsafe.Pointer(&data[0])) {
		panic(errOverflow)
	}
	return x
}

// Append returns the UintptrMulti result of appending x to m as C.size_t data.
func (m *UintptrMulti) Append(x uintptr) *UintptrMulti {
	var buf [sizetSize]byte
	*(*C.size_t)(unsafe.Pointer(&buf[0])) = C.size_t(x)
	if sizetSize < uintptrSize && uintptr(*(*C.size_t)(unsafe.Pointer(&buf[0]))) != x {
		panic(errOverflow)
	}
	return &UintptrMulti{append(m.page, buf[:]...)}
}

// UintptrData is a Data that contains a C.size_t-sized data.
type UintptrData [sizetSize]byte

var _ Data = (*UintptrData)(nil)

func newSizetData(x uintptr) *UintptrData {
	v := new(UintptrData)
	v.SetUintptr(x)
	return v
}

// Uintptr returns contained data as a uint value.
func (v *UintptrData) Uintptr() uintptr {
	x := *(*C.size_t)(unsafe.Pointer(&(*v)[0]))
	if sizetSize > uintptrSize && C.size_t(uintptr(x)) != x {
		panic(errOverflow)
	}
	return uintptr(x)
}

// SetUintptr stores x as a C.size_t in v.
func (v *UintptrData) SetUintptr(x uintptr) {
	*(*C.size_t)(unsafe.Pointer(&(*v)[0])) = C.size_t(x)
	if sizetSize < uintptrSize && uintptr(*(*C.size_t)(unsafe.Pointer(&(*v)[0]))) != x {
		panic(errOverflow)
	}
}

func (v *UintptrData) tobytes() []byte {
	return (*v)[:]
}

// csizet is a helper type for tests because tests cannot import C
type csizet C.size_t

func (x csizet) C() C.size_t {
	return C.size_t(x)
}
