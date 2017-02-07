package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import "unsafe"

const sizetSize = unsafe.Sizeof(C.size_t(0))

// Uintptr returns a UintptrData containing the value C.size_t(x).
func Uintptr(x uintptr) *UintptrData {
	return newSizetData(x)
}

// GetUintptr interprets the bytes of b as a size_t and returns the uintptr
// value.
func GetUintptr(b []byte) (x uintptr, ok bool) {
	if uintptr(len(b)) != sizetSize {
		return 0, false
	}
	x = uintptr(*(*C.size_t)(unsafe.Pointer(&b[0])))
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
		panic("incongruent arguments")
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
	return uintptr(*(*C.size_t)(unsafe.Pointer(&data[0])))
}

// Append returns the UintptrMulti result of appending x to m as C.size_t data.
func (m *UintptrMulti) Append(x uintptr) *UintptrMulti {
	var buf [sizetSize]byte
	*(*C.size_t)(unsafe.Pointer(&buf[0])) = C.size_t(x)
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
	return uintptr(*(*C.size_t)(unsafe.Pointer(&(*v)[0])))
}

// SetUintptr stores x as a C.size_t in v.
func (v *UintptrData) SetUintptr(x uintptr) {
	*(*C.size_t)(unsafe.Pointer(&(*v)[0])) = C.size_t(x)
}

func (v *UintptrData) tobytes() []byte {
	return (*v)[:]
}

// csizet is a helper type for tests because tests cannot import C
type csizet C.size_t

func (x csizet) C() C.size_t {
	return C.size_t(x)
}
