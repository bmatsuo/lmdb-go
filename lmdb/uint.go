package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import "unsafe"

const uintSize = unsafe.Sizeof(C.uint(0))
const gouintSize = unsafe.Sizeof(uint(0))

// Uint allocates and returns a new Value that points to a value equal to
// C.uint(x).
func Uint(x uint) *UintValue {
	return newUintValue(x)
}

// GetUint interprets the bytes of b as a C.uint and returns the uint value.
func GetUint(b []byte) (x uint, ok bool) {
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

// MultiUint is a FixedPage implementation that stores C.uint-sized data
// values.
type MultiUint struct {
	page []byte
}

var _ FixedPage = (*MultiUint)(nil)

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

// Len implements FixedPage.
func (m *MultiUint) Len() int {
	return len(m.page) / int(uintSize)
}

// Stride implements FixedPage.
func (m *MultiUint) Stride() int {
	return int(uintSize)
}

// Size implements FixedPage.
func (m *MultiUint) Size() int {
	return len(m.page)
}

// Page implements FixedPage.
func (m *MultiUint) Page() []byte {
	return m.page
}

// Val returns the uint value of data at index i.
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

// UintValue is a Value implementation for C.uint-sized values.
type UintValue [uintSize]byte

var _ Value = (*UintValue)(nil)

func newUintValue(x uint) *UintValue {
	v := new(UintValue)
	v.SetUint(x)
	return v
}

// SetUint stores the value of x as a C.uint in v.
func (v *UintValue) SetUint(x uint) {
	*(*C.uint)(unsafe.Pointer(&(*v)[0])) = C.uint(x)
	if uintSize != gouintSize && uint(*(*C.uint)(unsafe.Pointer(&(*v)[0]))) != x {
		panic("value overflows unsigned int")
	}
}

func (v *UintValue) tobytes() []byte {
	return (*v)[:]
}
