package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import "unsafe"

const sizetSize = unsafe.Sizeof(C.size_t(0))

// Uintptr returns a UintptrValue containing the value C.size_t(x).
func Uintptr(x uintptr) *UintptrValue {
	return newSizetValue(x)
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

// UintptrMulti is FixedPage implementation that stores C.size_t-sized data
// values.
type UintptrMulti []byte

var _ FixedPage = (UintptrMulti)(nil)

// WrapUintptrMulti converts a page of contiguous uint value into a MultiUint.
// WrapMultiUint panics if len(page) is not a multiple of
// unsife.Sizeof(C.size_t(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func WrapUintptrMulti(page []byte) UintptrMulti {
	if len(page)%int(sizetSize) != 0 {
		panic("incongruent arguments")
	}
	return UintptrMulti(page)
}

// Len implements FixedPage.
func (m UintptrMulti) Len() int {
	return len(m) / int(sizetSize)
}

// Stride implements FixedPage.
func (m UintptrMulti) Stride() int {
	return int(sizetSize)
}

// Size implements FixedPage.
func (m UintptrMulti) Size() int {
	return len(m)
}

// Page implements FixedPage.
func (m UintptrMulti) Page() []byte {
	return []byte(m)
}

// Val returns the uint and index i.
func (m UintptrMulti) Val(i int) uint {
	data := m[i*int(sizetSize) : (i+1)*int(sizetSize)]
	return uint(*(*C.size_t)(unsafe.Pointer(&data[0])))
}

// Append returns the UintptrMulti result of appending x to m as C.size_t data.
func (m UintptrMulti) Append(x uintptr) UintptrMulti {
	var buf [sizetSize]byte
	*(*C.size_t)(unsafe.Pointer(&buf[0])) = C.size_t(x)
	return append(m, buf[:]...)
}

// UintptrValue is a Value that contains a C.size_t-sized data.
type UintptrValue [sizetSize]byte

var _ Value = (*UintptrValue)(nil)

func newSizetValue(x uintptr) *UintptrValue {
	v := new(UintptrValue)
	v.SetUintptr(x)
	return v
}

// SetUintptr stores x as a C.size_t in v.
func (v *UintptrValue) SetUintptr(x uintptr) {
	*(*C.size_t)(unsafe.Pointer(&(*v)[0])) = C.size_t(x)
}

func (v *UintptrValue) tobytes() []byte {
	return (*v)[:]
}
