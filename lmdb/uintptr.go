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

// UintptrMulti is a FixedMultiple implementation that stores C.size_t-sized data
// values.
type UintptrMulti struct {
	page []byte
}

var _ FixedMultiple = (*UintptrMulti)(nil)

// UintptrMultiple converts a page of contiguous C.size_t value into a
// UintptrMulti.  Use this function after calling Cursor.Get with op
// GetMultiple on a database with DupSort|DupFixed|IntegerDup that stores
// C.size_t values.  UintptrMultiple panics if len(page) is not a multiple of
// unsife.Sizeof(C.size_t(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func UintptrMultiple(page []byte) *UintptrMulti {
	if len(page)%int(sizetSize) != 0 {
		panic("argument is not a page of C.size_t values")
	}

	return &UintptrMulti{page}
}

// Len implements FixedMultiple.
func (m *UintptrMulti) Len() int {
	return len(m.page) / int(sizetSize)
}

// Stride implements FixedMultiple.
func (m *UintptrMulti) Stride() int {
	return int(sizetSize)
}

// Size implements FixedMultiple.
func (m *UintptrMulti) Size() int {
	return len(m.page)
}

// Page implements FixedMultiple.
func (m *UintptrMulti) Page() []byte {
	return m.page
}

// CSizet returns the CSizetData at index i.
func (m *UintptrMulti) CSizet(i int) CSizetData {
	var x CSizetData
	copy(x[:], m.page[i*int(sizetSize):(i+1)*int(sizetSize)])
	return x
}

// Append returns the UintptrMulti result of appending x to m as C.size_t data.
func (m *UintptrMulti) Append(x CSizetData) *UintptrMulti {
	return &UintptrMulti{append(m.page, x[:]...)}
}

// CSizetData contains an unsigned integer the size of a C.size_t.
type CSizetData [sizetSize]byte

// CSizet returns a UintptrData containing the value C.size_t(x).
func CSizet(x uintptr) CSizetData {
	return cSizetData(C.size_t(x))
}

func cSizetData(x C.size_t) CSizetData {
	return *(*CSizetData)(unsafe.Pointer(&x))
}

func (v CSizetData) csizet() C.size_t {
	return *(*C.size_t)(unsafe.Pointer(&v))
}

// Uintptr returns contained data as a uint value.
func (v CSizetData) Uintptr() uintptr {
	return uintptr(v.csizet())
}

// csizet is a helper type for tests because tests cannot import C
type csizet C.size_t

func (x csizet) C() C.size_t {
	return C.size_t(x)
}
