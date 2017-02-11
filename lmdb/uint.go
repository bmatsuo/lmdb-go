package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "lmdb.h"
*/
import "C"

import "unsafe"

// CUintMax is the largest value for a C.uint type and the largest value that
// can be stored in a Uint object.  On 64-bit architectures it is likely that
// CUintMax is less than the largest value of Go's uint type.
//
// Applications on 64-bit architectures that would like to store a 64-bit
// unsigned value should use the UintptrData type instead of the CUintData
// type.
const CUintMax = C.UINT_MAX

const cUintSize = unsafe.Sizeof(C.uint(0))
const goUintSize = unsafe.Sizeof(uint(0))

// CanFitInCUint returns true if value x can be stored in type C.uint.  It is the
// application programmer's responsibility to call this function when
// applicable.
func CanFitInCUint(x uint) bool {
	if cUintSize < goUintSize && x != uint(C.uint(x)) {
		return false
	}
	return true
}

// CanFitInUint returns true if value x can stored in type uint, which is
// typically always possible.  It is the application programmer's
// responsibility to call this function when applicable.
func CanFitInUint(x CUintData) bool {
	_x := x.cuint()
	if cUintSize > goUintSize && _x != C.uint(uint(_x)) {
		return false
	}
	return true
}

// getUint interprets the bytes of b as a C.uint and returns the uint value.
// getUint returns false if b is not the size of a C.uint or cannot be decoded
// to a uint.
//
// It is the callers responsibility that b is large enough to hold a C.uint.
func getUint(b []byte) (x uint, ok bool) {
	_ = b[cUintSize-1]
	x = uint(*(*C.uint)(unsafe.Pointer(&b[0])))
	if cUintSize > goUintSize && C.uint(x) != *(*C.uint)(unsafe.Pointer(&b[0])) {
		// overflow
		return 0, false
	}
	return x, true
}

// MultiCUint is a FixedMultiple implementation that stores C.uint-sized data
// values that are read from Cursor.GetMultiple or to Cursor.PutMultiple.
type MultiCUint struct {
	page []byte
}

var _ FixedMultiple = (*MultiCUint)(nil)

// MultipleCUint converts a page of contiguous C.uint value into a MultiCUint.
// Use this function after calling Cursor.Get with op GetMultiple on a database
// with DupSort|DupFixed|IntegerDup that stores C.uint values.  MultipleCUint
// panics if len(page) is not a multiple of unsife.Sizeof(C.uint(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func MultipleCUint(page []byte) *MultiCUint {
	if len(page)%int(cUintSize) != 0 {
		panic("argument is not a page of C.uint values")
	}

	return &MultiCUint{page}
}

// Len implements FixedMultiple.
func (m *MultiCUint) Len() int {
	return len(m.page) / int(cUintSize)
}

// Stride implements FixedMultiple.
func (m *MultiCUint) Stride() int {
	return int(cUintSize)
}

// Size implements FixedMultiple.
func (m *MultiCUint) Size() int {
	return len(m.page)
}

// Page implements FixedMultiple.
func (m *MultiCUint) Page() []byte {
	return m.page
}

// CUint returns the uint value at index i.
func (m *MultiCUint) CUint(i int) CUintData {
	var x CUintData
	copy(x[:], m.page[i*int(cUintSize):(i+1)*int(cUintSize)])
	return x
}

// Append returns the MultiCUint result of appending x to m.
func (m *MultiCUint) Append(x CUintData) *MultiCUint {
	return &MultiCUint{append(m.page, x[:]...)}
}

// CUintData contains an unsigned integer with size of a C.uint.
type CUintData [cUintSize]byte

// CUint returns a CUintData containing the value C.uint(x).  It is the
// caller's responsibility to check for any potential overflow using the
// function CanFitInCUint.
//
// Applications on 64-bit architectures that want to store a 64-bit unsigned
// value should use Uintptr type instead of Uint.
func CUint(x uint) CUintData {
	return cUintData(C.uint(x))
}

func cUintData(x C.uint) CUintData {
	return *(*CUintData)(unsafe.Pointer(&x))
}

func (v CUintData) cuint() C.uint {
	return *(*C.uint)(unsafe.Pointer(&v))
}

// Uint returns contained data as a uint value.  It is the callers
// responsibility to check for overflow using function CanFitInUint.
func (v CUintData) Uint() uint {
	return uint(v.cuint())
}

// cuint is a helper type for tests because tests cannot import C
type cuint C.uint

func (x cuint) C() C.uint {
	return C.uint(x)
}
