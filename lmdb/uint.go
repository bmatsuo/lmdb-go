package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "lmdb.h"
*/
import "C"

import "unsafe"

// UintMax is the largest value for a C.uint type and the largest value that
// can be stored in a Uint object.  On 64-bit architectures it is likely that
// UintMax is less than the largest value of Go's uint type.
//
// Applications on 64-bit architectures that would like to store a 64-bit
// unsigned value should use the UintptrData type instead of the UintData
// type.
const UintMax = C.UINT_MAX

const uintSize = unsafe.Sizeof(C.uint(0))
const gouintSize = unsafe.Sizeof(uint(0))

// Uint returns a UintData containing the value C.uint(x).  If the value
// passed to Uint is greater than UintMax a runtime panic will occur.
//
// Applications on 64-bit architectures that want to store a 64-bit unsigned
// value should use Uintptr type instead of Uint.
func Uint(x uint) *UintData {
	return newUintData(x)
}

// getUint interprets the bytes of b as a C.uint and returns the uint value.
// getUint returns false if b is not the size of a C.uint or cannot be decoded
// to a uint.
//
// It is the callers responsibility that b is large enough to hold a C.uint.
func getUint(b []byte) (x uint, ok bool) {
	_ = b[uintSize-1]
	x = uint(*(*C.uint)(unsafe.Pointer(&b[0])))
	if uintSize > gouintSize && C.uint(x) != *(*C.uint)(unsafe.Pointer(&b[0])) {
		// overflow
		return 0, false
	}
	return x, true
}

// UintMulti is a FixedMultiple implementation that stores C.uint-sized data
// values that are read from Cursor.GetMultiple or to Cursor.PutMultiple.
type UintMulti struct {
	page []byte
}

var _ FixedMultiple = (*UintMulti)(nil)

// UintMultiple converts a page of contiguous C.uint value into a UintMulti.
// Use this function after calling Cursor.Get with op GetMultiple on a database
// with DupSort|DupFixed|IntegerDup that stores C.uint values.  UintMultiple
// panics if len(page) is not a multiple of unsife.Sizeof(C.uint(0)).
//
// See mdb_cursor_get and MDB_GET_MULTIPLE.
func UintMultiple(page []byte) *UintMulti {
	if len(page)%int(uintSize) != 0 {
		panic("argument is not a page of C.uint values")
	}

	return &UintMulti{page}
}

// Len implements FixedMultiple.
func (m *UintMulti) Len() int {
	return len(m.page) / int(uintSize)
}

// Stride implements FixedMultiple.
func (m *UintMulti) Stride() int {
	return int(uintSize)
}

// Size implements FixedMultiple.
func (m *UintMulti) Size() int {
	return len(m.page)
}

// Page implements FixedMultiple.
func (m *UintMulti) Page() []byte {
	return m.page
}

// Uint returns the uint value at index i.
func (m *UintMulti) Uint(i int) uint {
	data := m.page[i*int(uintSize) : (i+1)*int(uintSize)]
	x := uint(*(*C.uint)(unsafe.Pointer(&data[0])))
	if uintSize > gouintSize && C.uint(x) != *(*C.uint)(unsafe.Pointer(&data[0])) {
		panic(errOverflow)
	}
	return x
}

// Append returns the UintMulti result of appending x to m as C.uint data.  If
// the value passed to Append is greater than UintMax a runtime panic will
// occur.
//
// Applications on 64-bit architectures that want to store a 64-bit unsigned
// value should use UintptrMulti type instead of UintMulti.
func (m *UintMulti) Append(x uint) *UintMulti {
	if uintSize < gouintSize && x > UintMax {
		panic(errOverflow)
	}

	var buf [uintSize]byte
	*(*C.uint)(unsafe.Pointer(&buf[0])) = C.uint(x)
	return &UintMulti{append(m.page, buf[:]...)}
}

// UintData is a Data implementation that contains C.uint-sized data.
type UintData [uintSize]byte

var _ Data = (*UintData)(nil)

func newUintData(x uint) *UintData {
	v := new(UintData)
	v.SetUint(x)
	return v
}

// Uint returns contained data as a uint value.
func (v *UintData) Uint() uint {
	x := *(*C.uint)(unsafe.Pointer(&(*v)[0]))
	if uintSize > gouintSize && C.uint(uint(x)) != x {
		panic(errOverflow)
	}
	return uint(x)
}

// SetUint stores the value of x as a C.uint in v.  The value of x must not be
// greater than UintMax otherwise a runtime panic will occur.
func (v *UintData) SetUint(x uint) {
	if uintSize < gouintSize && x > UintMax {
		panic(errOverflow)
	}

	*(*C.uint)(unsafe.Pointer(&(*v)[0])) = C.uint(x)
}

func (v *UintData) tobytes() []byte {
	return (*v)[:]
}

// cuint is a helper type for tests because tests cannot import C
type cuint C.uint

func (x cuint) C() C.uint {
	return C.uint(x)
}
