package lmdb

import (
	"testing"
	"unsafe"
)

func TestUint(t *testing.T) {
	const BitWidth = gouintSize
	const CBitWidth = uintSize
	for i := uint(0); i < uint(BitWidth); i++ {
		x := uint(1 << i)
		cx := Uint(x)
		_x, ok := GetUint(dataToBytes(cx))
		if !ok {
			t.Errorf("GetUint(Uint(%x)) == false", x)
		}
		if _x != x {
			t.Errorf("GetUint(Uint(%x)) != %x (%x)", x, x, _x)
		}
		_x = cx.Uint()
		if _x != x {
			t.Errorf("Uint(%x).Uint() != %x (%x)", x, x, _x)
		}
	}

	for i := uint(0); i < uint(CBitWidth); i++ {
		x := cuint(1 << i)
		var cx UintData
		*(*cuint)(unsafe.Pointer(&cx[0])) = x
		_x, ok := GetUint(dataToBytes(&cx))
		if !ok {
			t.Errorf("GetUint(C.uint(%x)) == false", x)
		}
		if cuint(_x) != x {
			t.Errorf("C.uint(GetUint(C.uint(%x))) != C.uint(%x) (C.uint(%x))", x, x, _x)
		}
	}
}

func TestUintMulti(t *testing.T) {
	const BitWidth = unsafe.Sizeof(uint(0))
	var xs []uint
	for i := uint(0); i < uint(BitWidth); i++ {
		xs = append(xs, 1<<i)
	}
	m := &UintMulti{}
	for i := range xs {
		m = m.Append(xs[i])
	}
	for i := range xs {
		x := m.Uint(i)
		if x != xs[i] {
			t.Errorf("%x != %x (index %d)", x, xs[i], i)
		}
	}
}
