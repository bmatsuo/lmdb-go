package lmdb

import (
	"testing"
	"unsafe"
)

func TestUint(t *testing.T) {
	const BitWidth = goUintSize
	const CBitWidth = cUintSize
	for i := uint(0); i < uint(BitWidth); i++ {
		x := uint(1 << i)
		cx := CUint(x)
		ok := UintCanFit(cx)
		if !ok {
			t.Errorf("getUint(Uint(%x)) == false", x)
		}
		_x := cx.Uint()
		if _x != x {
			t.Errorf("Uint(%x).Uint() != %x (%x)", x, x, _x)
		}
	}

	for i := uint(0); i < uint(CBitWidth); i++ {
		x := cuint(1 << i)
		var cx CUintData
		*(*cuint)(unsafe.Pointer(&cx)) = x
		ok := UintCanFit(cx)
		if !ok {
			t.Errorf("getUint(CUint(%x)) == false", x)
		}
		_x := cx.Uint()
		if cuint(_x) != x {
			t.Errorf("C.uint(CUint(%x).Uint()) != C.uint(%x) (C.uint(%x))", x, x, _x)
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
		x := m.CUint(i).Uint()
		if x != xs[i] {
			t.Errorf("%x != %x (index %d)", x, xs[i], i)
		}
	}
}
