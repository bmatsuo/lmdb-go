package lmdb

import (
	"testing"
	"unsafe"
)

func TestSizetSize(t *testing.T) {
	if sizetSize != unsafe.Sizeof(uintptr(0)) {
		t.Errorf("incorrect assumption about the size of C.size_t")
	}
}

func TestUintptr(t *testing.T) {
	const BitWidth = unsafe.Sizeof(uintptr(0))
	const CBitWidth = unsafe.Sizeof(csizet(0))
	for i := uint(0); i < uint(BitWidth); i++ {
		x := uintptr(1 << i)
		cx := Uintptr(x)
		_x, ok := getUintptr(dataToBytes(cx))
		if !ok {
			t.Errorf("getUintptr(Uintptr(%x)) == false", x)
		}
		if _x != x {
			t.Errorf("getUintptr(Uintptr(%x)) != %x (%x)", x, x, _x)
		}
		_x = cx.Uintptr()
		if _x != x {
			t.Errorf("Uintptr(%x).Uintptr() != %x (%x)", x, x, _x)
		}
	}

	for i := uint(0); i < uint(CBitWidth); i++ {
		x := csizet(1 << i)
		var cx UintptrData
		*(*csizet)(unsafe.Pointer(&cx[0])) = x
		_x, ok := getUintptr(dataToBytes(&cx))
		if !ok {
			t.Errorf("getUintptr(C.size_t(%x)) == false", x)
		}
		if csizet(_x) != x {
			t.Errorf("C.size_t(getUintptr(C.size_t(%x))) != C.size_t(%x) (C.size_t(%x))", x, x, _x)
		}
	}
}

func TestUintptrMulti(t *testing.T) {
	const BitWidth = unsafe.Sizeof(uintptr(0))
	var xs []uintptr
	for i := uint(0); i < uint(BitWidth); i++ {
		xs = append(xs, 1<<i)
	}
	m := &UintptrMulti{}
	for i := range xs {
		m = m.Append(xs[i])
	}
	for i := range xs {
		x := m.Uintptr(i)
		if x != xs[i] {
			t.Errorf("%x != %x (index %d)", x, xs[i], i)
		}
	}
}
