package lmdb

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMultiVal(t *testing.T) {
	data := []byte("abcdef")
	m := WrapMulti(data, 2)
	vals := m.Vals()
	if !reflect.DeepEqual(vals, [][]byte{{'a', 'b'}, {'c', 'd'}, {'e', 'f'}}) {
		t.Errorf("unexpected vals: %q", vals)
	}
	size := m.Size()
	if size != 6 {
		t.Errorf("unexpected size: %v (!= %v)", size, 6)
	}
	length := m.Len()
	if length != 3 {
		t.Errorf("unexpected length: %v (!= %v)", length, 3)
	}
	stride := m.Stride()
	if stride != 2 {
		t.Errorf("unexpected stride: %v (!= %v)", stride, 2)
	}
	page := m.Page()
	if !bytes.Equal(page, data) {
		t.Errorf("unexpected page: %v (!= %v)", page, data)
	}
}

func TestVal(t *testing.T) {
	orig := "hey hey"
	val := wrapVal([]byte(orig))

	s := val.String()
	if s != orig {
		t.Errorf("String() not the same as original data: %q", s)
	}

	p := val.Bytes()
	if string(p) != orig {
		t.Errorf("Bytes() not the same as original data: %q", p)
	}
}

func TestValCopy(t *testing.T) {
	orig := "hey hey"
	val := wrapVal([]byte(orig))

	s := val.String()
	if s != orig {
		t.Errorf("String() not the same as original data: %q", s)
	}

	p := val.BytesCopy()
	if string(p) != orig {
		t.Errorf("Bytes() not the same as original data: %q", p)
	}
}
