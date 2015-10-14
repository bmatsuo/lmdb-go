package lmdbsync

import (
	"reflect"
	"testing"
)

func TestBag(t *testing.T) {
	b1 := Background()
	b2 := BagWith(b1, "k1", "v1")
	b3 := BagWith(b2, "k2", "v2")
	if b1.Value("k1") != nil {
		t.Errorf("unexpected \"k1\" value: %q (!= nil)", b1.Value("k1"))
	}
	if b1.Value("k2") != nil {
		t.Errorf("unexpected \"k2\" value: %q (!= nil)", b1.Value("k2"))
	}
	if !reflect.DeepEqual(b2.Value("k1"), "v1") {
		t.Errorf("unexpected \"k1\" value: %q (!= \"v1\")", b2.Value("k1"))
	}
	if b2.Value("k2") != nil {
		t.Errorf("unexpected \"k2\" value: %q (!= nil)", b2.Value("k2"))
	}
	if !reflect.DeepEqual(b3.Value("k1"), "v1") {
		t.Errorf("unexpected \"k1\" value: %q (!= \"v1\")", b3.Value("k1"))
	}
	if !reflect.DeepEqual(b3.Value("k2"), "v2") {
		t.Errorf("unexpected \"k2\" value: %q (!= \"v2\")", b3.Value("k2"))
	}
}
