package lmdbsync

import (
	"fmt"
	"testing"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type testHandler struct {
	called bool
	bag    Bag
	err    error
}

func (h *testHandler) HandleTxnErr(b Bag, err error) (Bag, error) {
	h.called = true
	h.bag = b
	h.err = err
	return b, err
}

func TestMapFullHandler(t *testing.T) {
	env := newEnv(t, 0)
	defer cleanEnv(t, env)

	info, err := env.Info()
	if err != nil {
		t.Error(err)
		return
	}
	orig := info.MapSize

	b := bagWithEnv(Background(), env)
	doubleSize := func(size int64) (int64, bool) { return size * 2, true }
	handler := MapFullHandler(doubleSize)

	errother := fmt.Errorf("testerr")
	b1, err := handler.HandleTxnErr(b, errother)
	if b1 != b {
		t.Errorf("bag changed: %q (!= %q)", b1, b)
	}

	errmapfull := &lmdb.OpError{
		Op:    "lmdbsync_test_op",
		Errno: lmdb.MapFull,
	}
	b1, err = handler.HandleTxnErr(b, errmapfull)
	if err != RetryTxn {
		t.Errorf("unexpected error: %v", err)
	}
	if b1 != b {
		t.Errorf("bag changed: %q (!= %q)", b1, b)
	}

	info, err = env.Info()
	if err != nil {
		t.Error(err)
		return
	}
	if info.MapSize <= orig {
		t.Errorf("unexpected map size: %d (<= %d)", info.MapSize, orig)
	}
}

func TestMapResizedHandler(t *testing.T) {
	env := newEnv(t, 0)
	defer cleanEnv(t, env)

	b := bagWithEnv(Background(), env)
	handler := MapResizedHandler(2, func(int) time.Duration { return 100 * time.Microsecond })

	errother := fmt.Errorf("testerr")
	_, err := handler.HandleTxnErr(b, errother)

	errmapresized := &lmdb.OpError{
		Op:    "lmdbsync_test_op",
		Errno: lmdb.MapResized,
	}
	b1, err := handler.HandleTxnErr(b, errmapresized)
	if err != RetryTxn {
		t.Errorf("unexpected error: %v", err)
	}
	b2, err := handler.HandleTxnErr(b1, errmapresized)
	if err != RetryTxn {
		t.Errorf("unexpected error: %v", err)
	}

	// after MapResized has been encountered enough times consecutively the
	// handler starts passing MapResized through to the caller.
	_, err = handler.HandleTxnErr(b2, errmapresized)
	if !lmdb.IsMapResized(err) {
		t.Errorf("unexpected error: %v", err)
	}
	b3, err := handler.HandleTxnErr(b2, errmapresized)
	if !lmdb.IsMapResized(err) {
		t.Errorf("unexpected error: %v", err)
	}

	b4, err := handler.HandleTxnErr(b3, errother)
	if err != errother {
		t.Errorf("unexpected error: %v", err)
	}

	// after encountering an error other than MapResized the handler resets its
	// failure count and will continue attempting to adopt the new map size
	// when MapResized is encountered.
	b5, err := handler.HandleTxnErr(b4, errmapresized)
	if err != RetryTxn {
		t.Errorf("unexpected error: %v", err)
	}
	_, err = handler.HandleTxnErr(b5, errmapresized)
	if err != RetryTxn {
		t.Errorf("unexpected error: %v", err)
	}
}
