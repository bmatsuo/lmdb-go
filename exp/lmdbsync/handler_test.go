package lmdbsync

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/bmatsuo/lmdb-go/internal/lmdbtest"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

type testHandler struct {
	called bool
	ctx    context.Context
	err    error
}

func (h *testHandler) HandleTxnErr(ctx context.Context, err error) (context.Context, error) {
	h.called = true
	h.ctx = ctx
	h.err = err
	return ctx, err
}

func TestHandlerChain(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	ctx := withEnv(context.Background(), env)

	var chain1 HandlerChain
	errother := fmt.Errorf("testerr")

	ctx1, err := chain1.HandleTxnErr(ctx, errother)
	if err != errother {
		t.Error(err)
	}
	if ctx1 != ctx {
		t.Errorf("unexpected ctx: %#v (!= %#v)", ctx1, ctx)
	}

	chain2 := chain1.Append(&passthroughHandler{})
	ctx2, err := chain2.HandleTxnErr(ctx, errother)
	if err != errother {
		t.Error(err)
	}
	if ctx2 != ctx {
		t.Errorf("unexpected ctx: %#v (!= %#v)", ctx2, ctx)
	}
}

type retryHandler struct{}

func (*retryHandler) HandleTxnErr(ctx context.Context, err error) (context.Context, error) {
	return ctx, ErrTxnRetry

}

type passthroughHandler struct{}

func (*passthroughHandler) HandleTxnErr(ctx context.Context, err error) (context.Context, error) {
	return ctx, err

}

func TestMapFullHandler(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	info, err := env.Info()
	if err != nil {
		t.Error(err)
		return
	}
	orig := info.MapSize

	ctx := withEnv(context.Background(), env)
	doubleSize := func(size int64) (int64, bool) { return size * 2, true }
	handler := MapFullHandler(doubleSize)

	errother := fmt.Errorf("testerr")
	ctx1, err := handler.HandleTxnErr(ctx, errother)
	if ctx1 != ctx {
		t.Errorf("ctx changed: %q (!= %q)", ctx1, ctx)
	}

	errmapfull := &lmdb.OpError{
		Op:    "lmdbsync_test_op",
		Errno: lmdb.MapFull,
	}
	ctx1, err = handler.HandleTxnErr(ctx, errmapfull)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
	if ctx1 != ctx {
		t.Errorf("ctx changed: %q (!= %q)", ctx1, ctx)
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
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	ctx := withEnv(context.Background(), env)
	handler := MapResizedHandler(2, func(int) time.Duration { return 100 * time.Microsecond })

	errother := fmt.Errorf("testerr")
	_, err = handler.HandleTxnErr(ctx, errother)

	errmapresized := &lmdb.OpError{
		Op:    "lmdbsync_test_op",
		Errno: lmdb.MapResized,
	}
	ctx1, err := handler.HandleTxnErr(ctx, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
	ctx2, err := handler.HandleTxnErr(ctx1, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}

	// after MapResized has been encountered enough times consecutively the
	// handler starts passing MapResized through to the caller.
	_, err = handler.HandleTxnErr(ctx2, errmapresized)
	if !lmdb.IsMapResized(err) {
		t.Errorf("unexpected error: %v", err)
	}
	ctx3, err := handler.HandleTxnErr(ctx2, errmapresized)
	if !lmdb.IsMapResized(err) {
		t.Errorf("unexpected error: %v", err)
	}

	ctx4, err := handler.HandleTxnErr(ctx3, errother)
	if err != errother {
		t.Errorf("unexpected error: %v", err)
	}

	// after encountering an error other than MapResized the handler resets its
	// failure count and will continue attempting to adopt the new map size
	// when MapResized is encountered.
	ctx5, err := handler.HandleTxnErr(ctx4, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
	_, err = handler.HandleTxnErr(ctx5, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
}
