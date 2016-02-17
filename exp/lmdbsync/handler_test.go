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
	env    *Env
	err    error
}

func (h *testHandler) HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error) {
	h.called = true
	h.ctx = ctx
	h.env = env
	h.err = err
	return ctx, err
}

func TestHandlerChain(t *testing.T) {
	ctx := context.Background()
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	var chain1 HandlerChain
	errother := fmt.Errorf("testerr")

	ctx1, err := chain1.HandleTxnErr(ctx, env, errother)
	if err != errother {
		t.Error(err)
	}
	if ctx1 != ctx {
		t.Errorf("unexpected ctx: %#v (!= %#v)", ctx1, ctx)
	}

	chain2 := chain1.Append(&passthroughHandler{})
	ctx2, err := chain2.HandleTxnErr(ctx, env, errother)
	if err != errother {
		t.Error(err)
	}
	if ctx2 != ctx {
		t.Errorf("unexpected ctx: %#v (!= %#v)", ctx2, ctx)
	}
}

type retryHandler struct{}

func (*retryHandler) HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error) {
	return ctx, ErrTxnRetry

}

type passthroughHandler struct{}

func (*passthroughHandler) HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error) {
	return ctx, err

}

func TestMapFullHandler(t *testing.T) {
	ctx := context.Background()
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

	doubleSize := func(size int64) (int64, bool) { return size * 2, true }
	handler := MapFullHandler(doubleSize)

	errother := fmt.Errorf("testerr")
	ctx1, err := handler.HandleTxnErr(ctx, env, errother)
	if ctx1 != ctx {
		t.Errorf("ctx changed: %q (!= %q)", ctx1, ctx)
	}

	errmapfull := &lmdb.OpError{
		Op:    "lmdbsync_test_op",
		Errno: lmdb.MapFull,
	}
	ctx1, err = handler.HandleTxnErr(ctx, env, errmapfull)
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
	ctx := context.Background()
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	handler := MapResizedHandler(2, func(int) time.Duration { return 100 * time.Microsecond })

	errother := fmt.Errorf("testerr")
	_, err = handler.HandleTxnErr(ctx, env, errother)

	errmapresized := &lmdb.OpError{
		Op:    "lmdbsync_test_op",
		Errno: lmdb.MapResized,
	}
	ctx1, err := handler.HandleTxnErr(ctx, env, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
	ctx2, err := handler.HandleTxnErr(ctx1, env, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}

	// after MapResized has been encountered enough times consecutively the
	// handler starts passing MapResized through to the caller.
	_, err = handler.HandleTxnErr(ctx2, env, errmapresized)
	if !lmdb.IsMapResized(err) {
		t.Errorf("unexpected error: %v", err)
	}
	ctx3, err := handler.HandleTxnErr(ctx2, env, errmapresized)
	if !lmdb.IsMapResized(err) {
		t.Errorf("unexpected error: %v", err)
	}

	ctx4, err := handler.HandleTxnErr(ctx3, env, errother)
	if err != errother {
		t.Errorf("unexpected error: %v", err)
	}

	// after encountering an error other than MapResized the handler resets its
	// failure count and will continue attempting to adopt the new map size
	// when MapResized is encountered.
	ctx5, err := handler.HandleTxnErr(ctx4, env, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
	_, err = handler.HandleTxnErr(ctx5, env, errmapresized)
	if err != ErrTxnRetry {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	base := time.Millisecond
	max := 3 * time.Millisecond
	factor := 2.0
	backoff := ExponentialBackoff(base, max, factor)

	const numtest = 100
	for i := 0; i < numtest; i++ {
		n := backoff(0)
		if n < 0 || n > base {
			t.Errorf("unexpected backoff: %v", n)
		}
	}
	for i := 0; i < numtest; i++ {
		n := backoff(1)
		if n < 0 || n > 2*base {
			t.Errorf("unexpected backoff: %v", n)
		}
	}
	for i := 0; i < numtest; i++ {
		n := backoff(2)
		if n < 0 || n > max {
			t.Errorf("unexpected backoff: %v", n)
		}
	}
}
