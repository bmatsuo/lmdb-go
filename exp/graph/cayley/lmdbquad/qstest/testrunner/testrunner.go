package testrunner

import (
	"testing"

	"golang.org/x/net/context"
)

// Func is a test function executed by a TestRunner.
type Func func(t *testing.T, ctx context.Context)

// Vars provides mutable shared storage for a test.
type Vars map[interface{}]interface{}

type varsKey struct{}

// ContextVars retrieves map that QuadStoreImpl functions can use to share
// information.
func ContextVars(ctx context.Context) Vars {
	m, _ := ctx.Value(varsKey{}).(Vars)
	return m
}

func contextWithVars(ctx context.Context) context.Context {
	return context.WithValue(ctx, varsKey{}, Vars{})
}

// Stage performs application specific setup and teardown for a test.
type Stage interface {
	Setup(ctx context.Context, name string) error
	Teardown(ctx context.Context, name string)
}

// TestRunner runs tests with a QuadStoreImpl
type TestRunner struct {
	Context context.Context
	Stage   Stage
}

// New is a convenience method for allocating and initializing a TestRunner.
func New(ctx context.Context) *TestRunner {
	return &TestRunner{
		Context: ctx,
	}
}

// Run runs a Func with t
func (r *TestRunner) Run(t *testing.T, name string, fn Func) {
	ctx := contextWithVars(r.Context)
	if !r.setup(t, ctx, name) {
		return
	}
	defer r.teardown(ctx, name)
	fn(t, ctx)
}

func (r *TestRunner) setup(t *testing.T, ctx context.Context, name string) (ok bool) {
	if r.Stage != nil {
		err := r.Stage.Setup(ctx, name)
		if err != nil {
			t.Error(err)
			return false
		}
	}
	return true
}

func (r *TestRunner) teardown(ctx context.Context, name string) {
	if r.Stage != nil {
		r.Stage.Teardown(ctx, name)
	}
}
