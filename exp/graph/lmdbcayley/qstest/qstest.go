package qstest

import (
	"fmt"
	"testing"

	"github.com/google/cayley/graph"
	"golang.org/x/net/context"
)

// Func is a test function executed by a Runner.
type Func func(t *testing.T, ctx context.Context, qs graph.QuadStore)

type implVarsKey struct{}

// ContextVars retrieves map that QuadStoreImpl functions can use to share
// information.
func ContextVars(ctx context.Context) map[interface{}]interface{} {
	m, _ := ctx.Value(implVarsKey{}).(map[interface{}]interface{})
	return m
}

func contextWithImplVars(ctx context.Context) context.Context {
	return context.WithValue(ctx, implVarsKey{}, map[interface{}]interface{}{})
}

// QuadStoreImpl defines operations that allow generated tests to run against a
// graph.QuadStore implementation..
type QuadStoreImpl struct {
	Name    string
	NewArgs func(ctx context.Context, name string) (path string, opt graph.Options, err error)
	Close   func(ctx context.Context, name string)
}

func (impl *QuadStoreImpl) close(ctx context.Context, name string) {
	if impl.Close != nil {
		impl.Close(ctx, name)
	}
}

// Runner runs tests with a QuadStoreImpl
type Runner struct {
	C context.Context
	Q *QuadStoreImpl
}

// NewRunner is a convenience method for allocating and initialing a Runner.
func NewRunner(ctx context.Context, impl *QuadStoreImpl) *Runner {
	return &Runner{
		C: ctx,
		Q: impl,
	}
}

func (r *Runner) initTest(ctx context.Context, name string) (context.Context, graph.QuadStore, error) {
	ctx = contextWithImplVars(ctx)
	path, opt, err := r.Q.NewArgs(ctx, name)
	if err != nil {
		return ctx, nil, fmt.Errorf("initializing %s: %v", name, err)
	}

	qs, err := graph.NewQuadStore(r.Q.Name, path, opt)
	if err != nil {
		return ctx, nil, err
	}

	return ctx, qs, nil
}

// Run runs a Func with t
func (r *Runner) Run(t *testing.T, name string, fn Func) {
	ctx := contextWithImplVars(r.C)
	ctx, qs, err := r.initTest(ctx, name)
	defer r.Q.close(ctx, name)
	if err != nil {
		t.Error(err)
		return
	}

	fn(t, ctx, qs)
}
