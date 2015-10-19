package lmdbcayley

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/bmatsuo/cayley/graph/graphtest/qstest"
	"github.com/bmatsuo/cayley/graph/graphtest/testrunner"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"golang.org/x/net/context"
)

func init() {
	Runner = testrunner.New(context.Background())
	Runner.Stage = Impl
}

//go:generate qstest -runner=Runner -output=quadstore_qstest_test.go

var Runner *testrunner.TestRunner

var Impl = &qstest.QuadStoreImpl{
	Name: QuadStoreType,
	NewArgs: func(ctx context.Context, name string) (string, graph.Options, error) {
		vars := testrunner.ContextVars(ctx)
		tmpDir, err := qstest.MkTempDir(vars, "", "cayley_test")
		if err != nil {
			return "", nil, fmt.Errorf("temporary directory: %v", err)
		}
		err = graph.InitQuadStore(QuadStoreType, tmpDir, nil)
		if err != nil {
			os.RemoveAll(tmpDir)
			return "", nil, err
		}
		return tmpDir, nil, err
	},
	Close: func(ctx context.Context, name string) {
		vars := testrunner.ContextVars(ctx)
		defer qstest.RmTempDir(vars)

		qs := qstest.ContextQuadStore(ctx)
		qs.Close()
	},
}

func TestQuadStoreOptimizeIterator(t *testing.T) {
	Runner.Run(t, "TestQuadStoreOptimizeIterator", testQuadStoreOptimizeIterator)
}

// testQuadStoreOptimizeIterator iterates the nodes in a fixture and asserts
// the result.
func testQuadStoreOptimizeIterator(t *testing.T, ctx context.Context) {
	qs := qstest.ContextQuadStore(ctx)

	_, err := qstest.WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	// With an linksto-fixed pair
	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf("F"))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if newIt.Type() != Type() {
		t.Errorf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), Type())
	}

	newQuads := qstest.IterateQuads(qs, newIt)
	oldQuads := qstest.IterateQuads(qs, oldIt)
	if !reflect.DeepEqual(newQuads, oldQuads) {
		t.Errorf("Optimized iteration does not match original")
	}

	graph.Next(oldIt)
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	graph.Next(newIt)
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	if !reflect.DeepEqual(newResults, oldResults) {
		t.Errorf("Discordant tag results, new:%v old:%v", newResults, oldResults)
	}
}
