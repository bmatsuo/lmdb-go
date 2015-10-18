package lmdbcayley

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/bmatsuo/lmdb-go/exp/graph/lmdbcayley/qstest"
	"github.com/bmatsuo/lmdb-go/exp/graph/lmdbcayley/qstest/testrunner"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"golang.org/x/net/context"
)

func init() {
	Runner = testrunner.New(context.Background())
	Runner.Stage = Impl
}

var Runner *testrunner.TestRunner

type tmpDirKey struct{}

var Impl = &qstest.QuadStoreImpl{
	Name: QuadStoreType,
	NewArgs: func(ctx context.Context, name string) (string, graph.Options, error) {
		vars := testrunner.ContextVars(ctx)
		tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
		if err != nil {
			return "", nil, fmt.Errorf("temporary directory: %v", err)
		}
		vars[tmpDirKey{}] = tmpDir
		err = graph.InitQuadStore(QuadStoreType, tmpDir, nil)
		if err != nil {
			os.RemoveAll(tmpDir)
			return "", nil, err
		}
		return tmpDir, nil, err
	},
	Close: func(ctx context.Context, name string) {
		vars := testrunner.ContextVars(ctx)
		tmpDir, ok := vars[tmpDirKey{}].(string)
		if ok {
			os.RemoveAll(tmpDir)
		}
		qs := qstest.ContextQuadStore(ctx)
		qs.Close()
	},
}

func TestQuadStoreCreate(t *testing.T) {
	Runner.Run(t, "TestQuadStoreCreate", qstest.TestQuadStoreCreate)
}

func TestQuadStoreLoadFixture(t *testing.T) {
	Runner.Run(t, "TestQuadStoreLoadFixture", qstest.TestQuadStoreLoadFixture)
}

func TestQuadStoreRemoveQuad(t *testing.T) {
	Runner.Run(t, "TestQuadStoreRemoveQuad", qstest.TestQuadStoreRemoveQuad)
}

func TestQuadStoreNodesAllIterator(t *testing.T) {
	Runner.Run(t, "TestQuadStoreNodesAllIterator", qstest.TestQuadStoreNodesAllIterator)
}

func TestQuadStoreQuadsAllIterator(t *testing.T) {
	Runner.Run(t, "TestQuadStoreQuadsAllIterator", qstest.TestQuadStoreQuadsAllIterator)
}

func TestQuadStoreQuadIterator(t *testing.T) {
	Runner.Run(t, "TestQuadStoreQuadIterator", qstest.TestQuadStoreQuadIterator)
}

func TestQuadStoreQuadIteratorAnd(t *testing.T) {
	Runner.Run(t, "TestQuadStoreQuadIteratorAnd", qstest.TestQuadStoreQuadIteratorAnd)
}

func TestQuadStoreQuadIteratorReset(t *testing.T) {
	Runner.Run(t, "TestQuadStoreQuadIteratorReset", qstest.TestQuadStoreQuadIteratorReset)
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
