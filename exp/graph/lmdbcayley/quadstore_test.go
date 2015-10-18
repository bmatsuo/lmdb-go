package lmdbcayley

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bmatsuo/lmdb-go/exp/graph/lmdbcayley/qstest"
	"github.com/bmatsuo/lmdb-go/exp/graph/lmdbcayley/qstest/testrunner"
	"github.com/google/cayley/graph"
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
