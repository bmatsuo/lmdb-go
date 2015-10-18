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
	runner := testrunner.New(context.Background())
	runner.Stage = Impl
	runner.Run(t, "TestQuadStoreCreate", qstest.TestQuadStoreCreate)
}

func TestQuadStoreLoadFixture(t *testing.T) {
	runner := testrunner.New(context.Background())
	runner.Stage = Impl
	runner.Run(t, "TestQuadStoreLoadFixture", qstest.TestQuadStoreLoadFixture)
}

func TestQuadStoreRemoveQuad(t *testing.T) {
	runner := testrunner.New(context.Background())
	runner.Stage = Impl
	runner.Run(t, "TestQuadStoreRemoveQuad", qstest.TestQuadStoreRemoveQuad)
}
