package lmdbcayley

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/bmatsuo/cayley/graph/graphtest/qstest"
	"github.com/google/cayley/graph"
)

func BenchmarkTokenKey(b *testing.B) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		b.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	err = graph.InitQuadStore("lmdb", tmpDir, nil)
	if err != nil {
		b.Error(err)
		return
	}
	qs, err := graph.NewQuadStore("lmdb", tmpDir, nil)
	if err != nil {
		b.Error(err)
		return
	}
	defer qs.Close()

	_, err = qstest.WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		b.Errorf("Unexpected error writing fixures: %v", err)
	}

	BValueKey(b, qs.NodesAllIterator())
}

// BValueKey benchmarks the Key() method of random elements from it.
func BValueKey(b *testing.B, it graph.Iterator) {
	// interface used by several packages to identify graph.Value types
	type keyer interface {
		Key() interface{}
	}

	var ks []keyer
	vals, err := iterateValues(it)
	if err != nil {
		b.Errorf("iterate: %v", err)
		return
	}
	for _, v := range vals {
		if k, ok := v.(keyer); ok {
			ks = append(ks, k)
		}
	}
	if len(ks) == 0 {
		b.Error("cannot key any items")
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := ks[rand.Intn(len(ks))]
		k.Key()
	}
	b.StopTimer()
}

func iterateValues(it graph.Iterator) ([]graph.Value, error) {
	var vals []graph.Value
	for graph.Next(it) {
		v := it.Result()
		vals = append(vals, v)
	}
	return vals, it.Err()
}
