package lmdbcayley

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/writer"
)

func BenchmarkTokenKey(b *testing.B) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		b.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	err = createNewLMDB(tmpDir, nil)
	if err != nil {
		b.Fatal("Failed to create LMDB database.", err)
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		b.Error("Failed to create LMDB QuadStore.")
	}

	w, err := writer.NewSingleReplication(qs, nil)
	if err != nil {
		b.Errorf("Failed to create writer: %v", err)
	}
	err = w.AddQuadSet(makeQuadSet())
	if err != nil {
		b.Errorf("Failed to write quad: %v", err)
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
	vals, err := readAllValues(it)
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

func readAllValues(it graph.Iterator) ([]graph.Value, error) {
	var vals []graph.Value
	for graph.Next(it) {
		v := it.Result()
		vals = append(vals, v)
	}
	return vals, it.Err()
}
