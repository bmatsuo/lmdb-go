package qstest

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"
	"golang.org/x/net/context"
)

// TestQuadStoreCreate is a bearbones test to create a quadstore
func TestQuadStoreCreate(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)
	if qs == nil {
		t.Errorf("Expected quad store in context")
	}
}

// TestQuadStoreLoadFixture loads a fixture into a graph.QuadStore and checks
// that the size of the database makes sense.
func TestQuadStoreLoadFixture(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	horizon := qs.Horizon()
	if horizon.Int() != 0 {
		t.Errorf("Unexpected horizon value, got:%d expect:0", horizon.Int())
	}

	fixsize, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	if s := qs.Size(); s != int64(fixsize) {
		t.Errorf("Unexpected quadstore size, got:%d expect:%d", s, fixsize)
	}

	horizon = qs.Horizon()
	if horizon.Int() != int64(fixsize) {
		t.Errorf("Unexpected horizon value, got:%d expect:%d", horizon.Int(), fixsize)
	}
}

// TestQuadStoreRemoveQuad loads a fixture into a graph.QuadStore, removes a
// quad, and checks quadstore statistics.
func TestQuadStoreRemoveQuad(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	fixsize, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
		return
	}

	err = removeQuad(qs, []quad.Quad{
		{
			Subject:   "A",
			Predicate: "follows",
			Object:    "B",
			Label:     "",
		},
	})
	if err != nil {
		t.Error(err)
	}

	if s := qs.Size(); s != int64(fixsize)-1 {
		t.Errorf("Unexpected quadstore size after RemoveQuad, got:%d expect:%d", s, fixsize-1)
	}
}

// TestQuadStoreNodesAllIterator iterates the nodes in a fixture and asserts
// the result.
func TestQuadStoreNodesAllIterator(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	_, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	var it graph.Iterator
	it = qs.NodesAllIterator()
	if it == nil {
		t.Fatal("Got nil iterator.")
	}
	defer it.Reset()

	size, _ := it.Size()
	if size <= 0 || size >= 20 {
		t.Errorf("Unexpected size, got:%d expect:(0, 20)", size)
	}
	if typ := it.Type(); typ != graph.All {
		t.Errorf("Unexpected iterator type, got:%v expect:%v", typ, graph.All)
	}
	optIt, changed := it.Optimize()
	if changed || optIt != it {
		t.Errorf("Optimize unexpectedly changed iterator.")
	}

	expect := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	sort.Strings(expect)
	for i := 0; i < 2; i++ {
		got := IterateNames(qs, it)
		sort.Strings(got)
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Unexpected iterated result on repeat %d, got:%v expect:%v", i, got, expect)
		}
		it.Reset()
	}

	for _, pq := range expect {
		if !it.Contains(qs.ValueOf(pq)) {
			t.Errorf("Failed to find and check %q correctly", pq)
		}
	}
	for _, pq := range []string{"baller"} {
		if it.Contains(qs.ValueOf(pq)) {
			t.Errorf("Failed to check %q correctly", pq)
		}
	}
}

// TestQuadStoreQuadsAllIterator iterates the nodes in a fixture and asserts
// the result.
func TestQuadStoreQuadsAllIterator(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	_, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	it := qs.QuadsAllIterator()
	defer it.Reset()
	graph.Next(it)
	t.Logf("%#v\n", it.Result())
	q := qs.Quad(it.Result())
	t.Log(q)
	set := Fixtures.QuadSet("simple").Quads
	var ok bool
	for _, e := range set {
		if e.String() == q.String() {
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("Failed to find %q during iteration, got:%q", q, set)
	}
}

// TestQuadStoreQuadIterator tests the QuadIterator method of a graph.QuadStore
// by issuing several queries against a fixture.
func TestQuadStoreQuadIterator(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	_, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	var tests = []struct {
		dir    quad.Direction
		name   string
		expect []quad.Quad
	}{
		{
			quad.Subject, "C", []quad.Quad{
				{"C", "follows", "B", ""},
				{"C", "follows", "D", ""},
			},
		},
		{
			quad.Object, "F", []quad.Quad{
				{"B", "follows", "F", ""},
				{"E", "follows", "F", ""},
			},
		},
		{
			quad.Predicate, "status", []quad.Quad{
				{"B", "status", "cool", "status_graph"},
				{"D", "status", "cool", "status_graph"},
				{"G", "status", "cool", "status_graph"},
			},
		},
		{
			quad.Label, "status_graph", []quad.Quad{
				{"B", "status", "cool", "status_graph"},
				{"D", "status", "cool", "status_graph"},
				{"G", "status", "cool", "status_graph"},
			},
		},
	}

	for i, test := range tests {
		func() {
			sort.Sort(ordered(test.expect))

			it := qs.QuadIterator(test.dir, qs.ValueOf(test.name))
			defer it.Reset()

			quads := IterateQuads(qs, it)
			if !reflect.DeepEqual(quads, test.expect) {
				t.Errorf("Test %d: Failed to get expected results, got:%q expect:%q", i, quads, test.expect)
			}
		}()
	}

}

// TestQuadStoreQuadIteratorAnd tests the QuadIterator method of a
// graph.QuadStore by issuing several queries against a fixture.
func TestQuadStoreQuadIteratorAnd(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	_, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	var tests = []struct {
		dir     quad.Direction
		name    string
		anddir  quad.Direction
		andname string
		expect  []quad.Quad
	}{
		{
			quad.Subject, "C",
			quad.Any, "",
			[]quad.Quad{
				{"C", "follows", "B", ""},
				{"C", "follows", "D", ""},
			},
		},
		{
			quad.Object, "F",
			quad.Subject, "B",
			[]quad.Quad{
				{"B", "follows", "F", ""},
			},
		},
		{
			quad.Predicate, "status",
			quad.Subject, "G",
			[]quad.Quad{
				{"G", "status", "cool", "status_graph"},
			},
		},
		{
			quad.Label, "status_graph",
			quad.Subject, "B",
			[]quad.Quad{
				{"B", "status", "cool", "status_graph"},
			},
		},
	}

	for i, test := range tests {
		func() {
			it := qs.QuadIterator(test.dir, qs.ValueOf(test.name))
			defer it.Reset()
			and := iterator.NewAnd(qs)
			var other graph.Iterator
			if test.anddir == quad.Any {
				other = qs.QuadsAllIterator()
			} else {
				other = qs.QuadIterator(test.anddir, qs.ValueOf(test.andname))
			}
			defer other.Reset()
			and.AddSubIterator(other)
			and.AddSubIterator(it)
			defer and.Reset()

			quads := IterateQuads(qs, and)
			if !reflect.DeepEqual(quads, test.expect) {
				t.Errorf("Test %d: Failed to get expected results, got:%q expect:%q", i, quads, test.expect)
			}
		}()
	}

}

// TestQuadStoreQuadIteratorReset iterates a set and resets the iterator.  When an
// item is deleted the set is iterated again and should have one fewer item.
func TestQuadStoreQuadIteratorReset(t *testing.T, ctx context.Context) {
	qs := ContextQuadStore(ctx)

	_, err := WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	it := qs.QuadIterator(quad.Subject, qs.ValueOf("E"))

	expect := []quad.Quad{
		{"E", "follows", "F", ""},
	}
	quads := IterateQuads(qs, it)
	if !reflect.DeepEqual(quads, expect) {
		t.Errorf("Failed to get expected results, got:%q expect:%q", quads, expect)
	}
	it.Reset()

	err = removeQuad(qs, []quad.Quad{{"E", "follows", "F", ""}})
	if err != nil {
		t.Error(err)
	}

	quads = IterateQuads(qs, it)
	expect = nil
	if !reflect.DeepEqual(quads, expect) {
		t.Errorf("Failed to get expected results, got:%q expect:%q", quads, expect)
	}
}

func removeQuad(qs graph.QuadStore, q []quad.Quad) error {
	w, err := writer.NewSingleReplication(qs, nil)
	if err != nil {
		return err
	}

	for _, q := range q {
		err = w.RemoveQuad(q)
		if err != nil {
			return fmt.Errorf("removing quad: %v (%v)", err, q)
		}
	}

	return nil
}
