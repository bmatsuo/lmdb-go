package qstest

import (
	"testing"

	"github.com/google/cayley/graph"
	"golang.org/x/net/context"
)

// TestQuadStoreLoadFixture loads a fixture into a graph.QuadStore and checks
// that the size of the database makes sense.
func TestQuadStoreLoadFixture(t *testing.T, ctx context.Context, qs graph.QuadStore) {
	horizon := qs.Horizon()
	if horizon.Int() != 0 {
		t.Errorf("Unexpected horizon value, got:%d expect:0", horizon.Int())
	}

	fixsize, err = WriteFixtureQuadStore(qs, "simple")
	if err != nil {
		t.Errorf("Unexpected error writing fixures: %v", err)
	}

	if s := qs.Size(); s != 11 {
		t.Errorf("Unexpected quadstore size, got:%d expect:11", s)
	}

	if s := ts2.SizeOf(qs.ValueOf("B")); s != 5 {
		t.Errorf("Unexpected quadstore size, got:%d expect:5", s)
	}

	horizon = qs.Horizon()
	if horizon.Int() != 11 {
		t.Errorf("Unexpected horizon value, got:%d expect:11", horizon.Int())
	}
}
