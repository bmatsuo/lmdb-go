package qstest

import (
	"testing"

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

	w, err := writer.NewSingleReplication(qs, nil)
	if err != nil {
		t.Errorf("Unexpected error creating writer: %v", err)
		return
	}
	w.RemoveQuad(quad.Quad{
		Subject:   "A",
		Predicate: "follows",
		Object:    "B",
		Label:     "",
	})

	if s := qs.Size(); s != int64(fixsize)-1 {
		t.Errorf("Unexpected quadstore size after RemoveQuad, got:%d expect:%d", s, fixsize-1)
	}
}
