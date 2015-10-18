package qstest

import (
	"fmt"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"
)

func init() {
	Fixtures = &fixtures{qs: allQuads}
	Fixtures.(*fixtures).init()
}

// Fixtures is the set of known QuadSets
var Fixtures interface {
	QuadSet(id string) *QuadSet
}

var allQuads = []*QuadSet{
	{
		"simple",
		[]quad.Quad{
			{"A", "follows", "B", ""},
			{"C", "follows", "B", ""},
			{"C", "follows", "D", ""},
			{"D", "follows", "B", ""},
			{"B", "follows", "F", ""},
			{"F", "follows", "G", ""},
			{"D", "follows", "G", ""},
			{"E", "follows", "F", ""},
			{"B", "status", "cool", "status_graph"},
			{"D", "status", "cool", "status_graph"},
			{"G", "status", "cool", "status_graph"},
		},
	},
}

// WriteFixtureQuadStore writes id QuadSet into qs.
func WriteFixtureQuadStore(qs graph.QuadStore, id string) (int, error) {
	quads := Fixtures.QuadSet(id)
	if quads == nil {
		return 0, fmt.Errorf("unknown fixture: %q", id)
	}
	w, err := writer.NewSingleReplication(qs, nil)
	if err != nil {
		return 0, err
	}
	err = w.AddQuadSet(quads.Quads)
	if err != nil {
		return 0, err
	}
	return len(quads.Quads), nil
}

type fixtures struct {
	qs []*QuadSet
	m  map[string]*QuadSet
}

func (f *fixtures) init() {
	if f.m == nil {
		f.m = map[string]*QuadSet{}
	}
	for _, qs := range f.qs {
		f.m[qs.ID] = qs
	}
}

func (f *fixtures) QuadSet(id string) *QuadSet {
	return f.m[id].copy()
}

// QuadSet represents a static set of quads that can be loaded into a new
// database for testing.
type QuadSet struct {
	ID    string
	Quads []quad.Quad
}

func (qs *QuadSet) copy() *QuadSet {
	if qs == nil {
		return nil
	}
	_qs := &QuadSet{ID: qs.ID}
	_qs.Quads = append(_qs.Quads, qs.Quads...)
	return _qs
}
