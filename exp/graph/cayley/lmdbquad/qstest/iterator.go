package qstest

import (
	"sort"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

// IterateNames returns the names for all remanaing elements in the result set
// of it.
func IterateNames(qs graph.QuadStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		res = append(res, qs.NameOf(it.Result()))
	}
	sort.Strings(res)
	return res
}

// IterateQuads returns the quads for all remanaing elements in the result set
// of it.
func IterateQuads(qs graph.QuadStore, it graph.Iterator) []quad.Quad {
	var res ordered
	for graph.Next(it) {
		res = append(res, qs.Quad(it.Result()))
	}
	sort.Sort(res)
	return res
}

// SortedQuads returns a sorted copy of quads.
func SortedQuads(quads []quad.Quad) []quad.Quad {
	o := make(ordered, len(quads))
	copy(o, ordered(quads))
	sort.Sort(o)
	return o
}

type ordered []quad.Quad

func (o ordered) Len() int { return len(o) }
func (o ordered) Less(i, j int) bool {
	switch {
	case o[i].Subject < o[j].Subject,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate < o[j].Predicate,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object < o[j].Object,

		o[i].Subject == o[j].Subject &&
			o[i].Predicate == o[j].Predicate &&
			o[i].Object == o[j].Object &&
			o[i].Label < o[j].Label:

		return true

	default:
		return false
	}
}
func (o ordered) Swap(i, j int) { o[i], o[j] = o[j], o[i] }
