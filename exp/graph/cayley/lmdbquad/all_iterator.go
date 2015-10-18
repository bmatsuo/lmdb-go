// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lmdbcayley

import (
	"bytes"
	"fmt"

	"github.com/barakmich/glog"
	"github.com/bmatsuo/lmdb-go/lmdb"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

// AllIterator is an implementation of graph.Nexter.
type AllIterator struct {
	uid    uint64
	tags   graph.Tagger
	db     string
	dir    quad.Direction
	qs     *QuadStore
	result *Token
	err    error
	buffer [][]byte
	offset int
	done   bool
}

// NewAllIterator allocates and initializes an AllIterator that is returned to
// the caller.
func NewAllIterator(db string, d quad.Direction, qs *QuadStore) *AllIterator {
	return &AllIterator{
		uid: iterator.NextUID(),
		db:  db,
		dir: d,
		qs:  qs,
	}
}

// UID returns iterator UID for it.
func (it *AllIterator) UID() uint64 {
	return it.uid
}

// Reset ???.
func (it *AllIterator) Reset() {
	it.buffer = nil
	it.offset = 0
	it.done = false
}

// Tagger returns the iterator's tagger.
func (it *AllIterator) Tagger() *graph.Tagger {
	return &it.tags
}

// TagResults returns the iterators tags.
func (it *AllIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

// Clone returns an independent copy of it.
func (it *AllIterator) Clone() graph.Iterator {
	out := NewAllIterator(it.db, it.dir, it.qs)
	out.tags.CopyFrom(it)
	return out
}

// Next ???.
func (it *AllIterator) Next() bool {
	if it.done {
		return false
	}
	if len(it.buffer) <= it.offset+1 {
		it.offset = 0
		var last []byte
		if it.buffer != nil {
			last = it.buffer[len(it.buffer)-1]
		}
		it.buffer = make([][]byte, 0, bufferSize)
		err := it.qs.env.View(func(tx *lmdb.Txn) error {
			tx.RawRead = true

			i := 0
			dbi := it.qs.dbis[it.db]
			cur, err := tx.OpenCursor(dbi)
			if err != nil {
				return err
			}

			if last == nil {
				k, _, _ := cur.Get(nil, nil, lmdb.First)
				var out []byte
				out = make([]byte, len(k))
				copy(out, k)
				it.buffer = append(it.buffer, out)
				i++
			} else {
				k, _, _ := cur.Get(last, nil, lmdb.SetKey)
				if !bytes.Equal(k, last) {
					return fmt.Errorf("could not pick up after %v", k)
				}
			}
			for i < bufferSize {
				k, _, _ := cur.Get(nil, nil, lmdb.Next)
				if k == nil {
					it.buffer = append(it.buffer, k)
					break
				}
				var out []byte
				out = make([]byte, len(k))
				copy(out, k)
				it.buffer = append(it.buffer, out)
				i++
			}
			return nil
		})
		if err != nil {
			glog.Error("Error nexting in database: ", err)
			it.err = err
			it.done = true
			return false
		}
	} else {
		it.offset++
	}
	if it.Result() == nil {
		it.done = true
		return false
	}
	return true
}

// Err ???
func (it *AllIterator) Err() error {
	return it.err
}

// Result ???
func (it *AllIterator) Result() graph.Value {
	if it.done {
		return nil
	}
	if it.result != nil {
		return it.result
	}
	if it.offset >= len(it.buffer) {
		return nil
	}
	if it.buffer[it.offset] == nil {
		return nil
	}
	return token(it.db, it.buffer[it.offset])
}

// NextPath ???
func (it *AllIterator) NextPath() bool {
	return false
}

// SubIterators are not supported and a nil slice is always returned.
func (it *AllIterator) SubIterators() []graph.Iterator {
	return nil
}

// Contains ???
// BUG(bmatsuo):
// Contains is surely broken. It just looks for v in the given database.  It
// seems like it should buffer the output, or run the iteration again.
func (it *AllIterator) Contains(v graph.Value) (ok bool) {
	graph.ContainsLogIn(it, v)
	defer func() { graph.ContainsLogOut(it, v, ok) }()

	tok, ok := v.(*Token)
	if !ok {
		return false
	}
	if tok.db != it.db {
		return false
	}

	err := it.qs.env.View(func(tx *lmdb.Txn) (err error) {
		tx.RawRead = true
		_, err = tx.Get(it.qs.dbis[tok.db], tok.key)
		ok = !lmdb.IsNotFound(err)
		return err
	})
	if err != nil {
		return false
	}
	if ok {
		it.result = tok
	}
	return ok

}

// Close ???
func (it *AllIterator) Close() error {
	it.result = nil
	it.buffer = nil
	it.done = true
	return nil
}

// Size ???
func (it *AllIterator) Size() (int64, bool) {
	return it.qs.size, true
}

// Describe ???
func (it *AllIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:       it.UID(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      size,
		Direction: it.dir,
	}
}

// Type ???
func (it *AllIterator) Type() graph.Type { return graph.All }

// Sorted ???
func (it *AllIterator) Sorted() bool { return false }

// Optimize ???
func (it *AllIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

// Stats ???
func (it *AllIterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
	}
}

var _ graph.Nexter = &AllIterator{}
