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
	"errors"
	"fmt"

	"github.com/barakmich/glog"
	"github.com/bmatsuo/lmdb-go/lmdb"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/proto"
	"github.com/google/cayley/quad"
)

var (
	lmdbType    graph.Type
	bufferSize  = 50
	errNotExist = errors.New("quad does not exist")
)

func init() {
	lmdbType = graph.RegisterIterator("lmdb")
}

// Iterator is an implementation of graph.Nexter.
type Iterator struct {
	uid     uint64
	tags    graph.Tagger
	dbi     lmdb.DBI
	db      string
	checkID []byte
	dir     quad.Direction
	qs      *QuadStore
	result  *Token
	buffer  [][]byte
	offset  int
	done    bool
	size    int64
	err     error
}

// NewIterator allocates and initializes a new Iterator that is returned to the
// caller.
func NewIterator(db string, d quad.Direction, value graph.Value, qs *QuadStore) *Iterator {
	tok := value.(*Token)
	if tok.db != nodeDB {
		glog.Error("creating an iterator from a non-node value")
		return &Iterator{done: true}
	}

	it := Iterator{
		uid:  iterator.NextUID(),
		dbi:  qs.nodeDBI,
		db:   db,
		dir:  d,
		qs:   qs,
		size: qs.SizeOf(value),
	}

	it.checkID = make([]byte, len(tok.key))
	copy(it.checkID, tok.key)

	return &it
}

// Type ???
func Type() graph.Type { return lmdbType }

// UID ???
func (it *Iterator) UID() uint64 {
	return it.uid
}

// Reset ???
func (it *Iterator) Reset() {
	it.buffer = nil
	it.offset = 0
	it.done = false
}

// Tagger ??
func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

// TagResults ??
func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

// Clone ??
func (it *Iterator) Clone() graph.Iterator {
	out := NewIterator(it.db, it.dir, token(nodeDB, it.checkID), it.qs)
	out.Tagger().CopyFrom(it)
	return out
}

// Close ??
func (it *Iterator) Close() error {
	it.result = nil
	it.buffer = nil
	it.done = true
	return nil
}

func (it *Iterator) isLiveValue(val []byte) bool {
	var entry proto.HistoryEntry
	entry.Unmarshal(val)
	return len(entry.History)%2 != 0
}

// Next ??
func (it *Iterator) Next() bool {
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
				k, v, err := cur.Get(it.checkID, nil, lmdb.SetRange)
				if err != nil {
					it.buffer = append(it.buffer, nil)
					return errNotExist
				}

				if bytes.HasPrefix(k, it.checkID) {
					if it.isLiveValue(v) {
						var out []byte
						out = make([]byte, len(k))
						copy(out, k)
						it.buffer = append(it.buffer, out)
						i++
					}
				}
			} else {
				k, _, err := cur.Get(last, nil, lmdb.SetKey)
				if err != nil || !bytes.Equal(k, last) {
					return fmt.Errorf("could not pick up after %v", k)
				}
			}
			for i < bufferSize {
				k, v, err := cur.Get(nil, nil, lmdb.Next)
				if err != nil || !bytes.HasPrefix(k, it.checkID) {
					it.buffer = append(it.buffer, nil)
					break
				}
				if !it.isLiveValue(v) {
					continue
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
			if err != errNotExist {
				glog.Errorf("Error nexting in database: %v", err)
				it.err = err
			}
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

// Err ??
func (it *Iterator) Err() error {
	return it.err
}

// Result ??
func (it *Iterator) Result() graph.Value {
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

// NextPath ??
func (it *Iterator) NextPath() bool {
	return false
}

// SubIterators are not supported and a nil slice is always returned.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

// PositionOf ??
func PositionOf(tok *Token, d quad.Direction, qs *QuadStore) int {
	if tok.db == spoDB {
		switch d {
		case quad.Subject:
			return 0
		case quad.Predicate:
			return hashSize
		case quad.Object:
			return 2 * hashSize
		case quad.Label:
			return 3 * hashSize
		}
	}
	if tok.db == posDB {
		switch d {
		case quad.Subject:
			return 2 * hashSize
		case quad.Predicate:
			return 0
		case quad.Object:
			return hashSize
		case quad.Label:
			return 3 * hashSize
		}
	}
	if tok.db == ospDB {
		switch d {
		case quad.Subject:
			return hashSize
		case quad.Predicate:
			return 2 * hashSize
		case quad.Object:
			return 0
		case quad.Label:
			return 3 * hashSize
		}
	}
	if tok.db == cpsDB {
		switch d {
		case quad.Subject:
			return 2 * hashSize
		case quad.Predicate:
			return hashSize
		case quad.Object:
			return 3 * hashSize
		case quad.Label:
			return 0
		}
	}
	panic("unreachable")
}

// Contains ??
func (it *Iterator) Contains(v graph.Value) bool {
	val := v.(*Token)
	if val.db == nodeDB {
		return false
	}
	offset := PositionOf(val, it.dir, it.qs)
	if len(val.key) != 0 && bytes.HasPrefix(val.key[offset:], it.checkID) {
		// You may ask, why don't we check to see if it's a valid (not deleted) quad
		// again?
		//
		// We've already done that -- in order to get the graph.Value token in the
		// first place, we had to have done the check already; it came from a Next().
		//
		// However, if it ever starts coming from somewhere else, it'll be more
		// efficient to change the interface of the graph.Value for LMDB to a
		// struct with a flag for isValid, to save another random read.
		//
		// NOTE(bmatsuo):
		// Original lineage from LevelDB backend implementation via BoltDB.
		return true
	}
	return false
}

// Size ??
func (it *Iterator) Size() (int64, bool) {
	return it.size, true
}

// Describe ??
func (it *Iterator) Describe() graph.Description {
	tok := token(it.db, it.checkID)
	return graph.Description{
		UID:       it.UID(),
		Name:      it.qs.NameOf(tok),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      it.size,
		Direction: it.dir,
	}
}

// Type ??
func (it *Iterator) Type() graph.Type { return lmdbType }

// Sorted ??
func (it *Iterator) Sorted() bool { return false }

// Optimize ??
func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

// Stats ??
func (it *Iterator) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     4,
		Size:         s,
	}
}

var _ graph.Nexter = &Iterator{}
