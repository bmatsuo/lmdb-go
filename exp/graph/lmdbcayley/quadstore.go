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
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"os"
	"sync"

	"github.com/barakmich/glog"
	"github.com/bmatsuo/lmdb-go/lmdb"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/proto"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       upgradeLMDB,
		InitFunc:          createNewLMDB,
		IsPersistent:      true,
	})
}

var (
	errNoDB = errors.New("lmdb: db is missing")
)

var (
	hashPool = sync.Pool{
		New: func() interface{} { return sha1.New() },
	}
	hashSize         = sha1.Size
	localFillPercent = 0.7
)

const (
	// QuadStoreType is used to select a QuadStore backend to Cayley.
	QuadStoreType = "lmdb"
)

// Token ??
type Token struct {
	db  string
	key []byte
}

func token(db string, key []byte) *Token {
	return &Token{
		db:  db,
		key: key,
	}
}

// Key ??
func (t *Token) Key() interface{} {
	return fmt.Sprint(t.db, t.key)
}

// QuadStore ??
type QuadStore struct {
	env     *lmdb.Env
	path    string
	open    bool
	size    int64
	horizon int64
	version int64

	dbis    map[string]lmdb.DBI
	logDBI  lmdb.DBI
	nodeDBI lmdb.DBI
	metaDBI lmdb.DBI
}

func createLMDB(path string, opt graph.Options) (env *lmdb.Env, err error) {
	err = os.Mkdir(path, 0700)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	env, err = lmdb.NewEnv()
	if err != nil {
		return env, err
	}
	defer func() {
		if err != nil {
			env.Close()
			env = nil
		}
	}()

	maxreaders, _, err := opt.IntKey("maxreaders")
	if err != nil {
		return nil, err
	}
	if maxreaders > 0 {
		err = env.SetMaxReaders(maxreaders)
		if err != nil {
			return nil, err
		}
	}

	maxdbs, _, err := opt.IntKey("maxdbs")
	if err != nil {
		return nil, err
	}
	if maxdbs == 0 {
		maxdbs = 7
	}
	err = env.SetMaxDBs(maxdbs)
	if err != nil {
		return nil, err
	}

	mapsize, _, err := opt.IntKey("mapsize")
	if err != nil {
		return nil, err
	}
	err = env.SetMapSize(int64(mapsize))
	if err != nil {
		return nil, err
	}

	var flags uint
	dbnosync, _, err := opt.BoolKey("nosync")
	if err != nil {
		return nil, err
	}
	if dbnosync {
		flags |= lmdb.NoSync
	}
	err = env.Open(path, flags, 0600)
	if err != nil {
		return nil, err
	}

	return env, nil
}

func createNewLMDB(path string, opt graph.Options) error {
	env, err := createLMDB(path, opt)
	if err != nil {
		glog.Errorf("Error: couldn't create LMDB environment: %v", err)
		return err
	}
	defer env.Close()

	qs := &QuadStore{}
	qs.env = env
	err = qs.createDBIs()
	if err != nil {
		return err
	}
	err = setVersionLMDB(qs.env, qs.metaDBI, latestDataVersion)
	if err != nil {
		return err
	}
	qs.Close()
	return nil
}

func newQuadStore(path string, options graph.Options) (graph.QuadStore, error) {
	env, err := createLMDB(path, options)
	if err != nil {
		glog.Errorln("Error, couldn't open! ", err)
		return nil, err
	}

	var qs QuadStore
	qs.env = env
	err = qs.openDBIs()
	if lmdb.IsNotFound(err) {
		return nil, errors.New("lmdb: quadstore has not been initialised")
	}

	err = qs.getMetadata()
	if err != nil {
		return nil, err
	}
	if qs.version != latestDataVersion {
		return nil, errors.New("lmdb: data version is out of date. Run cayleyupgrade for your config to update the data")
	}

	return &qs, nil
}

func (qs *QuadStore) _openDBIs(flags uint) error {
	return qs.env.Update(func(tx *lmdb.Txn) (err error) {
		createdb := func(name string) (dbi lmdb.DBI) {
			if err != nil {
				return 0
			}
			dbi, err = tx.OpenDBI(name, flags)
			if err == nil {
				if qs.dbis == nil {
					qs.dbis = map[string]lmdb.DBI{}
				}
				qs.dbis[name] = dbi
			}
			return dbi
		}
		for _, index := range [][4]quad.Direction{spo, osp, pos, cps} {
			createdb(dbFor(index))
		}
		qs.logDBI = createdb(logDB)
		qs.nodeDBI = createdb(nodeDB)
		qs.metaDBI = createdb(metaDB)
		return err
	})
}

func (qs *QuadStore) openDBIs() error {
	return qs._openDBIs(0)
}

func (qs *QuadStore) createDBIs() error {
	return qs._openDBIs(lmdb.Create)
}

func setVersionLMDB(env *lmdb.Env, metadbi lmdb.DBI, version int64) error {
	return env.Update(func(tx *lmdb.Txn) error {
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, version)
		if err != nil {
			glog.Errorf("Couldn't convert version!")
			return err
		}
		werr := tx.Put(metadbi, []byte("version"), buf.Bytes(), 0)
		if werr != nil {
			glog.Error("Couldn't write version!")
			return werr
		}
		return nil
	})
}

// Size ??
func (qs *QuadStore) Size() int64 {
	return qs.size
}

// Horizon ??
func (qs *QuadStore) Horizon() graph.PrimaryKey {
	return graph.NewSequentialKey(qs.horizon)
}

func (qs *QuadStore) createDeltaKeyFor(id int64) []byte {
	return []byte(fmt.Sprintf("%018x", id))
}

func dbFor(d [4]quad.Direction) string {
	p := [4]byte{d[0].Prefix(), d[1].Prefix(), d[2].Prefix(), d[3].Prefix()}
	return string(p[:])
}

func hashOf(s string) []byte {
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	key := make([]byte, 0, hashSize)
	h.Write([]byte(s))
	key = h.Sum(key)
	return key
}

func (qs *QuadStore) createKeyFor(d [4]quad.Direction, q quad.Quad) []byte {
	key := make([]byte, 0, (hashSize * 4))
	key = append(key, hashOf(q.Get(d[0]))...)
	key = append(key, hashOf(q.Get(d[1]))...)
	key = append(key, hashOf(q.Get(d[2]))...)
	key = append(key, hashOf(q.Get(d[3]))...)
	return key
}

func (qs *QuadStore) createValueKeyFor(s string) []byte {
	key := make([]byte, 0, hashSize)
	key = append(key, hashOf(s)...)
	return key
}

var (
	// Short hand for direction permutations.
	spo = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	osp = [4]quad.Direction{quad.Object, quad.Subject, quad.Predicate, quad.Label}
	pos = [4]quad.Direction{quad.Predicate, quad.Object, quad.Subject, quad.Label}
	cps = [4]quad.Direction{quad.Label, quad.Predicate, quad.Subject, quad.Object}

	// Byte arrays for each db name.
	spoDB = dbFor(spo)
	ospDB = dbFor(osp)
	posDB = dbFor(pos)
	cpsDB = dbFor(cps)
)

const (
	logDB  = "log"
	nodeDB = "node"
	metaDB = "meta"
)

func deltaToProto(delta graph.Delta) proto.LogDelta {
	var newd proto.LogDelta
	newd.ID = uint64(delta.ID.Int())
	newd.Action = int32(delta.Action)
	newd.Timestamp = delta.Timestamp.UnixNano()
	newd.Quad = &proto.Quad{
		Subject:   delta.Quad.Subject,
		Predicate: delta.Quad.Predicate,
		Object:    delta.Quad.Object,
		Label:     delta.Quad.Label,
	}
	return newd
}

// ApplyDeltas ??
func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	oldSize := qs.size
	oldHorizon := qs.horizon
	err := qs.env.Update(func(tx *lmdb.Txn) error {
		tx.RawRead = true

		resizeMap := make(map[string]int64)
		sizeChange := int64(0)
		for _, d := range deltas {
			if d.Action != graph.Add && d.Action != graph.Delete {
				return errors.New("lmdb: invalid action")
			}
			p := deltaToProto(d)
			bytes, err := p.Marshal()
			if err != nil {
				return err
			}
			err = tx.Put(qs.logDBI, qs.createDeltaKeyFor(d.ID.Int()), bytes, 0)
			if err != nil {
				return err
			}
		}
		for _, d := range deltas {
			err := qs.buildQuadWriteLMDB(tx, d.Quad, d.ID.Int(), d.Action == graph.Add)
			if err != nil {
				if err == graph.ErrQuadExists && ignoreOpts.IgnoreDup {
					continue
				}
				if err == graph.ErrQuadNotExist && ignoreOpts.IgnoreMissing {
					continue
				}
				return err
			}
			delta := int64(1)
			if d.Action == graph.Delete {
				delta = int64(-1)
			}
			resizeMap[d.Quad.Subject] += delta
			resizeMap[d.Quad.Predicate] += delta
			resizeMap[d.Quad.Object] += delta
			if d.Quad.Label != "" {
				resizeMap[d.Quad.Label] += delta
			}
			sizeChange += delta
			qs.horizon = d.ID.Int()
		}
		for k, v := range resizeMap {
			if v != 0 {
				err := qs.UpdateValueKeyBy(k, v, tx)
				if err != nil {
					return err
				}
			}
		}
		qs.size += sizeChange
		return qs.WriteHorizonAndSize(tx)
	})

	if err != nil {
		glog.Error("Couldn't write to DB for Delta set. Error: ", err)
		qs.horizon = oldHorizon
		qs.size = oldSize
		return err
	}
	return nil
}

func (qs *QuadStore) buildQuadWriteLMDB(tx *lmdb.Txn, q quad.Quad, id int64, isAdd bool) error {
	var entry proto.HistoryEntry
	dbi := qs.dbis[spoDB]
	data, err := tx.Get(dbi, qs.createKeyFor(spo, q))
	if err == nil {
		// We got something.
		err := entry.Unmarshal(data)
		if err != nil {
			return err
		}
	}

	if isAdd && len(entry.History)%2 == 1 {
		glog.Errorf("attempt to add existing quad %v: %#v", entry, q)
		return graph.ErrQuadExists
	}
	if !isAdd && len(entry.History)%2 == 0 {
		glog.Errorf("attempt to delete non-existent quad %v: %#v", entry, q)
		return graph.ErrQuadNotExist
	}

	entry.History = append(entry.History, uint64(id))

	bytes, err := entry.Marshal()
	if err != nil {
		glog.Errorf("Couldn't write to buffer for entry %#v: %s", entry, err)
		return err
	}
	for _, index := range [][4]quad.Direction{spo, osp, pos, cps} {
		if index == cps && q.Get(quad.Label) == "" {
			continue
		}
		dbi = qs.dbis[dbFor(index)]
		err = tx.Put(dbi, qs.createKeyFor(index, q), bytes, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateValueKeyBy ??
func (qs *QuadStore) UpdateValueKeyBy(name string, amount int64, tx *lmdb.Txn) error {
	value := proto.NodeData{
		Name:  name,
		Size_: amount,
	}
	key := qs.createValueKeyFor(name)
	data, err := tx.Get(qs.nodeDBI, key)
	if err == nil {
		// Node exists in the database -- unmarshal and update.
		var oldvalue proto.NodeData
		err := oldvalue.Unmarshal(data)
		if err != nil {
			glog.Errorf("Error: couldn't reconstruct value: %v", err)
			return err
		}
		oldvalue.Size_ += amount
		value = oldvalue
	}

	// Are we deleting something?
	if value.Size_ <= 0 {
		value.Size_ = 0
	}

	// Repackage and rewrite.
	bytes, err := value.Marshal()
	if err != nil {
		glog.Errorf("Couldn't write to buffer for value %s: %s", name, err)
		return err
	}
	err = tx.Put(qs.nodeDBI, key, bytes, 0)
	return err
}

// WriteHorizonAndSize ??
func (qs *QuadStore) WriteHorizonAndSize(tx *lmdb.Txn) error {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, qs.size)
	if err != nil {
		glog.Errorf("Couldn't convert size!")
		return err
	}
	werr := tx.Put(qs.metaDBI, []byte("size"), buf.Bytes(), 0)
	if werr != nil {
		glog.Error("Couldn't write size!")
		return werr
	}
	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, qs.horizon)

	if err != nil {
		glog.Errorf("Couldn't convert horizon!")
	}

	werr = tx.Put(qs.metaDBI, []byte("horizon"), buf.Bytes(), 0)

	if werr != nil {
		glog.Error("Couldn't write horizon!")
		return werr
	}
	return err
}

// Close ??
func (qs *QuadStore) Close() {
	qs.env.Update(qs.WriteHorizonAndSize)
	qs.env.Close()
	qs.open = false
}

// Quad ??
func (qs *QuadStore) Quad(k graph.Value) quad.Quad {
	var d proto.LogDelta
	tok := k.(*Token)
	err := qs.env.View(func(tx *lmdb.Txn) (err error) {
		tx.RawRead = true

		dbi := qs.dbis[tok.db]
		data, _ := tx.Get(dbi, tok.key)
		if data == nil {
			return nil
		}
		var in proto.HistoryEntry
		err = in.Unmarshal(data)
		if err != nil {
			return err
		}
		if len(in.History) == 0 {
			return nil
		}
		data, _ = tx.Get(qs.logDBI, qs.createDeltaKeyFor(int64(in.History[len(in.History)-1])))
		if data == nil {
			// No harm, no foul.
			return nil
		}
		return d.Unmarshal(data)
	})
	if err != nil {
		glog.Error("Error getting quad: ", err)
		return quad.Quad{}
	}
	if d.Quad == nil {
		glog.Error("Unable to get quad: ", err)
		return quad.Quad{}
	}
	return quad.Quad{
		d.Quad.Subject,
		d.Quad.Predicate,
		d.Quad.Object,
		d.Quad.Label,
	}
}

// ValueOf ??
func (qs *QuadStore) ValueOf(s string) graph.Value {
	return token(nodeDB, qs.createValueKeyFor(s))
}

func (qs *QuadStore) valueDataLMDB(t *Token) proto.NodeData {
	var out proto.NodeData
	if glog.V(3) {
		glog.V(3).Infof("%s %v", string(t.db), t.key)
	}
	err := qs.env.View(func(tx *lmdb.Txn) (err error) {
		tx.RawRead = true

		dbi := qs.dbis[t.db]
		data, err := tx.Get(dbi, t.key)
		if err == nil {
			return out.Unmarshal(data)
		}
		return nil
	})
	if err != nil {
		glog.Errorln("Error: couldn't get value")
		return proto.NodeData{}
	}
	return out
}

// NameOf ??
func (qs *QuadStore) NameOf(k graph.Value) string {
	if k == nil {
		glog.V(2).Info("k was nil")
		return ""
	}
	return qs.valueDataLMDB(k.(*Token)).Name
}

// SizeOf ??
func (qs *QuadStore) SizeOf(k graph.Value) int64 {
	if k == nil {
		return -1
	}
	return int64(qs.valueDataLMDB(k.(*Token)).Size_)
}

func (qs *QuadStore) getInt64ForMetaKey(tx *lmdb.Txn, key string, empty int64) (int64, error) {
	var out int64
	data, err := tx.Get(qs.metaDBI, []byte(key))
	if lmdb.IsNotFound(err) {
		return empty, nil
	}
	if err != nil {
		return 0, err
	}
	buf := bytes.NewBuffer(data)
	err = binary.Read(buf, binary.LittleEndian, &out)
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (qs *QuadStore) getMetadata() error {
	return qs.env.View(func(tx *lmdb.Txn) (err error) {
		tx.RawRead = true

		qs.size, err = qs.getInt64ForMetaKey(tx, "size", 0)
		if err != nil {
			return err
		}
		qs.version, err = qs.getInt64ForMetaKey(tx, "version", nilDataVersion)
		if err != nil {
			return err
		}
		qs.horizon, err = qs.getInt64ForMetaKey(tx, "horizon", 0)
		return err
	})
}

// QuadIterator ??
func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	var db string
	switch d {
	case quad.Subject:
		db = spoDB
	case quad.Predicate:
		db = posDB
	case quad.Object:
		db = ospDB
	case quad.Label:
		db = cpsDB
	default:
		panic("unreachable " + d.String())
	}
	return NewIterator(db, d, val, qs)
}

// NodesAllIterator ??
func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(nodeDB, quad.Any, qs)
}

// QuadsAllIterator ??
func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(posDB, quad.Predicate, qs)
}

// QuadDirection ??
func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	v := val.(*Token)
	offset := PositionOf(v, d, qs)
	if offset != -1 {
		return &Token{
			db:  nodeDB,
			key: v.key[offset : offset+hashSize],
		}
	}
	return qs.ValueOf(qs.Quad(v).Get(d))
}

func compareTokens(a, b graph.Value) bool {
	atok := a.(*Token)
	btok := b.(*Token)
	return bytes.Equal(atok.key, btok.key) && atok.db == btok.db
}

// FixedIterator ??
func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(compareTokens)
}

// Type ??
func (qs *QuadStore) Type() string {
	return QuadStoreType
}
