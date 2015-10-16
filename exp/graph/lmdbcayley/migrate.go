// Copyright 2015 The Cayley Authors. All rights reserved.
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
	"encoding/json"
	"fmt"

	"github.com/barakmich/glog"
	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/boltdb/bolt"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/proto"
)

const latestDataVersion = 2
const nilDataVersion = 1

type upgradeFunc func(*bolt.DB) error

type upgradeFuncLMDB func(*QuadStore) error

var migrateFunctions = []upgradeFunc{
	nil,
	upgrade1To2,
}

var migrateFunctionsLMDB = []upgradeFuncLMDB{
	nil,
	upgrade1To2LMDB,
}

func upgradeLMDB(path string, opts graph.Options) error {
	env, err := createLMDB(path, opts)
	if err != nil {
		glog.Errorln("Error, couldn't open! ", err)
		return err
	}
	defer env.Close()

	qs := &QuadStore{}
	qs.env = env
	err = qs.openDBIs()
	if err != nil {
		return err
	}

	var version int64
	err = env.View(func(tx *lmdb.Txn) (err error) {
		version, err = qs.getInt64ForMetaKey(tx, "version", nilDataVersion)
		return err
	})
	if err != nil {
		glog.Errorln("error:", err)
		return err
	}

	if version == latestDataVersion {
		fmt.Printf("Already at latest version: %d\n", latestDataVersion)
		return nil
	}

	if version > latestDataVersion {
		err := fmt.Errorf("Unknown data version: %d -- upgrade this tool", version)
		glog.Errorln("error:", err)
		return err
	}

	for i := version; i < latestDataVersion; i++ {
		err := migrateFunctionsLMDB[i](qs)
		if err != nil {
			return err
		}
		err = setVersionLMDB(qs.env, qs.metaDBI, i+1)
		if err != nil {
			return err
		}
	}

	return nil
}

type v1ValueData struct {
	Name string
	Size int64
}

type v1IndexEntry struct {
	History []int64
}

func upgrade1To2LMDB(qs *QuadStore) error {
	fmt.Println("Upgrading v1 to v2...")
	err := qs.env.Update(func(tx *lmdb.Txn) (err error) {
		fmt.Println("Upgrading bucket", string(logBucket))

		s := lmdbscan.New(tx, qs.logDBI)
		defer s.Close()

		for s.Scan() {
			var delta graph.Delta
			err := json.Unmarshal(s.Val(), &delta)
			if err != nil {
				return err
			}
			newd := deltaToProto(delta)
			data, err := newd.Marshal()
			if err != nil {
				return err
			}
			err = tx.Put(qs.logDBI, s.Key(), data, 0)
			if err != nil {
				return err
			}
		}

		return s.Err()
	})
	if err != nil {
		return err
	}
	err = qs.env.Update(func(tx *lmdb.Txn) (err error) {
		fmt.Println("Upgrading bucket", string(nodeBucket))

		s := lmdbscan.New(tx, qs.nodeDBI)
		defer s.Close()

		for s.Scan() {
			var vd proto.NodeData
			err := json.Unmarshal(s.Val(), &vd)
			if err != nil {
				return err
			}
			data, err := vd.Marshal()
			if err != nil {
				return err
			}
			err = tx.Put(qs.nodeDBI, s.Key(), data, 0)
			if err != nil {
				return err
			}
		}

		return s.Err()
	})
	if err != nil {
		return err
	}

	for _, bucket := range [4]string{string(spoBucket), string(ospBucket), string(posBucket), string(cpsBucket)} {
		err = qs.env.Update(func(tx *lmdb.Txn) (err error) {
			fmt.Println("Upgrading bucket", bucket)
			dbi, err := tx.OpenDBI(bucket, 0)
			if err != nil {
				return err
			}

			s := lmdbscan.New(tx, dbi)
			defer s.Close()

			for s.Scan() {
				var h proto.HistoryEntry
				err := json.Unmarshal(s.Val(), &h)
				if err != nil {
					return err
				}
				data, err := h.Marshal()
				if err != nil {
					return err
				}
				err = tx.Put(dbi, s.Key(), data, 0)
				if err != nil {
					return err
				}
			}

			return s.Err()
		})
		if err != nil {
			return err
		}

		return nil
	}
	return nil
}

func upgrade1To2(db *bolt.DB) error {
	fmt.Println("Upgrading v1 to v2...")
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Println("Upgrading bucket", string(logBucket))
	lb := tx.Bucket(logBucket)
	c := lb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var delta graph.Delta
		err := json.Unmarshal(v, &delta)
		if err != nil {
			return err
		}
		newd := deltaToProto(delta)
		data, err := newd.Marshal()
		if err != nil {
			return err
		}
		lb.Put(k, data)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Println("Upgrading bucket", string(nodeBucket))
	nb := tx.Bucket(nodeBucket)
	c = nb.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var vd proto.NodeData
		err := json.Unmarshal(v, &vd)
		if err != nil {
			return err
		}
		data, err := vd.Marshal()
		if err != nil {
			return err
		}
		nb.Put(k, data)
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	for _, bucket := range [4][]byte{spoBucket, ospBucket, posBucket, cpsBucket} {
		tx, err = db.Begin(true)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		fmt.Println("Upgrading bucket", string(bucket))
		b := tx.Bucket(bucket)
		cur := b.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			var h proto.HistoryEntry
			err := json.Unmarshal(v, &h)
			if err != nil {
				return err
			}
			data, err := h.Marshal()
			if err != nil {
				return err
			}
			b.Put(k, data)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
