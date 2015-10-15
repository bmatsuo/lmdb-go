package lmdbscan

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type errcheck func(err error) (ok bool)

var pIsNil = func(err error) bool { return err == nil }

func TestScanner_Scan(t *testing.T) {
	env := testEnv(t)
	items := []simpleitem{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	loadTestData(t, env, items)
	for i, test := range []struct {
		filtered []simpleitem
		errcheck
	}{
		{
			items,
			nil,
		},
	} {
		filtered, err := simplescan(env)
		if err != nil {
			t.Errorf("test %d: %v", i, err)
		}
		if !reflect.DeepEqual(filtered, test.filtered) {
			t.Errorf("test %d: unexpected items %q (!= %q)", i, filtered, test.filtered)
		}
	}
}

type simpleitem [2]string

func loadTestData(t *testing.T, env *lmdb.Env, items []simpleitem) {
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		for _, item := range items {
			err := txn.Put(db, []byte(item[0]), []byte(item[1]), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func simplescan(env *lmdb.Env) (items []simpleitem, err error) {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		s := New(txn, db)
		defer s.Close()

		for s.Scan() {
			items = append(items, simpleitem{string(s.Key()), string(s.Val())})
		}
		return s.Err()
	})
	return items, err
}

func testEnv(t *testing.T) *lmdb.Env {
	dir, err := ioutil.TempDir("", "test-lmdb-env-")
	if err != nil {
		t.Fatal(err)
	}
	cleanAndDie := func() {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		cleanAndDie()
	}
	err = env.Open(dir, 0, 0644)
	if err != nil {
		cleanAndDie()
	}

	return env
}
