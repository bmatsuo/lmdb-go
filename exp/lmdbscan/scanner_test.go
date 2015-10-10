package lmdbscan

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type errcheck func(err error) (ok bool)

var pIsSkip = func(err error) bool { return err == Skip }
var pIsStop = func(err error) bool { return err == Stop }
var pIsNil = func(err error) bool { return err == nil }

func TestSelect(t *testing.T) {
	var ret bool
	fn := Select(func(k, v []byte) bool { return ret })

	for i, step := range []struct {
		ret bool
		errcheck
	}{
		{true, pIsNil},
		{false, pIsSkip},
		{true, pIsNil},
		{false, pIsSkip},
	} {
		ret = step.ret
		err := fn(nil, nil)
		if !step.errcheck(err) {
			t.Errorf("step %d: unexpected error: %v", i, err)
		}
	}
}

func TestWhile(t *testing.T) {
	var ret bool
	fn := While(func(k, v []byte) bool { return ret })

	for i, step := range []struct {
		ret bool
		errcheck
	}{
		{true, pIsNil},
		{true, pIsNil},
		{false, pIsStop},
	} {
		ret = step.ret
		err := fn(nil, nil)
		if !step.errcheck(err) {
			t.Errorf("step %d: unexpected error: %v", i, err)
		}
	}
}

func TestSkipErr(t *testing.T) {
	var ret error
	fn := SkipErr(func(_, _ []byte) error { return ret })

	testerr := fmt.Errorf("testerror")
	for i, step := range []struct {
		ret error
		errcheck
	}{
		{nil, pIsNil},
		{testerr, pIsSkip},
		{nil, pIsNil},
		{testerr, pIsSkip},
	} {
		ret = step.ret
		err := fn(nil, nil)
		if !step.errcheck(err) {
			t.Errorf("step %d: unexpected error: %v", i, err)
		}
	}
}

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
		filters  Func
		filtered []simpleitem
		errcheck
	}{
		{
			func(k, v []byte) error { return nil },
			items,
			nil,
		},
		{
			func(k, v []byte) error { return Stop },
			nil,
			nil,
		},
		{
			func(k, v []byte) error { return Skip },
			nil,
			nil,
		},
		{
			func() Func {
				var i int
				return func(k, v []byte) error {
					if i > 4 {
						return Stop
					}
					if i++; i%2 == 1 {
						return Skip
					} else {
						return nil
					}
				}
			}(),
			[]simpleitem{
				{"k1", "v1"},
				{"k3", "v3"},
			},
			nil,
		},
	} {
		filtered, err := simplescan(env, test.filters)
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

func simplescan(env *lmdb.Env, fn Func) (items []simpleitem, err error) {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		s := New(txn, db)
		defer s.Close()

		var filter []Func
		if fn != nil {
			filter = []Func{fn}
		}
		for s.Scan(filter...) {
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
