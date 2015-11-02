package lmdbscan

import (
	"reflect"
	"syscall"
	"testing"

	"github.com/bmatsuo/lmdb-go/internal/lmdbtest"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

type errcheck func(err error) (ok bool)

var pIsNil = func(err error) bool { return err == nil }

func TestScanner_err(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	err = env.View(func(txn *lmdb.Txn) (err error) {
		scanner := New(txn, 123)
		defer scanner.Close()
		for scanner.Scan() {
			t.Error("loop should not execute")
		}
		if scanner.Set(nil, nil, lmdb.First) {
			t.Error("Set returned true")
		}
		if scanner.SetNext(nil, nil, lmdb.NextNoDup, lmdb.NextDup) {
			t.Error("SetNext returned true")
		}
		return scanner.Err()
	})
	if !lmdb.IsErrnoSys(err, syscall.EINVAL) {
		t.Errorf("unexpected error: %q (!= %q)", err, syscall.EINVAL)
	}
}

func TestScanner_closed(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	err = env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		scanner := New(txn, dbi)

		err = scanner.Err()
		if err != nil {
			return err
		}

		scanner.Close()

		for scanner.Scan() {
			t.Error("loop should not execute")
		}
		return scanner.Err()
	})
	if err != errClosed {
		t.Errorf("unexpected error: %q (!= %q)", err, errClosed)
	}
}

func TestScanner_Scan(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	dbi, err := lmdbtest.OpenRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	items := lmdbtest.SimpleItemList{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	err = lmdbtest.Put(env, dbi, items)
	if err != nil {
		t.Error(err)
	}
	scanned, err := simplescan(env, dbi)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !reflect.DeepEqual(scanned, items) {
		t.Errorf("unexpected items %q (!= %q)", scanned, items)
	}
}

func TestScanner_Set(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	dbi, err := lmdbtest.OpenRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	items := lmdbtest.SimpleItemList{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	err = lmdbtest.Put(env, dbi, items)
	if err != nil {
		t.Error(err)
	}

	var tail lmdbtest.SimpleItemList
	err = env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		s := New(txn, dbi)
		defer s.Close()

		s.Set([]byte("k34"), nil, lmdb.SetRange)
		tail, err = remaining(s)
		return err
	})
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(tail, items[4:]) {
		t.Errorf("items: %q (!= %q)", tail, items)
	}
}

func TestScanner_SetNext(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	dbi, err := lmdbtest.OpenRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	items := lmdbtest.SimpleItemList{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	err = lmdbtest.Put(env, dbi, items)
	if err != nil {
		t.Error(err)
	}

	var head lmdbtest.SimpleItemList
	err = env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		s := New(txn, dbi)
		defer s.Close()

		s.SetNext([]byte("k34"), nil, lmdb.SetRange, lmdb.Prev)
		head, err = remaining(s)
		return err
	})
	if err != nil {
		t.Error(err)
	}

	// reverse head before testing its value
	n := len(head)
	for i := 0; i < n/2; i++ {
		head[i], head[n-1-i] = head[n-1-i], head[i]
	}

	if !reflect.DeepEqual(head, items[:5]) {
		t.Errorf("items: %q (!= %q)", head, items)
	}
}

func TestScanner_Del(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	dbi, err := lmdbtest.OpenRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	items := lmdbtest.SimpleItemList{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	err = lmdbtest.Put(env, dbi, items)
	if err != nil {
		t.Error(err)
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		s := New(txn, dbi)
		defer s.Close()
		for s.Scan() {
			err = s.Del(0)
			if err != nil {
				return err
			}
		}
		return s.Err()
	})
	if err != nil {
		t.Error(err)
	}

	var rem lmdbtest.SimpleItemList
	err = env.View(func(txn *lmdb.Txn) (err error) {
		s := New(txn, dbi)
		defer s.Close()
		rem, err = remaining(s)
		return err
	})
	if err != nil {
		t.Error(err)
	}

	if len(rem) != 0 {
		t.Errorf("items: %q (!= %q)", rem, []string{})
	}
}

func TestScanner_Del_closed(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	dbi, err := lmdbtest.OpenRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	items := lmdbtest.SimpleItemList{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	err = lmdbtest.Put(env, dbi, items)
	if err != nil {
		t.Error(err)
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		s := New(txn, dbi)
		s.Close()
		return s.Del(0)
	})
	if err != errClosed {
		t.Errorf("unexpected error: %q (!= %q)", err, errClosed)
	}
}

func TestScanner_Cursor_Del(t *testing.T) {
	env, err := lmdbtest.NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env)

	dbi, err := lmdbtest.OpenRoot(env, 0)
	if err != nil {
		t.Error(err)
		return
	}

	items := lmdbtest.SimpleItemList{
		{"k0", "v0"},
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
		{"k5", "v5"},
	}
	err = lmdbtest.Put(env, dbi, items)
	if err != nil {
		t.Error(err)
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		s := New(txn, dbi)
		defer s.Close()
		cur := s.Cursor()
		for s.Scan() {
			err = cur.Del(0)
			if err != nil {
				return err
			}
		}
		return s.Err()
	})
	if err != nil {
		t.Error(err)
	}

	var rem lmdbtest.SimpleItemList
	err = env.View(func(txn *lmdb.Txn) (err error) {
		s := New(txn, dbi)
		defer s.Close()
		rem, err = remaining(s)
		return err
	})
	if err != nil {
		t.Error(err)
	}

	if len(rem) != 0 {
		t.Errorf("items: %q (!= %q)", rem, []string{})
	}
}

func simplescan(env *lmdb.Env, dbi lmdb.DBI) (items lmdbtest.SimpleItemList, err error) {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		s := New(txn, dbi)
		defer s.Close()

		items, err = remaining(s)
		return err
	})
	return items, err
}

func remaining(s *Scanner) (items lmdbtest.SimpleItemList, err error) {
	for s.Scan() {
		item := &lmdbtest.SimpleItem{
			K: string(s.Key()),
			V: string(s.Val()),
		}
		items = append(items, item)
	}
	err = s.Err()
	if err != nil {
		return nil, err
	}
	return items, nil
}
