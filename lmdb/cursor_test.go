package lmdb

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
)

func TestCursor_Txn(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}

		_txn := cur.Txn()
		if _txn == nil {
			t.Errorf("nil cursor txn")
		}

		cur.Close()

		_txn = cur.Txn()
		if _txn != nil {
			t.Errorf("non-nil cursor txn")
		}

		return err
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestCursor_DBI(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	err := env.Update(func(txn *Txn) (err error) {
		db, err := txn.OpenDBI("db", Create)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		dbcur := cur.DBI()
		if dbcur != db {
			cur.Close()
			return fmt.Errorf("unequal db: %v != %v", dbcur, db)
		}
		cur.Close()
		dbcur = cur.DBI()
		if dbcur == db {
			return fmt.Errorf("db: %v", dbcur)
		}
		if dbcur != 0 {
			return fmt.Errorf("db: %v", dbcur)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestCursor_Close(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Abort()

	db, err := txn.OpenDBI("testing", Create)
	if err != nil {
		t.Fatal(err)
	}

	cur, err := txn.OpenCursor(db)
	if err != nil {
		t.Fatal(err)
	}
	cur.Close()
	cur.Close()
	err = cur.Put([]byte("closedput"), []byte("shouldfail"), 0)
	if err == nil {
		t.Fatalf("expected error: put on closed cursor")
	}
}

func TestCursor_PutReserve(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	key := "reservekey"
	val := "reserveval"
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.CreateDBI("testing")
		if err != nil {
			return err
		}

		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()

		p, err := cur.PutReserve([]byte(key), len(val), 0)
		if err != nil {
			return err
		}
		copy(p, val)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		dbval, err := txn.Get(db, []byte(key))
		if err != nil {
			return err
		}
		if !bytes.Equal(dbval, []byte(val)) {
			return fmt.Errorf("unexpected val %q != %q", dbval, val)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// This test verifies the behavior of Cursor.Count when DupSort is provided.
func TestCursor_Count_DupSort(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenDBI("testingdup", Create|DupSort)
		if err != nil {
			return err
		}

		put := func(k, v string) {
			if err != nil {
				return
			}
			err = txn.Put(db, []byte(k), []byte(v), 0)
		}
		put("k", "v0")
		put("k", "v1")

		return err
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()

		_, _, err = cur.Get(nil, nil, First)
		if err != nil {
			return err
		}
		numdup, err := cur.Count()
		if err != nil {
			return err
		}

		if numdup != 2 {
			t.Errorf("unexpected count: %d != %d", numdup, 2)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

// This test verifies the behavior of Cursor.Count when DupSort is not enabled
// on the database.
func TestCursor_Count_noDupSort(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenDBI("testingnodup", Create)
		if err != nil {
			return err
		}

		return txn.Put(db, []byte("k"), []byte("v1"), 0)
	})
	if err != nil {
		t.Error(err)
	}

	// it is an error to call Count if the underlying database does not allow
	// duplicate keys.
	err = env.View(func(txn *Txn) (err error) {
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()

		_, _, err = cur.Get(nil, nil, First)
		if err != nil {
			return err
		}
		_, err = cur.Count()
		if err == nil {
			t.Error("expected error")
			return nil
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestCursor_Renew(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		put := func(k, v string) {
			if err == nil {
				err = txn.Put(db, []byte(k), []byte(v), 0)
			}
		}
		put("k1", "v1")
		put("k2", "v2")
		put("k3", "v3")
		return err
	})
	if err != nil {
		t.Error("err")
	}

	var cur *Cursor
	err = env.View(func(txn *Txn) (err error) {
		cur, err = txn.OpenCursor(db)
		if err != nil {
			return err
		}

		k, v, err := cur.Get(nil, nil, Next)
		if err != nil {
			return err
		}
		if string(k) != "k1" {
			return fmt.Errorf("key: %q", k)
		}
		if string(v) != "v1" {
			return fmt.Errorf("val: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		err = cur.Renew(txn)
		if err != nil {
			return err
		}

		k, v, err := cur.Get(nil, nil, Next)
		if err != nil {
			return err
		}
		if string(k) != "k1" {
			return fmt.Errorf("key: %q", k)
		}
		if string(v) != "v1" {
			return fmt.Errorf("val: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkOpenCursor(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	var db DBI
	var cur *Cursor
	err := env.View(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err = txn.OpenCursor(db)
		return err
	})
	if err != nil {
		b.Error(err)
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	txn.Reset()
	defer txn.Abort()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}
		b.StartTimer()

		cur, err := txn.OpenCursor(db)
		if err != nil {
			b.Error(err)
			return
		}
		cur.Close()

		b.StopTimer()
		txn.Reset()
	}
}

func BenchmarkCursor_Renew(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	var cur *Cursor
	err := env.View(func(txn *Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err = txn.OpenCursor(db)
		return err
	})
	if err != nil {
		b.Error(err)
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	txn.Reset()
	defer txn.Abort()

	b.StopTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}
		b.StartTimer()

		err = cur.Renew(txn)
		if err != nil {
			b.Error(err)
			return
		}

		b.StopTimer()
		txn.Reset()
	}
}
