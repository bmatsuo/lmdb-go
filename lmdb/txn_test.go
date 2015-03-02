package lmdb

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"testing"
)

func TestTxn_OpenDBI(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	err := env.View(func(txn *Txn) (err error) {
		_, err = txn.OpenDBI("", 0)
		return err
	})
	if !IsErrno(err, BadValSize) {
		t.Errorf("mdb_dbi_open: %v", err)
	}
}

func TestTxn_Commit_managed(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	err := env.View(func(txn *Txn) (err error) {
		defer func() {
			if e := recover(); e == nil {
				t.Errorf("expected panic: %v", err)
			}
		}()
		return txn.Commit()
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		defer func() {
			if e := recover(); e == nil {
				t.Errorf("expected panic: %v", err)
			}
		}()
		txn.Abort()
		return fmt.Errorf("abort")
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		defer func() {
			if e := recover(); e == nil {
				t.Errorf("expected panic: %v", err)
			}
		}()
		return txn.Renew()
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *Txn) (err error) {
		defer func() {
			if e := recover(); e == nil {
				t.Errorf("expected panic: %v", err)
			}
		}()
		txn.Reset()
		return fmt.Errorf("reset")
	})
	if err != nil {
		t.Error(err)
	}
}

func TestTxn_Commit(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Error(err)
		return
	}
	txn.Abort()
	err = txn.Commit()
	if !IsErrnoSys(err, syscall.EINVAL) {
		t.Errorf("mdb_txn_commit: %v", err)
	}
}

func TestTxn_Update(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(Create)
		if err != nil {
			return err
		}
		err = txn.Put(db, []byte("mykey"), []byte("myvalue"), 0)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Errorf("update: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		v, err := txn.Get(db, []byte("mykey"))
		if err != nil {
			return err
		}
		if string(v) != "myvalue" {
			return fmt.Errorf("value: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Errorf("view: %v", err)
		return
	}
}

func TestTxn_View_noSubTxn(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	// view transactions cannot create subtransactions.  were it possible, they
	// would provide no utility.
	var executed bool
	err := env.View(func(txn *Txn) (err error) {
		return txn.Sub(func(txn *Txn) error {
			executed = true
			return nil
		})
	})
	if err == nil {
		t.Errorf("view: %v", err)
	}
	if executed {
		t.Errorf("view executed: %v", err)
	}
}

func TestTxn_Sub(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

	var errSubAbort = fmt.Errorf("aborted subtransaction")
	var db DBI
	err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(Create)
		if err != nil {
			return err
		}

		// set the key in the root transaction
		err = txn.Put(db, []byte("mykey"), []byte("myvalue"), 0)
		if err != nil {
			return err
		}

		// set the key in a sub transaction
		err = txn.Sub(func(txn *Txn) (err error) {
			return txn.Put(db, []byte("mykey"), []byte("yourvalue"), 0)
		})
		if err != nil {
			return err
		}

		// set the key before aborting a subtransaction
		err = txn.Sub(func(txn *Txn) (err error) {
			err = txn.Put(db, []byte("mykey"), []byte("badvalue"), 0)
			if err != nil {
				return err
			}
			return errSubAbort
		})
		if err != errSubAbort {
			return fmt.Errorf("expected abort: %v", err)
		}

		return nil
	})
	if err != nil {
		t.Errorf("update: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		v, err := txn.Get(db, []byte("mykey"))
		if err != nil {
			return err
		}
		if string(v) != "yourvalue" {
			return fmt.Errorf("value: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Errorf("view: %v", err)
		return
	}
}

func TestTxn_Flags(t *testing.T) {
	env := setup(t)
	path, err := env.Path()
	if err != nil {
		env.Close()
		t.Error(err)
		return
	}
	defer os.RemoveAll(path)

	dbflags := uint(ReverseKey | ReverseDup | DupSort | DupFixed)
	err = env.Update(func(txn *Txn) (err error) {
		db, err := txn.OpenDBI("testdb", dbflags|Create)
		if err != nil {
			return err
		}
		err = txn.Put(db, []byte("bcd"), []byte("exval1"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(db, []byte("abcda"), []byte("exval3"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(db, []byte("abcda"), []byte("exval2"), 0)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		defer cur.Close()
		k, v, err := cur.Get(nil, nil, Next)
		if err != nil {
			return err
		}
		if string(k) != "abcda" { // ReverseKey does not do what one might expect
			return fmt.Errorf("unexpected first key: %q", k)
		}
		if string(v) != "exval2" {
			return fmt.Errorf("unexpected first value: %q", v)
		}
		return nil
	})
	env.Close()
	if err != nil {
		t.Error(err)
		return
	}

	// opening the database after it is created inherits the original flags.
	env, err = NewEnv()
	if err != nil {
		t.Error(err)
		return
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()
	err = env.Open(path, 0, 0644)
	if err != nil {
		t.Error(err)
		return
	}
	err = env.View(func(txn *Txn) (err error) {
		db, err := txn.OpenDBI("testdb", 0)
		if err != nil {
			return err
		}
		flags, err := txn.Flags(db)
		if err != nil {
			return err
		}
		if flags != dbflags {
			return fmt.Errorf("unexpected flags")
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		k, v, err := cur.Get(nil, nil, Next)
		if err != nil {
			return err
		}
		if string(k) != "abcda" {
			return fmt.Errorf("unexpected first key: %q", k)
		}
		if string(v) != "exval2" {
			return fmt.Errorf("unexpected first value: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestTxn_Renew(t *testing.T) {
	env := setup(t)
	path, err := env.Path()
	if err != nil {
		env.Close()
		t.Error(err)
		return
	}
	defer os.RemoveAll(path)
	defer env.Close()

	var dbroot DBI
	err = env.Update(func(txn *Txn) (err error) {
		dbroot, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		t.Error(err)
		return
	}

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Error(err)
		return
	}
	defer txn.Abort()
	val, err := txn.Get(dbroot, []byte("k"))
	if !IsNotFound(err) {
		t.Errorf("get: %v", err)
	}

	err = env.Update(func(txn *Txn) (err error) {
		dbroot, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.Put(dbroot, []byte("k"), []byte("v"), 0)
	})
	if err != nil {
		t.Error(err)
	}

	val, err = txn.Get(dbroot, []byte("k"))
	if !IsNotFound(err) {
		t.Errorf("get: %v", err)
	}
	txn.Reset()

	err = txn.Renew()
	if err != nil {
		t.Error(err)
	}
	val, err = txn.Get(dbroot, []byte("k"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "v" {
		t.Errorf("unexpected value: %q", val)
	}
}

func TestTxn_Renew_noReset(t *testing.T) {
	env := setup(t)
	path, err := env.Path()
	if err != nil {
		env.Close()
		t.Error(err)
		return
	}
	defer os.RemoveAll(path)
	defer env.Close()

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Error(err)
		return
	}
	defer txn.Abort()

	err = txn.Renew()
	if !IsErrnoSys(err, syscall.EINVAL) {
		t.Errorf("renew: %v", err)
	}
}

func TestTxn_Reset_doubleReset(t *testing.T) {
	env := setup(t)
	path, err := env.Path()
	if err != nil {
		env.Close()
		t.Error(err)
		return
	}
	defer os.RemoveAll(path)
	defer env.Close()

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		t.Error(err)
		return
	}
	defer txn.Abort()

	txn.Reset()
	txn.Reset()
}

// This test demonstrates that Reset/Renew have no effect on writable
// transactions. The transaction may be commited after Reset/Renew are called.
func TestTxn_Reset_writeTxn(t *testing.T) {
	env := setup(t)
	path, err := env.Path()
	if err != nil {
		env.Close()
		t.Error(err)
		return
	}
	defer os.RemoveAll(path)
	defer env.Close()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer txn.Abort()

	db, err := txn.OpenRoot(0)
	if err != nil {
		t.Error(err)
	}
	err = txn.Put(db, []byte("k"), []byte("v"), 0)
	if err != nil {
		t.Error(err)
	}

	// Reset is a noop and Renew will always error out.
	txn.Reset()
	err = txn.Renew()
	if !IsErrnoSys(err, syscall.EINVAL) {
		t.Errorf("renew: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Errorf("commit: %v", err)
	}

	err = env.View(func(txn *Txn) (err error) {
		val, err := txn.Get(db, []byte("k"))
		if err != nil {
			return err
		}
		if string(val) != "v" {
			return fmt.Errorf("unexpected value: %q", val)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func BenchmarkBeginTxn(b *testing.B) {
	env := setup(b)
	path, err := env.Path()
	if err != nil {
		env.Close()
		b.Error(err)
		return
	}
	defer os.RemoveAll(path)
	defer env.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, err := env.BeginTxn(nil, Readonly)
		if err != nil {
			b.Error(err)
			return
		}
		txn.Abort()
	}
}

func BenchmarkTxn_Renew(b *testing.B) {
	env := setup(b)
	path, err := env.Path()
	if err != nil {
		env.Close()
		b.Error(err)
		return
	}
	defer os.RemoveAll(path)
	defer env.Close()

	txn, err := env.BeginTxn(nil, Readonly)
	if err != nil {
		b.Error(err)
		return
	}
	defer txn.Abort()
	txn.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = txn.Renew()
		if err != nil {
			b.Error(err)
			return
		}
		txn.Reset()
	}
}
