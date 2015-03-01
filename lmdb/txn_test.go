package lmdb

import (
	"fmt"
	"os"
	"testing"
)

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
