package lmdb

import (
	"fmt"
	"testing"
)

func TestTxnUpdate(t *testing.T) {
	env := setup(t)
	defer env.Close()

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

func TestTxnViewSub(t *testing.T) {
	env := setup(t)
	defer env.Close()

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

func TestTxnUpdateSub(t *testing.T) {
	env := setup(t)
	defer env.Close()

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
