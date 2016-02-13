package lmdbsync

import (
	"io/ioutil"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/bmatsuo/lmdb-go/internal/lmdbtest"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

var optNoLock = &lmdbtest.EnvOptions{Flags: lmdb.NoLock}

func newEnv(opt *lmdbtest.EnvOptions) (*Env, error) {
	env, err := lmdbtest.NewEnv(opt)
	if err != nil {
		return nil, err
	}
	return NewEnv(env)
}

func TestNewEnv(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmdbsync-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := NewEnv(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()
	err = env.Open(dir, 0, 0644)
	if err != nil {
		t.Error(err)
		return
	}

	if env.noLock {
		t.Errorf("flag lmdb.NoLock detected incorrectly")
	}

	info, err := env.Info()
	if err != nil {
		t.Error(err)
	}
	if info.MapSize <= 0 {
		t.Errorf("bad mapsize: %v", info.MapSize)
	}
}

func TestEnv_Open(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmdbsync-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := NewEnv(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()
	err = env.Open(dir, 0, 0644)
	if err != nil {
		t.Error(err)
		return
	}

	if env.noLock {
		t.Error("flag lmdb.NoLock detected incorrectly")
	}

	// calling Open on an open environment will fail.  env.noLock should not be
	// set on a failing call to Open.
	err = env.Open(dir, lmdb.NoLock, 0644)
	if !lmdb.IsErrnoSys(err, syscall.EINVAL) {
		t.Error(err)
	}

	if env.noLock {
		t.Error("flag lmdb.NoLock detected incorrectly")
	}
}

func TestNewEnv_noLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmdbsync-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	env, err := NewEnv(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()
	err = env.Open(dir, lmdb.NoLock, 0644)
	if err != nil {
		t.Error(err)
		return
	}

	if !env.noLock {
		t.Errorf("flag lmdb.NoLock not detected correctly")
	}

	info, err := env.Info()
	if err != nil {
		t.Error(err)
	}
	if info.MapSize <= 0 {
		t.Errorf("bad mapsize: %v", info.MapSize)
	}
}

func TestNewEnv_noLock2(t *testing.T) {
	env, err := newEnv(optNoLock)
	if err != nil {
		t.Error(err)
		return
	}
	defer lmdbtest.Destroy(env.Env)

	if !env.noLock {
		t.Errorf("flag lmdb.NoLock not detected correctly")
	}

	info, err := env.Info()
	if err != nil {
		t.Error(err)
	}
	if info.MapSize <= 0 {
		t.Errorf("bad mapsize: %v", info.MapSize)
	}
}

func TestNewEnv_arg(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmdbsync-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	_env, err := lmdb.NewEnv()
	if err != nil {
		t.Error(err)
		return
	}
	env, err := NewEnv(_env)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()

	if env.noLock {
		t.Errorf("flag lmdb.NoLock detected incorrectly")
	}

	err = env.Open(dir, lmdb.NoLock, 0644)
	if err != nil {
		t.Error(err)
		return
	}

	if !env.noLock {
		t.Errorf("flag lmdb.NoLock not detected correctly")
	}

	info, err := env.Info()
	if err != nil {
		t.Error(err)
	}
	if info.MapSize <= 0 {
		t.Errorf("bad mapsize: %v", info.MapSize)
	}
}

func TestNewEnv_noLock_arg(t *testing.T) {
	dir, err := ioutil.TempDir("", "lmdbsync-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	_env, err := lmdb.NewEnv()
	if err != nil {
		t.Error(err)
		return
	}
	err = _env.Open(dir, lmdb.NoLock, 0644)
	if err != nil {
		t.Error(err)
		return
	}

	env, err := NewEnv(_env)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()

	if !env.noLock {
		t.Errorf("flag lmdb.NoLock not detected correctly")
	}

	info, err := env.Info()
	if err != nil {
		t.Error(err)
	}
	if info.MapSize <= 0 {
		t.Errorf("bad mapsize: %v", info.MapSize)
	}
}

func TestEnv_SetMapSize(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	txnopen := make(chan struct{})
	errc := make(chan error, 1)
	go func() {
		// open a transaction, signal to the main routine than the transaction
		// is open, and wait for a short period.
		errc <- env.View(func(txn *lmdb.Txn) (err error) {
			txnopen <- struct{}{}
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}()

	// once the transaction has been opened attempt to change the map size.
	// the call to SetMapSize will block until the transaction completes.
	<-txnopen
	err = env.SetMapSize(10 << 20)
	if err != nil {
		t.Error(err)
	}

	// finally check for any error in the transaction.
	err = <-errc
	if err != nil {
		t.Error(err)
	}

}

func TestEnv_BeginTxn(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	txn, err := env.BeginTxn(nil, 0)
	if err == nil {
		t.Error("transaction was created")
		txn.Abort()
	}
}

func testView(t *testing.T, env TxnRunner) {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("k"), []byte("v"), 0)
		if err == nil {
			t.Error("put allowed inside view transaction")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func testUpdate(t *testing.T, env TxnRunner) {
	var dbi lmdb.DBI
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.Put(dbi, []byte("k"), []byte("v"), 0)
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Errorf("unexpected value: %q (!= %q)", v, "v")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func testUpdateLocked(t *testing.T, env TxnRunner) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var dbi lmdb.DBI
	err := env.UpdateLocked(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.Put(dbi, []byte("k"), []byte("v"), 0)
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Errorf("unexpected value: %q (!= %q)", v, "v")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func testRunTxn(t *testing.T, env TxnRunner) {
	var dbi lmdb.DBI
	err := env.RunTxn(0, func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.Put(dbi, []byte("k"), []byte("v"), 0)
	})
	if err != nil {
		t.Error(err)
	}

	err = env.RunTxn(lmdb.Readonly, func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("k"), []byte("V"), 0)
		if err == nil {
			t.Error("put allowed inside view transaction")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	err = env.View(func(txn *lmdb.Txn) (err error) {
		v, err := txn.Get(dbi, []byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Errorf("unexpected value: %q (!= %q)", v, "v")
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestEnv_View(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testView(t, env)
}

func TestEnv_Update(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testUpdate(t, env)
}

func TestEnv_UpdateLocked(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testUpdateLocked(t, env)
}

func TestEnv_RunTxn(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testRunTxn(t, env)
}

func TestEnv_View_NoLock(t *testing.T) {
	env, err := newEnv(optNoLock)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testView(t, env)
}

func TestEnv_Update_NoLock(t *testing.T) {
	env, err := newEnv(optNoLock)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testUpdate(t, env)
}

func TestEnv_UpdateLocked_NoLock(t *testing.T) {
	env, err := newEnv(optNoLock)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testUpdateLocked(t, env)
}

func TestEnv_RunTxn_NoLock(t *testing.T) {
	env, err := newEnv(optNoLock)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	testRunTxn(t, env)
}

func TestEnv_WithHandler_View(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	handler := &testHandler{}
	runner := env.WithHandler(handler)

	testView(t, runner)

	if handler.env != env {
		t.Errorf("handler does not include original env")
	}
	if !handler.called {
		t.Errorf("handler was not called")
	}
}

func TestEnv_WithHandler_Update(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	handler := &testHandler{}
	runner := env.WithHandler(handler)

	testUpdate(t, runner)

	if handler.env != env {
		t.Errorf("handler does not include original env")
	}
	if !handler.called {
		t.Errorf("handler was not called")
	}
}

func TestEnv_WithHandler_UpdateLocked(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	handler := &testHandler{}
	runner := env.WithHandler(handler)

	testUpdateLocked(t, runner)

	if handler.env != env {
		t.Errorf("handler does not include original env")
	}
	if !handler.called {
		t.Errorf("handler was not called")
	}
}

func TestEnv_WithHandler_RunTxn(t *testing.T) {
	env, err := newEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lmdbtest.Destroy(env.Env)

	handler := &testHandler{}
	runner := env.WithHandler(handler)

	testRunTxn(t, runner)

	if handler.env != env {
		t.Errorf("handler does not include original env")
	}
	if !handler.called {
		t.Errorf("handler was not called")
	}
}
