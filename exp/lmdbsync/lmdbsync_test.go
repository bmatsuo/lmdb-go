package lmdbsync

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

func newEnv(t *testing.T, flags uint) *Env {
	dir, err := ioutil.TempDir("", "lmdbsync-test-")
	if err != nil {
		t.Fatal(err)
	}

	env, err := NewEnv(nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	err = env.Open(dir, flags, 0644)
	if err != nil {
		os.RemoveAll(dir)
		env.Close()
		t.Fatal(err)
	}

	return env
}

func cleanEnv(t *testing.T, env *Env) {
	path, err := env.Path()
	if err != nil {
		t.Error(err)
	}
	err = env.Close()
	if err != nil {
		t.Error(err)
	}
	err = os.RemoveAll(path)
	if err != nil {
		t.Error(err)
	}
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
	env := newEnv(t, 0)
	defer cleanEnv(t, env)

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
	err := env.SetMapSize(10 << 20)
	if err != nil {
		t.Error(err)
	}

	// finally check for any error in the transaction.
	err = <-errc
	if err != nil {
		t.Error(err)
	}

}
