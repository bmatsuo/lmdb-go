package lmdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
)

func TestErrno(t *testing.T) {
	zeroerr := errno(0)
	if zeroerr != nil {
		t.Errorf("errno(0) != nil: %#v", zeroerr)
	}
	syserr := _errno(int(syscall.EINVAL))
	if syserr != syscall.EINVAL { // fails if syserr is Errno(syscall.EINVAL)
		t.Errorf("errno(syscall.EINVAL) != syscall.EINVAL: %#v", syserr)
	}
	mdberr := _errno(int(ErrKeyExist))
	if mdberr != ErrKeyExist { // fails if syserr is Errno(syscall.EINVAL)
		t.Errorf("errno(ErrKeyExist) != ErrKeyExist: %#v", syserr)
	}
}

func TestTest1(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("Cannot create environment: %s", err)
	}
	err = env.SetMapSize(10485760)
	if err != nil {
		t.Fatalf("Cannot set mapsize: %s", err)
	}
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0770)
	defer os.RemoveAll(path)
	if err != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	err = env.Open(path, 0, 0664)
	defer env.Close()
	if err != nil {
		t.Fatalf("Cannot open environment: %s", err)
	}

	var db DBI
	numEntries := 10
	var data = map[string]string{}
	var key string
	var val string
	for i := 0; i < numEntries; i++ {
		key = fmt.Sprintf("Key-%d", i)
		val = fmt.Sprintf("Val-%d", i)
		data[key] = val
	}
	err = env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}

		for k, v := range data {
			err = txn.Put(db, []byte(k), []byte(v), NoOverwrite)
			if err != nil {
				return fmt.Errorf("put: %v", err)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	stat, err := env.Stat()
	if err != nil {
		t.Fatalf("Cannot get stat %s", err)
	} else if stat.Entries != uint64(numEntries) {
		t.Errorf("Less entry in the database than expected: %d <> %d", stat.Entries, numEntries)
	}
	t.Logf("%#v", stat)

	err = env.View(func(txn *Txn) error {
		cursor, err := txn.OpenCursor(db)
		if err != nil {
			cursor.Close()
			return fmt.Errorf("cursor: %v", err)
		}
		var bkey, bval []byte
		var bNumVal int
		for {
			bkey, bval, err = cursor.Get(nil, nil, Next)
			if err == ErrNotFound {
				break
			}
			if err != nil {
				return fmt.Errorf("cursor get: %v", err)
			}
			bNumVal++
			skey := string(bkey)
			sval := string(bval)
			t.Logf("Val: %s", sval)
			t.Logf("Key: %s", skey)
			var d string
			var ok bool
			if d, ok = data[skey]; !ok {
				return fmt.Errorf("cursor get: key does not exist %q", skey)
			}
			if d != sval {
				return fmt.Errorf("cursor get: value %q does not match %q", sval, d)
			}
		}
		if bNumVal != numEntries {
			t.Errorf("cursor iterated over %d entries when %d expected", bNumVal, numEntries)
		}
		cursor.Close()
		bval, err = txn.Get(db, []byte("Key-0"))
		if err != nil {
			return fmt.Errorf("get: %v", err)
		}
		if string(bval) != "Val-0" {
			return fmt.Errorf("get: value %q does not match %q", bval, "Val-0")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
