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
	var txn *Txn
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("Cannot begin transaction: %s", err)
	}
	db, err := txn.OpenDBI("", 0)
	defer env.CloseDBI(db)
	if err != nil {
		t.Fatalf("Cannot create DBI %s", err)
	}
	var data = map[string]string{}
	var key string
	var val string
	num_entries := 10
	for i := 0; i < num_entries; i++ {
		key = fmt.Sprintf("Key-%d", i)
		val = fmt.Sprintf("Val-%d", i)
		data[key] = val
		err = txn.Put(db, []byte(key), []byte(val), NoOverwrite)
		if err != nil {
			txn.Abort()
			t.Fatalf("Error during put: %s", err)
		}
	}
	err = txn.Commit()
	if err != nil {
		txn.Abort()
		t.Fatalf("Cannot commit %s", err)
	}
	stat, err := env.Stat()
	if err != nil {
		t.Fatalf("Cannot get stat %s", err)
	}
	t.Logf("%+v", stat)
	if stat.Entries != uint64(num_entries) {
		t.Errorf("Less entry in the database than expected: %d <> %d", stat.Entries, num_entries)
	}
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		t.Fatalf("Cannot begin transaction: %s", err)
	}
	cursor, err := txn.OpenCursor(db)
	if err != nil {
		cursor.Close()
		txn.Abort()
		t.Fatalf("Error during cursor open %s", err)
	}
	var bkey, bval []byte
	var rc error
	for {
		bkey, bval, rc = cursor.Get(nil, nil, Next)
		if rc != nil {
			break
		}
		skey := string(bkey)
		sval := string(bval)
		t.Logf("Val: %s", sval)
		t.Logf("Key: %s", skey)
		var d string
		var ok bool
		if d, ok = data[skey]; !ok {
			t.Errorf("Cannot found: %q", skey)
		}
		if d != sval {
			t.Errorf("Data missmatch: %q <> %q", sval, d)
		}
	}
	cursor.Close()
	bval, err = txn.Get(db, []byte("Key-0"))
	txn.Abort()
	if err != nil {
		t.Fatalf("Error during txn get %s", err)
	}
	if string(bval) != "Val-0" {
		t.Fatalf("Invalid txn get %s", string(bval))
	}
}
