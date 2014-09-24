package lmdb

import (
	"syscall"
	"testing"
)

func TestCursorClose(t *testing.T) {
	env := setup(t)
	defer clean(env, t)

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
	if err != syscall.EINVAL {
		t.Fatalf("unexpected: %v", err)
	}
}
