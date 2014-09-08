package lmdb_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bmatsuo/lmdb.exp"
)

// This example shows how to use the Env type and open a database.
func ExampleEnv() {
	// create a directory to hold the database
	path, _ := ioutil.TempDir("", "mdb_test")
	defer os.RemoveAll(path)

	// open the LMDB environment
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	env.SetMaxDBs(1)
	env.Open(path, 0, 0664)
	defer env.Close()

	// open a database.
	txn, err := env.BeginTxn(nil, 0)
	if err != nil {
		panic(err)
	}
	db, err := txn.OpenDBI("exampledb", lmdb.Create)
	if err != nil {
		txn.Abort()
		panic(err)
	}

	// get statistics about the db. print the number of key-value pairs.
	stat, err := txn.Stat(db)
	if err != nil {
		txn.Abort()
		panic(err)
	}
	fmt.Println(stat.Entries)

	err = txn.Commit()
	if err != nil {
		panic(err)
	}

	// .. open more transactions and use the database

	// Output:
	// 0
}

// This example shows how to read and write data with a Txn.  Errors are
// ignored for brevity.  Real code should check and handle are errors which may
// require more modular code.
func ExampleTxn() {
	// create a directory to hold the database
	path, _ := ioutil.TempDir("", "mdb_test")
	defer os.RemoveAll(path)

	// open the LMDB environment
	env, _ := lmdb.NewEnv()
	env.SetMaxDBs(1)
	env.Open(path, 0, 0664)
	defer env.Close()

	// open a database.
	txn, _ := env.BeginTxn(nil, 0)
	db, _ := txn.OpenDBI("exampledb", lmdb.Create)
	txn.Commit()

	// write some data
	txn, _ = env.BeginTxn(nil, 0)
	txn.Put(db, []byte("key0"), []byte("val0"), 0)
	txn.Put(db, []byte("key1"), []byte("val1"), 0)
	txn.Put(db, []byte("key2"), []byte("val2"), 0)

	// inspect the transaction
	stat, _ := txn.Stat(db)
	fmt.Println(stat.Entries)

	// commit the transaction
	_ = txn.Commit()

	// perform random access on db.  Transactions created with the
	// lmdb.Readonly flag can always be aborted.
	txn, _ = env.BeginTxn(nil, lmdb.Readonly)
	defer txn.Abort()
	bval, _ := txn.Get(db, []byte("key1"))
	fmt.Println(string(bval))

	// Output:
	// 3
	// val1
}

// This example shows how to read and write data using a Cursor.  Errors are
// ignored for brevity.  Real code should check and handle are errors which may
// require more modular code.
func ExampleCursor() {
	// create a directory to hold the database
	path, _ := ioutil.TempDir("", "mdb_test")
	defer os.RemoveAll(path)

	// open the LMDB environment
	env, _ := lmdb.NewEnv()
	env.SetMaxDBs(1)
	env.Open(path, 0, 0664)
	defer env.Close()

	// open a database.
	txn, _ := env.BeginTxn(nil, 0)
	db, _ := txn.OpenDBI("exampledb", lmdb.Create)
	txn.Commit()

	// write some data
	txn, _ = env.BeginTxn(nil, 0)
	cursor, _ := txn.OpenCursor(db)
	cursor.Put([]byte("key0"), []byte("val0"), 0)
	cursor.Put([]byte("key1"), []byte("val1"), 0)
	cursor.Put([]byte("key2"), []byte("val2"), 0)
	cursor.Close()

	// inspect the transaction
	stat, _ := txn.Stat(db)
	fmt.Println(stat.Entries)

	// commit the transaction
	_ = txn.Commit()

	// scan the database
	txn, _ = env.BeginTxn(nil, lmdb.Readonly)
	defer txn.Abort()
	cursor, _ = txn.OpenCursor(db)
	defer cursor.Close()

	for {
		bkey, bval, err := cursor.Get(nil, nil, lmdb.Next)
		if err == lmdb.ErrNotFound {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s: %s\n", bkey, bval)
	}

	// Output:
	// 3
	// key0: val0
	// key1: val1
	// key2: val2
}
