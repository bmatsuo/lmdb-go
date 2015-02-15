package lmdb_test

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

const MB = 1 << 20

func ExampleTxn_OpenDBI() {
	dbpath, err := ioutil.TempDir("", "lmdb-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dbpath)

	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}

	// when using named databases SetMaxDBs is required to be at least the
	// number of named databases needed.
	if err = env.SetMaxDBs(4); err != nil {
		panic(err)
	}

	err = env.Open(dbpath, 0, 0644)
	defer env.Close()
	if err != nil {
		panic(err)
	}

	var db1, db2, db3, db4 lmdb.DBI
	var dbroot lmdb.DBI
	env.Update(func(txn *lmdb.Txn) (err error) {
		_, err = txn.OpenDBI("db0", 0) // ErrNotFound
		if err != nil {
			fmt.Println("db0", err)
		}
		db1, err = txn.OpenDBI("db1", lmdb.Create)
		if err != nil {
			fmt.Println("db1", err)
		}
		db2, err = txn.OpenDBI("db2", lmdb.Create)
		if err != nil {
			fmt.Println("db2", err)
		}
		db3, err = txn.OpenDBI("db3", lmdb.Create)
		if err != nil {
			fmt.Println("db3", err)
		}
		db4, err = txn.OpenDBI("db4", lmdb.Create)
		if err != nil {
			fmt.Println("db4", err)
		}
		_, err = txn.OpenDBI("db5", lmdb.Create) // ErrDBsFull
		if err != nil {
			fmt.Println("db5", err)
		}
		dbroot, err = txn.OpenRoot(0) // does not count against maxdbs
		if err != nil {
			fmt.Println("root", err)
		}
		return nil
	})

	env.View(func(txn *lmdb.Txn) error {
		cursor, err := txn.OpenCursor(dbroot)
		if err != nil {
			return fmt.Errorf("cursor: %v", err)
		}

		fmt.Println("databases:")
		for {
			k, _, err := cursor.Get(nil, nil, lmdb.Next)
			if err == lmdb.ErrNotFound {
				return nil
			}
			if err != nil {
				return fmt.Errorf("root next: %v", err)
			}
			fmt.Printf("  %s\n", k)
		}
	})

	// Output:
	// db0 MDB_NOTFOUND: No matching key/data pair found
	// db5 MDB_DBS_FULL: Environment maxdbs limit reached
	// databases:
	//   db1
	//   db2
	//   db3
	//   db4
}
