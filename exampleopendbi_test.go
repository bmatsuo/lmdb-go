package lmdb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/bmatsuo/lmdb.exp"
)

const MB = 1 << 20

func ExampleTxn_openDBI() {
	// the number of named databases to create
	const numdb = 4

	dbpath, err := ioutil.TempDir("", "lmdb-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dbpath)

	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	if err = env.SetMapSize(10 * MB); err != nil {
		panic(err)
	}

	// when using named databases SetMaxDBs is required to be at least the
	// number of databases needed.
	if err = env.SetMaxDBs(numdb); err != nil {
		panic(err)
	}

	err = env.Open(dbpath, 0, 0644)
	defer env.Close()
	if err != nil {
		panic(err)
	}

	// if you attempt to open a non-existent named database and you don't pass
	// the lmdb.Create flag lmdb.ErrNotFound will be returned.
	err = env.Update(func(txn *lmdb.Txn) error {
		dbname := "a non-existent database"
		fmt.Printf("opening %s\n", dbname)
		_, err := txn.OpenDBI(dbname, 0) // lmdb.Create is not passed
		if err != nil {
			fmt.Println(err)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// create the maximum number of named databases allowed and store a value
	// for fun.
	err = env.Update(func(txn *lmdb.Txn) error {
		for i := 0; i < numdb; i++ {
			name := fmt.Sprintf("database%d", i)
			db, err := txn.CreateDBI(name)
			if err != nil {
				return fmt.Errorf("%s open: %v", name, err)
			}
			fmt.Printf("create %s: %x\n", name, db)
			err = txn.Put(db, []byte("hello"), []byte(strings.Repeat("database", i)), 0)
			if err != nil {
				return fmt.Errorf("%s put: %v", name, err)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Open the root dbi and iterate. Notice that this database is not created
	// with lmdb.Create (because it was created behind the scenes when creating
	// the named database).
	err = env.View(func(txn *lmdb.Txn) error {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return fmt.Errorf("root open: %v", err)
		}
		fmt.Printf("open root: %x\n", db)
		cursor, err := txn.OpenCursor(db)
		if err != nil {
			return fmt.Errorf("root cursor: %v", err)
		}
		for {
			k, _, err := cursor.Get(nil, nil, lmdb.Next)
			if err == lmdb.ErrNotFound {
				return nil
			}
			if err != nil {
				return fmt.Errorf("root next: %v", err)
			}
			fmt.Printf("%s\n", k)
		}
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// opening a non-existent database
	// MDB_NOTFOUND: No matching key/data pair found
	// create database0: 2
	// create database1: 3
	// create database2: 4
	// create database3: 5
	// open root: 1
	// database0
	// database1
	// database2
	// database3
}
