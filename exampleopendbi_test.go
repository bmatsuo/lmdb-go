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

	err = env.Update(func(txn *lmdb.Txn) error {
		for i := 0; i < numdb; i++ {
			name := fmt.Sprintf("database%d", i)
			db, err := txn.OpenDBI(name, lmdb.Create)
			if err != nil {
				return fmt.Errorf("%s open: %v", name, err)
			}
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

	err = env.View(func(txn *lmdb.Txn) error {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return fmt.Errorf("root open: %v", err)
		}
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
	// database0
	// database1
	// database2
	// database3
}
