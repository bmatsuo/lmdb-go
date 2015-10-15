package lmdbscan_test

import (
	"bytes"
	"log"

	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

var env *lmdb.Env

// This example demonstrates basic usage of a Scanner to scan the root
// database.  It is important to always call scanner.Err() which will returned
// any unexpected error which interrupted scanner.Scan().
func ExampleScanner() {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		for scanner.Scan() {
			log.Printf("k=%q v=%q", scanner.Key(), scanner.Val())
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}

// This example demonstrates scanning a key range in the root database.  Set is
// used to move the cursor's starting position to the desired prefix.
func ExampleScanner_Set() {
	keyprefix := []byte("users:")
	err := env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		scanner.Set(keyprefix, nil, lmdb.SetRange)
		for scanner.Scan() {
			if !bytes.HasPrefix(scanner.Key(), keyprefix) {
				break
			}
			log.Printf("k=%q v=%q", scanner.Key(), scanner.Val())
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}

// This example demonstrates scanning all values for a key in a root database
// with the lmdb.DupSort flag set.  SetNext is used instead of Set to configure
// Cursor the to return ErrNotFound (EOF) after all duplicate keys have been
// iterated.
func ExampleScanner_SetNext() {
	key := []byte("userphone:123")
	err := env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		scanner.SetNext(key, nil, lmdb.GetBothRange, lmdb.NextDup)
		for scanner.Scan() {
			log.Printf("k=%q v=%q", scanner.Key(), scanner.Val())
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}
