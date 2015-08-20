package lmdbscan_test

import (
	"bytes"
	"encoding/binary"
	"log"
	"time"

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
		for scanner.Scan(nil) {
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

// This (mildly contrived) example demonstrates how the features of lmdbscan
// combine to effectively query a database.  In the example time series data is
// being filtered.  The timestamp of each series entry is encoded in the
// database key, prefixed with the bytes "data:".  This information is used
// more efficiently filter keys.
func Example() {
	prefix := []byte("data:")
	cutoff := time.Now().Add(-time.Minute)

	err := env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		scanner.SetNext(prefix, nil, lmdb.SetRange, lmdb.Next)
		hasPrefix := lmdbscan.While(func(k, v []byte) bool { return bytes.HasPrefix(k, prefix) })
		isTimeSeries := lmdbscan.Select(func(k, v []byte) bool { return len(k)-len(prefix) == 8 })
		notCutOff := lmdbscan.Select(func(k, v []byte) bool {
			nsbytes := k[len(prefix):]
			ns := binary.BigEndian.Uint64(nsbytes)
			t := time.Unix(0, int64(ns))
			return t.After(cutoff)
		})
		for scanner.Scan(hasPrefix, isTimeSeries, notCutOff) {
			log.Print(scanner.Val())

			// ... process the series entry
		}

		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}

// This simple example shows how to iterate over a database that indexes json
// document.
func ExampleScanner_Scan() {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		for scanner.Scan(nil) {
			log.Printf("%q=%q", scanner.Key(), scanner.Val())
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}
