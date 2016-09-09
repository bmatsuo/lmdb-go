package lmdbscan_test

import (
	"bytes"
	"log"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
)

var env *lmdb.Env
var dbi lmdb.DBI

// This example demonstrates basic usage of a Scanner to scan a database.  It
// is important to always call scanner.Err() which will returned any unexpected
// error which interrupted scanner.Scan().
func ExampleScanner() {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		scanner := lmdbscan.New(txn, dbi)
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

// This example demonstrates scanning a key range in a database.  Set is used
// to move the cursor's starting position to the desired prefix.
func ExampleScanner_Set() {
	keyprefix := []byte("users:")
	err := env.View(func(txn *lmdb.Txn) (err error) {
		scanner := lmdbscan.New(txn, dbi)
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
		scanner := lmdbscan.New(txn, dbi)
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

// This example demonstrates scanning all values for duplicate keys in a
// database with the lmdb.DupSort flag set.  Two loops are used to iterate over
// unique keys and their values respectively.  The example exploits the return
// value from SetNext as the termination condition for the first loop.
func ExampleScanner_SetNext_nextNoDup() {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		scanner := lmdbscan.New(txn, dbi)
		defer scanner.Close()

		for scanner.SetNext(nil, nil, lmdb.NextNoDup, lmdb.NextDup) {
			key := scanner.Key()
			var vals [][]byte
			for scanner.Scan() {
				vals = append(vals, scanner.Val())
			}
			log.Printf("k=%q v=%q", key, vals)
			if scanner.Err() != nil {
				break
			}
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}

// This advanced example demonstrates batch scanning of values for duplicate
// keys in a database with the lmdb.DupFixed and lmdb.DupSort flags set.  The
// outer loop scans unique keys and the inner loop scans duplicate values.  The
// GetMultiple op requires an additional check following its use to determine
// if no duplicates exist in the database.
func ExampleScanner_SetNext_getMultiple() {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		scanner := lmdbscan.New(txn, dbi)
		defer scanner.Close()

		for scanner.Set(nil, nil, lmdb.NextNoDup) {
			key := scanner.Key()
			valFirst := scanner.Val()
			var vals [][]byte
			if !scanner.SetNext(nil, nil, lmdb.GetMultiple, lmdb.NextMultiple) {
				// only one value exists for the key, and it has been scanned.
				vals = append(vals, valFirst)
			}
			for scanner.Scan() {
				// this loop is only entered if multiple values exist for key.
				multi := lmdb.WrapMulti(scanner.Val(), len(valFirst))
				vals = append(vals, multi.Vals()...)
			}
			log.Printf("k=%q v=%q", key, vals)
			if scanner.Err() != nil {
				break
			}
		}
		return scanner.Err()
	})
	if err != nil {
		panic(err)
	}
}
