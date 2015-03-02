package lmdbscan_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"time"

	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

var env *lmdb.Env

// This example demonstrates basic usage of a Scanner to scan the root
// database.
func ExampleScanner() {
	env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		for scanner.Scan() {
			log.Printf("k=%q v=%q", scanner.Key(), scanner.Val())
		}

		// if iteration terminated normally scanner.Err() returns nil.
		// otherwise a non-nil value is returned and the transaction is
		// aborted.
		return scanner.Err()
	})
}

// This example demonstrates scanning a key range in the root database.
func ExampleScanner_Set() {
	keyprefix := []byte("users:")
	env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		// Set is used to determine the cursor's starting position to the
		// desired prefix. And lmdb.While is used to stop iteration once the
		// Scanner has passed the prefix.
		scanner.Set(keyprefix, nil, lmdb.SetRange)
		scan := lmdbscan.While(func(k, v []byte) bool { return bytes.HasPrefix(k, keyprefix) })
		for scanner.Scan(scan) {
			log.Printf("k=%q v=%q", scanner.Key(), scanner.Val())
		}

		// when Scan is passed an lmdb.Func any non-nil value other than
		// Skip or Stop are returned by scanner.Err().
		return scanner.Err()
	})
}

// This example demonstrates scanning all values for a key in a root database
// with the lmdb.DupSort flag set.
func ExampleScanner_SetNext() {
	key := []byte("userphone:123")
	env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		// SetNext is used instead of Set to configure Cursor the to return
		// ErrNotFound (EOF) after all duplicate keys have been iterated.
		scanner.SetNext(key, nil, lmdb.GetBothRange, lmdb.NextDup)
		for scanner.Scan() {
			log.Printf("k=%q v=%q", scanner.Key(), scanner.Val())
		}

		return scanner.Err()
	})
}

func ExampleScanner_Scan() {
	cutoff := time.Now().Add(-time.Minute)

	timeVal := func(nsbytes []byte) time.Time {
		ns := binary.BigEndian.Uint64(nsbytes)
		return time.Unix(0, int64(ns))
	}

	env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		// Scan will accept multiple Funcs that can layer behavior.  Here
		// timeSeries skips keys which do not look like timestamps, and
		// beforeCutoff allows iteration to continue until the timestamp is
		// after some the given cutoff.
		timeSeries := lmdbscan.Select(func(k, v []byte) bool { return len(k) == 8 })
		beforeCutoff := lmdbscan.While(func(k, v []byte) bool {
			return !timeVal(v).After(cutoff)
		})
		for scanner.Scan(timeSeries, beforeCutoff) {
			log.Printf("%s v=%q", timeVal(scanner.Key()), scanner.Val())
		}

		// when Scan is passed an lmdb.Func non-nil error values other than
		// Skip or Stop are returned by scanner.Err().
		return scanner.Err()
	})
}

func Example() {
	env.View(func(txn *lmdb.Txn) (err error) {
		dbroot, _ := txn.OpenRoot(0)

		scanner := lmdbscan.New(txn, dbroot)
		defer scanner.Close()

		var m map[string]interface{}
		unmarshal := func(k, v []byte) error {
			m = nil
			return json.Unmarshal(v, &m)
		}
		for scanner.Scan(lmdbscan.Ignore(unmarshal)) {
			log.Printf("id: %v", m["id"])
		}

		// when Scan is passed an lmdb.Func non-nil error values other than
		// Skip or Stop are returned by scanner.Err().
		return scanner.Err()
	})
}
