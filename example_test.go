package lmdb_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"

	"github.com/bmatsuo/lmdb.exp"
)

// This complete example demonstrates populating and iterating a database with
// the DupFixed|DupSort DBI flags.  The use case is probably too trivial to
// warrant such optimization but it demonstrates the key points.
//
// Note the importance of supplying both DupFixed and DupSort flags on database
// creation.
func Example_dupFixed() {
	// Open an environment as normal. DupSort is applied at the database level.
	env, err := lmdb.NewEnv()
	if err != nil {
		log.Panic(err)
	}
	path, err := ioutil.TempDir("", "mdb_test")
	if err != nil {
		log.Panic(err)
	}
	defer os.RemoveAll(path)
	err = env.SetMaxDBs(1)
	if err != nil {
		log.Panic(err)
	}
	err = env.Open(path, 0, 0644)
	defer env.Close()
	if err != nil {
		log.Panic(err)
	}

	// open the database of friends' phone numbers.  in this limited world
	// phone nubers are all the same length.
	var phonedbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		phonedbi, err = txn.OpenDBI("phone-numbers", lmdb.Create|lmdb.DupSort|lmdb.DupFixed)
		return
	})
	if err != nil {
		panic(err)
	}

	// load entries into the database using PutMulti.  the numbers must be
	// sorted so they may be contiguous in memory.
	err = env.Update(func(txn *lmdb.Txn) error {
		entries := []struct {
			name    string
			numbers []string
		}{
			{"alice", []string{"234-1234"}},
			{"bob", []string{"825-1234"}},
			{"carol", []string{"502-1234", "824-1234", "828-1234"}},
			{"bob", []string{"433-1234", "957-1234"}}, // sorted dup values may be interleaved with existing dups
			{"jenny", []string{"867-5309"}},
		}
		cur, err := txn.OpenCursor(phonedbi)
		if err != nil {
			return fmt.Errorf("cursor: %v", err)
		}
		defer cur.Close()

		for _, e := range entries {
			sort.Strings(e.numbers)
			pnums := make([][]byte, len(e.numbers))
			for i := range e.numbers {
				pnums[i] = []byte(e.numbers[i])
			}
			page := bytes.Join(pnums, nil)
			err = cur.PutMulti([]byte(e.name), page, len(e.numbers[0]), 0)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// grabs the first (partial) page of phone numbers for each name and print
	// them.
	err = env.View(func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(phonedbi)
		if err != nil {
			return fmt.Errorf("cursor: %v", err)
		}
		defer cur.Close()
		for {
			// move to the next key
			k, vfirst, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if err == lmdb.ErrNotFound {
				break
			}
			if err != nil {
				return fmt.Errorf("get: nextnodup: %v", err)
			}

			// determine if multiple keys should be printed and short circuit if
			// so.
			ndup, err := cur.Count()
			if err != nil {
				return fmt.Errorf("count: %v", err)
			}
			if ndup < 2 {
				fmt.Printf("%s %s\n", k, vfirst)
				continue
			}

			// get a page of records and split it into discrete values.  the length
			// of the first dup is used to split the page of contiguous values.
			_, page, err := cur.Get(nil, nil, lmdb.GetMultiple)
			if err != nil {
				return fmt.Errorf("get: multiple: %v", err)
			}
			m, err := lmdb.WrapMulti(page, len(vfirst))
			if err != nil {
				return fmt.Errorf("wrapmulti: %v", err)
			}
			numbers := m.Bytes()

			// print the phone numbers for the person
			prim, others := numbers[0], numbers[1:]
			fmt.Printf("%s %s\n", k, prim)
			ph := bytes.Repeat([]byte{' '}, len(k))
			for i := range others {
				fmt.Printf("%s %s\n", ph, others[i])
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// alice 234-1234
	// bob 433-1234
	//     825-1234
	//     957-1234
	// carol 502-1234
	//       824-1234
	//       828-1234
	// jenny 867-5309
}

// This complete example demonstrates populating and iterating a database with
// the DupSort DBI flags.
func Example_dupSort() {
	// Open an environment as normal. DupSort is applied at the database level.
	env, err := lmdb.NewEnv()
	if err != nil {
		log.Panic(err)
	}
	path, err := ioutil.TempDir("", "mdb_test")
	if err != nil {
		log.Panic(err)
	}
	defer os.RemoveAll(path)
	err = env.SetMaxDBs(1)
	if err != nil {
		log.Panic(err)
	}
	err = env.Open(path, 0, 0644)
	defer env.Close()
	if err != nil {
		log.Panic(err)
	}

	var phonedbi lmdb.DBI

	// open the database of friends' phone numbers.  a single person can have
	// multiple phone numbers.
	err = env.Update(func(txn *lmdb.Txn) error {
		dbi, err := txn.OpenDBI("phone-numbers", lmdb.Create|lmdb.DupSort)
		if err != nil {
			return err
		}
		phonedbi = dbi
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		entries := []struct{ name, number string }{
			{"alice", "234-1234"},
			{"bob", "825-1234"},
			{"carol", "824-1234"},
			{"carol", "828-1234"}, // DupSort stores multiple values for a key.
			{"carol", "502-1234"}, // DupSort values are stored in sorted order.
			{"jenny", "867-5309"},
		}
		for _, e := range entries {
			err = cur.Put([]byte(e.name), []byte(e.number), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// iterate the database and print the first two phone numbers for each
	// person.  this is similar to iterating a database normally but the
	// NextNoDup flag may be used to skip ahead.
	env.View(func(txn *lmdb.Txn) error {
		var lastk []byte
		var isdup bool
		cur, err := txn.OpenCursor(phonedbi)
		if err != nil {
			return err
		}

		var next uint // zero is lmdb.First
		for {
			k, v, err := cur.Get(nil, nil, next)
			if err == lmdb.ErrNotFound {
				break
			}
			if err != nil {
				return err
			}
			next = lmdb.Next
			isdup, lastk = bytes.Equal(lastk, k), k

			// jump to the next key and omit the name
			if isdup {
				next = lmdb.NextNoDup
				k = bytes.Repeat([]byte{' '}, len(k))
			}

			fmt.Printf("%s %s\n", k, v)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// alice 234-1234
	// bob 825-1234
	// carol 502-1234
	//       824-1234
	// jenny 867-5309
}

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

	err = env.Update(func(txn *lmdb.Txn) error {
		// open a database, creating it if necessary.
		db, err := txn.OpenDBI("exampledb", lmdb.Create)
		if err != nil {
			return err
		}

		// get statistics about the db. print the number of key-value pairs (it
		// should be empty).
		stat, err := txn.Stat(db)
		if err != nil {
			return err
		}
		fmt.Println(stat.Entries)

		// commit the transaction, writing an entry for the newly created
		// database.
		return nil
	})
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
	var dbi lmdb.DBI
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("exampledb", lmdb.Create)
		// the transaction will be commited if the database was successfully
		// opened/created.
		return
	})
	if err != nil {
		panic(err)
	}

	// write some data
	err = env.Update(func(txn *lmdb.Txn) error {
		err := txn.Put(dbi, []byte("key0"), []byte("val0"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("key1"), []byte("val1"), 0)
		if err != nil {
			return err
		}
		err = txn.Put(dbi, []byte("key2"), []byte("val2"), 0)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// inspect the database
	err = env.View(func(txn *lmdb.Txn) error {
		stat, err := txn.Stat(dbi)
		if err != nil {
			return err
		}
		fmt.Println(stat.Entries)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// perform random access on the database
	err = env.Update(func(txn *lmdb.Txn) error {
		bval, err := txn.Get(dbi, []byte("key1"))
		if err != nil {
			return err
		}
		fmt.Println(string(bval))
		return nil
	})
	if err != nil {
		panic(err)
	}

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
	var dbi lmdb.DBI
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("exampledb", lmdb.Create)
		return
	})
	if err != nil {
		panic(err)
	}

	// write some data and print the number of items written
	err = env.Update(func(txn *lmdb.Txn) error {
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cursor.Close()

		err = cursor.Put([]byte("key0"), []byte("val0"), 0)
		if err != nil {
			return err
		}
		err = cursor.Put([]byte("key1"), []byte("val1"), 0)
		if err != nil {
			return err
		}
		err = cursor.Put([]byte("key2"), []byte("val2"), 0)
		if err != nil {
			return err
		}

		// inspect the transaction
		stat, err := txn.Stat(dbi)
		if err != nil {
			return err
		}
		fmt.Println(stat.Entries)

		return nil
	})
	if err != nil {
		panic(err)
	}

	// scan the database and print all key-value pairs
	err = env.View(func(txn *lmdb.Txn) error {
		cursor, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cursor.Close()

		for {
			bkey, bval, err := cursor.Get(nil, nil, lmdb.Next)
			if err == lmdb.ErrNotFound {
				break
			}
			if err != nil {
				return err
			}
			fmt.Printf("%s: %s\n", bkey, bval)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// 3
	// key0: val0
	// key1: val1
	// key2: val2
}
