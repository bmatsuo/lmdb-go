package lmdb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// These values shouldn't actually be assigned to.  The are used as stand-ins
// for tests which do not act as tests.
var EnvEx *lmdb.Env
var DBIEx lmdb.DBI

// These values can only be used is code-only examples (no test output).
var env *lmdb.Env
var txn *lmdb.Txn
var dbi lmdb.DBI
var dbname string
var err error

// These values can be used as no-op placeholders in examples.
func doUpdate(txn *lmdb.Txn) error { return nil }
func doView(txn *lmdb.Txn) error   { return nil }

// This example demonstrates how an application typically uses Env.SetMapSize.
// The call to Env.SetMapSize() is made before calling env.Open().  Any calls
// after calling Env.Open() must take special care to synchronize with other
// goroutines.
func ExampleEnv_SetMapSize() {
	env, err := lmdb.NewEnv()
	if err != nil {
		// ...
	}

	// set the memory map size (maximum database size) to 1GB.
	err = env.SetMapSize(1 << 30)
	if err != nil {
		// ...
	}

	err = env.Open("mydb", 0, 0644)
	if err != nil {
		// ...
	}
	// ...
}

// This example demonstrates how to handle a MapResized error, encountered
// after another process has called mdb_env_set_mapsize (Env.SetMapSize).
// Applications which don't expect another process to resize the mmap don't
// need to check for the MapResized error.
//
// The example is simplified for clarity.  Many real applications will need to
// synchronize calls to Env.SetMapSize using something like a sync.RWMutex to
// ensure there are no active readonly transactions (those opened successfully
// before MapResized was encountered).
func ExampleEnv_SetMapSize_mapResized() {
retry:
	err := env.Update(doUpdate)
	if lmdb.IsMapResized(err) {
		// If concurrent read transactions are possible then a sync.RWMutex
		// must be used here to ensure they all terminate before calling
		// env.SetMapSize().
		err = env.SetMapSize(0)
		if err != nil {
			panic(err)
		}

		// retry the update. a goto is not necessary but it simplifies error
		// handling with minimal overhead.
		goto retry
	} else if err != nil {
		// ...
	}
	// ...
}

// This complete example demonstrates populating and iterating a database with
// the DupFixed|DupSort DBI flags.  The use case is probably too trivial to
// warrant such optimization but it demonstrates the key points.
//
// Note the importance of supplying both DupFixed and DupSort flags on database
// creation.
func ExampleTxn_dupFixed() {
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
	err = env.Open(path, 0, 0644)
	defer env.Close()
	if err != nil {
		log.Panic(err)
	}

	// open the database of friends' phone numbers.  in this limited world
	// phone nubers are all the same length.
	var phonedbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		phonedbi, err = txn.OpenRoot(lmdb.DupSort | lmdb.DupFixed)
		return
	})
	if err != nil {
		panic(err)
	}

	// load some static values into the phone database.  values are loaded in
	// bulk using the PutMulti method on Cursor.
	err = env.Update(func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(phonedbi)
		if err != nil {
			return fmt.Errorf("cursor: %v", err)
		}
		defer cur.Close()

		for _, entry := range []struct {
			name    string
			numbers []string
		}{
			{"alice", []string{"234-1234"}},
			{"bob", []string{"825-1234"}},
			{"carol", []string{"828-1234", "824-1234", "502-1234"}}, // values are not sorted
			{"bob", []string{"433-1234", "957-1234"}},               // sorted dup values may be interleaved with existing dups
			{"jenny", []string{"867-5309"}},
		} {
			// write the values into a contiguous chunk of memory.  it is
			// critical that the values have the same length so the page has an
			// even stride.
			stride := len(entry.numbers[0])
			pagelen := stride * len(entry.numbers)
			data := bytes.NewBuffer(make([]byte, 0, pagelen))
			for _, num := range entry.numbers {
				io.WriteString(data, num)
			}

			// write the values to the database.
			err = cur.PutMulti([]byte(entry.name), data.Bytes(), stride, 0)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// grab the first page of phone numbers for each name and print them.
	err = env.View(func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(phonedbi)
		if err != nil {
			return fmt.Errorf("cursor: %v", err)
		}
		defer cur.Close()
		for {
			// move to the next key
			name, phoneFirst, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				break
			}
			if err != nil {
				return fmt.Errorf("nextnodup: %v", err)
			}

			// determine if multiple values should be printed and short circuit
			// if not.
			ndup, err := cur.Count()
			if err != nil {
				return fmt.Errorf("count: %v", err)
			}
			if ndup == 1 {
				fmt.Printf("%s %s\n", name, phoneFirst)
				continue
			}

			// get a page of records and split it into discrete values.  the
			// length of the first item is used to split the page of contiguous
			// values.
			_, page, err := cur.Get(nil, nil, lmdb.GetMultiple)
			if err != nil {
				return fmt.Errorf("getmultiple: %v", err)
			}

			// print the phone numbers for the person. the first number is
			// printed on the same line as the person's name. others numbers of
			// offset to the same depth as the primary number.
			m := lmdb.WrapMulti(page, len(phoneFirst))
			fmt.Printf("%s %s\n", name, m.Val(0))
			offsetRest := bytes.Repeat([]byte{' '}, len(name))
			for i, n := 1, m.Len(); i < n; i++ {
				fmt.Printf("%s %s\n", offsetRest, m.Val(i))
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
func ExampleTxn_dupSort() {
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
	err = env.Open(path, 0, 0644)
	defer env.Close()
	if err != nil {
		log.Panic(err)
	}

	var phonedbi lmdb.DBI

	// open the database of friends' phone numbers.  a single person can have
	// multiple phone numbers.
	err = env.Update(func(txn *lmdb.Txn) error {
		dbi, err := txn.OpenRoot(lmdb.DupSort)
		if err != nil {
			return err
		}
		phonedbi = dbi
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for _, entry := range []struct{ name, number string }{
			{"alice", "234-1234"},
			{"bob", "825-1234"},
			{"carol", "824-1234"},
			{"jenny", "867-5309"},
			{"carol", "828-1234"}, // a second value for the key
			{"carol", "502-1234"}, // will be retrieved in sorted order
		} {
			err = cur.Put([]byte(entry.name), []byte(entry.number), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// iterate the database and print the phone numbers for each name.
	// multiple phone numbers for the same name are printed aligned on separate
	// rows.
	env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(phonedbi)
		if err != nil {
			return err
		}

		var nameprev, name, phone []byte
		for {
			name, phone, err = cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				// the database was exausted
				return nil
			} else if err != nil {
				return err
			}

			// print the name and phone number. offset with space instead of
			// printing the name if name is a duplicate.
			isdup := bytes.Equal(nameprev, name)
			nameprev = name
			firstcol := name
			if isdup {
				firstcol = bytes.Repeat([]byte{' '}, len(name))
			}
			fmt.Printf("%s %s\n", firstcol, phone)
		}
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// alice 234-1234
	// bob 825-1234
	// carol 502-1234
	//       824-1234
	//       828-1234
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
		return err
	})
	if err != nil {
		panic(err)
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		// it can be helpful to define closures that abstract the transaction
		// and short circuit after errors.
		put := func(k, v string) {
			if err == nil {
				err = txn.Put(dbi, []byte(k), []byte(v), 0)
			}
		}

		// use the closure above to insert into the database.
		put("key0", "val0")
		put("key1", "val1")
		put("key2", "val2")

		return err
	})
	if err != nil {
		panic(err)
	}

	err = env.View(func(txn *lmdb.Txn) error {
		// databases can be inspected inside transactions.  here the number of
		// entries (keys) are printed.
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

	err = env.Update(func(txn *lmdb.Txn) error {
		// random access of a key
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
			if lmdb.IsNotFound(err) {
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

// This simple example shows how to iterate a database.  The lmdb.Next flag may
// be used without an initial call using lmdb.First.
func ExampleCursor_Get() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			fmt.Printf("%s %s\n", k, v)
		}
	})
}

// This simple example shows how to iterate a database in reverse.  As when
// calling lmdb.Next, the lmdb.Prev flag may be used without an initial call
// using lmdb.Last.
func ExampleCursor_Get_reverse() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, lmdb.Prev)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			fmt.Printf("%s %s\n", k, v)
		}
	})
}

// This example shows how to write a page of contiguous, fixed-size values to a
// database opened with DupSort|DupFixed.  It doesn't matter if the values are
// sorted.  Values will be stored in sorted order.
func ExampleCursor_PutMulti() {
	key := []byte("k")
	items := [][]byte{
		[]byte("v0"),
		[]byte("v2"),
		[]byte("v1"),
	}
	page := bytes.Join(items, nil)
	stride := 2

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		return cur.PutMulti(key, page, stride, 0)
	})
}

// This example demonstrates how to delete all elements in a database with a
// key less than a given value (an RFC3339 timestamp in this case).
func ExampleCursor_Del() {
	before := []byte("2014-05-06T03:04:02Z")
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, v, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			if bytes.Compare(k, before) != -1 {
				return nil
			}

			err = cur.Del(0)
			if err != nil {
				return err
			}
		}
	})
}

// Txn.OpenRoot does not need to be called with the lmdb.Create flag.
func ExampleTxn_OpenRoot() {
	err := EnvEx.Update(func(txn *lmdb.Txn) (err error) {
		DBIEx, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		panic(err)
	}
}

// Txn.OpenRoot may be called without flags inside View transactions.
func ExampleTxn_OpenRoot_view() {
	err := EnvEx.View(func(txn *lmdb.Txn) (err error) {
		db, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err := txn.OpenCursor(db)
		if err != nil {
			return err
		}
		for {
			k, v, err := cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s: %s", k, v)
		}
	})
	if err != nil {
		panic(err)
	}
}

// This example shows how to properly handle data retrieved from the database
// and applies to Txn.Get() as well as Cursor.Get().  It is important to handle
// data retreival carefully to make sure the application does not retain
// pointers to memory pages which may be reclaimed by LMDB after the
// transaction terminates.  Typically an application would define helper
// functions/methods to conveniently handle data safe retrieval.
func ExampleTxn_Get() {
	// variables to hold data extracted from the database
	var point struct{ X, Y int }
	var str string
	var p1, p2 []byte

	// extract data from an example environment/database.  it is critical for application
	// code to handle errors  but that is omitted here to save space.
	EnvEx.View(func(txn *lmdb.Txn) (err error) {
		// OK
		// A []byte to string conversion will always copy the data
		v, _ := txn.Get(DBIEx, []byte("mykey"))
		str = string(v)

		// OK
		// If []byte is the desired data type then an explicit copy is required
		// for safe access after the transaction returns.
		v, _ = txn.Get(DBIEx, []byte("mykey"))
		p1 = make([]byte, len(v))
		copy(p1, v)

		// OK
		// The data does not need be copied because it is parsed while txn is
		// open.
		v, _ = txn.Get(DBIEx, []byte("mykey"))
		_ = json.Unmarshal(v, &point)

		// BAD
		// Assigning the result directly to p2 leaves its pointer volatile
		// after the transaction completes which can result in unpredictable
		// behavior.
		p2, _ = txn.Get(DBIEx, []byte("mykey"))

		return nil
	})
}

// This example demonstrates the use of PutReserve to store a string value in
// the root database.  This may be faster than Put alone for large values
// because a string to []byte conversion is not required.
func ExampleTxn_PutReserve() {
	EnvEx.Update(func(txn *lmdb.Txn) (err error) {
		dbroot, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		valstr := "value"
		p, err := txn.PutReserve(dbroot, []byte("key"), len(valstr), 0)
		if err != nil {
			return err
		}
		copy(p, valstr)

		return nil
	})
}
