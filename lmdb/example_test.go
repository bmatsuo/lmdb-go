package lmdb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

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
var stop chan struct{}

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

func backupFailed(err error) {}

// This example uses env.Copy to periodically create atomic backups of an
// environment.  A real implementation would need to solve problems with
// failures, remote persistence, purging old backups, etc.  But the core loop
// would have the same form.
func ExampleEnv_Copy() {
	go func(backup <-chan time.Time) {
		for {
			select {
			case <-backup:
				now := time.Now().UTC()
				backup := fmt.Sprintf("backup-%s", now.Format(time.RFC3339))
				os.Mkdir(backup, 0755)
				err = env.Copy(backup)
				if err != nil {
					// ...
					continue
				}
			case <-stop:
				return
			}
		}
	}(time.Tick(time.Hour))
}

// This example shows the general workflow of an environment.  A database is
// created and configured before being opened.  After the environment is opened
// its databases are created and their handles are saved for use in future
// transactions.
func ExampleEnv() {
	// open the LMDB environment and configure common options like its size and
	// maximum number of databases.
	env, err := lmdb.NewEnv()
	if err != nil {
		// ...
	}
	err = env.SetMapSize(100 * 1024 * 1024) // 100MB
	if err != nil {
		// ...
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		// ...
	}

	// open the environment only after the environment has been configured.
	// some settings may only be called before the environment is opened where
	// others may have caveats.
	err = env.Open("mydb/", 0, 0664)
	if err != nil {
		// ...
	}
	defer env.Close()

	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		// open a database, creating it if necessary.  the database is stored
		// outside the transaction via closure and can be use after the
		// transaction is committed.
		dbi, err = txn.OpenDBI("exampledb", lmdb.Create)
		if err != nil {
			return err
		}

		// commit the transaction, writing an entry for the newly created
		// database if it was just created and allowing the dbi to be used in
		// future transactions.
		return nil
	})
	if err != nil {
		panic(err)
	}
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

// This example demonstrates how to iterate a database opened with lmdb.DupSort
// and get the number of values present for each unique key.
func ExampleCursor_Count() {
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, _, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			numdup, err := cur.Count()
			if err != nil {
				return err
			}
			fmt.Printf("%d values for key %q", numdup, k)
		}
	})
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

// This example shows how duplicates can be processed using LMDB.  It is
// possible to iterate all key-value pairs (including duplicate key values)
// using lmdb.Next.  But if special handling of duplicates is needed it may be
// beneficial to use lmdb.NextNoDup or lmdb.NextDup.
func ExampleCursor_Get_dupSort() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}

		for {
			k, v, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				// the database was exausted
				return nil
			} else if err != nil {
				return err
			}

			// process duplicates
			var dups [][]byte
			for {
				dups = append(dups, v)

				_, v, err = cur.Get(nil, nil, lmdb.NextDup)
				if lmdb.IsNotFound(err) {
					break
				} else if err != nil {
					return err
				}
			}

			log.Printf("%q %q", k, dups)
		}
	})
}

// This simple example shows how to iterate a database opened with the
// DupSort|DupSort flags.  It is not necessary to use the GetMultiple flag
// before passing the NextMultiple flag.
func ExampleCursor_Get_dupFixed() {
	err = env.View(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		for {
			k, first, err := cur.Get(nil, nil, lmdb.NextNoDup)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}

			stride := len(first)

			for {
				_, v, err := cur.Get(nil, nil, lmdb.NextMultiple)
				if lmdb.IsNotFound(err) {
					break
				}
				if err != nil {
					return err
				}

				multi := lmdb.WrapMulti(v, stride)
				for i := 0; i < multi.Len(); i++ {
					fmt.Printf("%s %s\n", k, multi.Val(i))
				}
			}
		}
	})
}

// This example shows a trivial case using Renew to service read requests on a
// database.  Close must be called when the cursor will no longer be renewed.
// Before using Renew benchmark your application to understand its benefits.
func ExampleCursor_Renew() {
	var cur *lmdb.Cursor
	err = env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		cur, err = txn.OpenCursor(dbi)
		return err
	})
	if err != nil {
		panic(err)
	}

	keys := make(chan []byte)
	go func() {

		// Close must called when the cursor is no longer needed.
		defer cur.Close()

		for key := range keys {
			err := env.View(func(txn *lmdb.Txn) (err error) {
				err = cur.Renew(txn)
				if err != nil {
					return err
				}

				// retrieve the number of items in the database with the given
				// key (lmdb.DupSort).
				count := uint64(0)
				_, _, err = cur.Get(key, nil, lmdb.SetKey)
				if lmdb.IsNotFound(err) {
					err = nil
				} else if err == nil {
					count, err = cur.Count()
				}
				if err != nil {
					return err
				}

				log.Printf("%d %q", count, key)

				return nil
			})
			if err != nil {
				panic(err)
			}
		}
	}()

	// ...
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
			k, _, err := cur.Get(nil, nil, lmdb.Next)
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
