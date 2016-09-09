/*
The lmdb_cmp_simple command demonstrates the recommended way to set up custom
comparison functions for databases in an lmdb environment.  The example creates
a database using a custom comparison function to sort keys in an application
defined order.

Custom comparison functions are most commonly used key values using structured
data.  They can be used to create a compound index with keys consisting of
multiple data fields.  There are many ways that applications can benefit from
custom comparison functions, but their use comes with a development cost that
is better avoided when practical.

The only supported method to define custom comparison functinos is to use
static C functions that are defined in a C (header) file or in the preamble of
a CGO source file.  Using Go functions for comparison cannot be officially
supported primarily due to inadequate speed, type checking problems, and
reliance on unspecified behaviors.
*/
package main

/*
#include "lmdb.h"
#include "compare.h"
*/
import "C"
import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"unsafe"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// BatchSize is the size of each update (additions or deletions).
const BatchSize = 1000000

// MaxID is the highest key id.
const MaxID = 100000

// RootDir contains the mdb database for this program.
const RootDir = "data"

func main() {
	randSeed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	// Perform setup that isn't all that important.  Seed the RNG and create an
	// empty work directory.
	rand.Seed(*randSeed)
	err := os.RemoveAll(RootDir)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(RootDir, 0755)
	if err != nil {
		panic(err)
	}

	// Env initialization takes place normally.
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	err = env.SetMapSize(100 << 20)
	if err != nil {
		panic(err)
	}
	err = env.Open(RootDir, 0, 0644)
	if err != nil {
		panic(err)
	}
	defer env.Close()

	// When the DBI is first opened we set its comparison function in the same
	// transaction.  It is extremely important that setting the comparison
	// function be the first operation on the DBI.
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.SetCmp(dbi, (*lmdb.CmpFunc)(unsafe.Pointer(C.lmdb_cmp_c)))
	})
	if err != nil {
		panic(err)
	}

	// Now you can use the DBI as normal, utilizing the custom comparison
	// function.
	err = populate(env, dbi, NumLoop, MaxID)
	if err != nil {
		panic(err)
	}
	err = clear(env, dbi, NumLoop, MaxID)
	if err != nil {
		panic(err)
	}
	err = populate(env, dbi, NumLoop, MaxID)
	if err != nil {
		panic(err)
	}
}

// populate adds random entries to the dbi in env.
func populate(env *lmdb.Env, dbi lmdb.DBI, entries, maxid int) error {
	return env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		for i := 0; i < entries; i++ {
			kn := rand.Intn(maxid)
			k := fmt.Sprintf("k%d", kn)
			v := fmt.Sprintf("v%d", i)
			err = cur.Put([]byte(k), []byte(v), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// clear removes random entries from the dbi in env
func clear(env *lmdb.Env, dbi lmdb.DBI, maxid, entries int) error {
	return env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		for i := 0; i < entries; i++ {
			kn := rand.Intn(maxid)
			k := fmt.Sprintf("k%d", kn)
			_, _, err = cur.Get([]byte(k), nil, lmdb.Set)
			if lmdb.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			err = cur.Del(0)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
