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

// NumLoop is the number of times each update should loop.
const NumLoop = 1000000

// MaxID is the highest key id.
const MaxID = 100000

// RootDir contains the mdb database for this program.
const RootDir = "data"

func main() {
	randSeed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	err := os.RemoveAll(RootDir)
	if err != nil {
		panic(err)
	}
	err = os.MkdirAll(RootDir, 0755)
	if err != nil {
		panic(err)
	}

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

	rand.Seed(*randSeed)

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

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		for i := 0; i < NumLoop; i++ {
			kn := rand.Intn(MaxID)
			k := fmt.Sprintf("k%d", kn)
			v := fmt.Sprintf("v%d", i)
			err = txn.Put(dbi, []byte(k), []byte(v), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		for i := 0; i < NumLoop; i++ {
			kn := rand.Intn(MaxID)
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
	if err != nil {
		panic(err)
	}

	err = env.Update(func(txn *lmdb.Txn) (err error) {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()
		for i := 0; i < NumLoop; i++ {
			kn := rand.Intn(MaxID)
			k := fmt.Sprintf("k%d", kn)
			v := fmt.Sprintf("v%d", i)
			err = cur.Put([]byte(k), []byte(v), 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
