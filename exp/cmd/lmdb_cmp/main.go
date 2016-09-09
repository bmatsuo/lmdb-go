package main

/*
#include "lmdb.h"
#include "compare.h"
*/
import "C"
import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"unsafe"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// NumLoop is the number of times each update should loop.
const NumLoop = 1000000

// MaxID is the highest key id.
const MaxID = 100000

func main() {
	randSeed := flag.Int64("seed", 1, "random seed")
	cmpfunc := flag.String("func", "c", "comparison func implementation to use")
	flag.Parse()
	env, err := lmdb.NewEnv()
	if err != nil {
		panic(err)
	}
	err = env.SetMapSize(100 << 20)
	if err != nil {
		panic(err)
	}
	err = env.Open("db.mdb", lmdb.NoSubdir, 0644)
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
		switch *cmpfunc {
		case "c":
			err = txn.SetCmp(dbi, (*lmdb.CmpFunc)(unsafe.Pointer(C.lmdb_cmp_c)))
		case "go":
			err = txn.SetCmp(dbi, (*lmdb.CmpFunc)(unsafe.Pointer(C.lmdb_cmp_go)))
		case "dyn":
			err = txn.SetCmp(dbi, (*lmdb.CmpFunc)(unsafe.Pointer(C.lmdb_cmp_dyn)))
		}
		if err != nil {
			return err
		}
		return nil
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

var rwmut = &sync.RWMutex{}

var cmpMap = map[int]func(c C.lmdb_cmp_t) C.int{
	0: nil,
	1: nil,
	2: lmdbCmp,
}

//export lmdbCmpDyn
func lmdbCmpDyn(c C.lmdb_cmp_t, ctx C.size_t) C.int {
	rwmut.RLock()
	fn := cmpMap[int(ctx)]
	rwmut.RUnlock()
	return fn(c)
}

//export lmdbCmp
func lmdbCmp(c C.lmdb_cmp_t) C.int {
	p1 := mdbValBytes(c.a)
	p2 := mdbValBytes(c.b)
	return C.int(-bytes.Compare(p1, p2))
}

func mdbValBytes(val *C.MDB_val) []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(val.mv_data)),
		Len:  int(val.mv_size),
		Cap:  int(val.mv_size),
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}
