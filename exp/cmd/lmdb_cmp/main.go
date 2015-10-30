package main

/*
#include "lmdb.h"

typedef struct{const MDB_val *a; const MDB_val *b;} lmdb_cmp_t;

extern int lmdbCmp(lmdb_cmp_t cmp);

int lmdb_cmp_go(const MDB_val *a, const MDB_val *b);
int lmdb_cmp_c(const MDB_val *a, const MDB_val *b);
*/
import "C"
import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
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
