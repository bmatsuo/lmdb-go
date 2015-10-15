package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/bmatsuo/lmdb-go/exp/lmdbsync"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

func main() {
	flag.Parse()

	failed := false
	defer func() {
		if failed {
			os.Exit(1)
		}
	}()
	fail := func(err error) {
		failed = true
		log.Print(err)
	}

	err := WriteRandomItems("db", 1<<20, 10<<10)
	if err != nil {
		fail(err)
	} else {
		log.Printf("success")
	}
}

// WriteRandomItems writes numitem items with checksize sized values full of
// random data.
func WriteRandomItems(path string, numitem, chunksize int64) error {
	env, err := OpenEnv(path)
	if err != nil {
		return err
	}
	defer env.Close()

	numResize := 0
	defer func() {
		log.Printf("%d resizes", numResize)
	}()
	mapFullLogger := func(b lmdbsync.Bag, err error) (lmdbsync.Bag, error) {
		if lmdb.IsMapFull(err) {
			log.Printf("resize required: %v", err)
			numResize++
		}
		return b, err
	}
	env.Handlers = env.Handlers.Append(
		lmdbsync.MapResizedHandler(2, func(int) time.Duration { return 100 * time.Microsecond }),
		handlerFunc(mapFullLogger),
		lmdbsync.MapFullHandler(func(size int64) (int64, bool) {
			newsize := size * 2
			log.Printf("oldsize=%d newsize=%d", size, newsize)
			return newsize, true
		}),
	)

	pid := os.Getpid()

	for i := int64(0); i < numitem; {
		start := i
		chunkmax := i + chunksize
		if chunkmax > numitem {
			chunkmax = numitem
		}
		v := make([]byte, 512)
		_, err := io.ReadFull(rand.Reader, v)
		if err != nil {
			return err
		}
		log.Printf("i=%d", i)
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			dbi, err := txn.OpenRoot(0)
			if err != nil {
				return err
			}

			for i = start; i < chunkmax; i++ {
				k := fmt.Sprintf("%d-%016x", pid, i)
				err = txn.Put(dbi, []byte(k), v, 0)
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// OpenEnv is a helper for opening an lmdbsync.Env.
func OpenEnv(path string) (*lmdbsync.Env, error) {
	env, err := lmdbsync.NewEnv(nil)
	if err != nil {
		return nil, err
	}
	err = env.Open(path, 0, 0644)
	if err != nil {
		env.Close()
		return nil, err
	}
	return env, nil
}

type handlerFunc func(b lmdbsync.Bag, err error) (lmdbsync.Bag, error)

func (fn handlerFunc) HandleTxnErr(b lmdbsync.Bag, err error) (lmdbsync.Bag, error) {
	return fn(b, err)
}
