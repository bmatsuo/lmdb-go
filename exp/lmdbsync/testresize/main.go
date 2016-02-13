/*
Command testresize is a utility used by the lmdbsync tests to validate its
multiprocessing capabilities.  An external command like testresize command is
required because a process is not allowed to map the same process twice.

Testresize writes batches of updates into a databaes, transparently handling
any lmdb.MapResized or lmdb.MapFull errors that occur.  To ensure that resizing
behavior is observed testresize waits for input before updating the environment
and writes a line to stdout after the update is committed.  If testresize
process observes zero of either error it will exit with a non-zero exit code.

Two testresize processes can communicate to each other using two unix pipes, if
the output of each pipe is connected to the input of the other.  Writing a
single line to one of the pipes will cause updates to ping-pong back and forth
between processes.
*/
package main

import (
	"bufio"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"golang.org/x/net/context"

	"github.com/bmatsuo/lmdb-go/exp/lmdbsync"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

func main() {
	numitems := flag.Int64("n", 5<<10, "the number of items to write")
	chunksize := flag.Int64("c", 100, "the number of items to write per txn")
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

	err := WriteRandomItems("db", *numitems, *chunksize)
	if err != nil {
		fail(err)
	} else {
		log.Printf("success")
	}
}

// WriteRandomItems writes numitem items with chunksize sized values full of
// random data.
func WriteRandomItems(path string, numitem, chunksize int64) (err error) {
	env, err := OpenEnv(path)
	if err != nil {
		return err
	}
	defer env.Close()

	numResize := 0
	numResized := 0
	defer func() {
		log.Printf("%d resizes", numResize)
		log.Printf("%d size adoptions", numResized)
		if err == nil {
			if numResize == 0 {
				err = fmt.Errorf("process did not resize the memory map")
			} else if numResized == 0 {
				err = fmt.Errorf("process did not adopt a new map size")
			}
		}
	}()
	mapResizedLogger := func(ctx context.Context, env *lmdbsync.Env, err error) (context.Context, error) {
		if lmdb.IsMapResized(err) {
			log.Printf("map resized")
			numResized++
		}
		return ctx, err
	}
	mapFullLogger := func(ctx context.Context, env *lmdbsync.Env, err error) (context.Context, error) {
		if lmdb.IsMapFull(err) {
			log.Printf("resize required")
			numResize++
		}
		return ctx, err
	}
	env.Handlers = env.Handlers.Append(
		handlerFunc(mapResizedLogger),
		lmdbsync.MapResizedHandler(2, func(int) time.Duration { return 100 * time.Microsecond }),
		handlerFunc(mapFullLogger),
		lmdbsync.MapFullHandler(func(size int64) (int64, bool) {
			newsize := size + 128<<10 // linear scale is bad -- but useful to test
			log.Printf("oldsize=%d newsize=%d", size, newsize)
			return newsize, true
		}),
	)

	pid := os.Getpid()

	scanner := bufio.NewScanner(os.Stdin)
	for i := int64(0); i < numitem; {
		if !scanner.Scan() {
			return scanner.Err()
		}

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
		err = env.Update(func(txn *lmdb.Txn) (err error) {
			dbi, err := txn.OpenRoot(0)
			if err != nil {
				return err
			}

			for i = start; i < chunkmax; i++ {
				k := fmt.Sprintf("%06d-%016x", pid, i)
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
		fmt.Println("ok")
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

type handlerFunc func(ctx context.Context, env *lmdbsync.Env, err error) (context.Context, error)

func (fn handlerFunc) HandleTxnErr(ctx context.Context, env *lmdbsync.Env, err error) (context.Context, error) {
	return fn(ctx, env, err)
}
