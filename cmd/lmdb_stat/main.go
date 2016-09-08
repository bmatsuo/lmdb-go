/*
Command lmdb_stat is a clone of mdb_stat that displays the status an LMDB
environment.

Command line flags mirror the flags for the original program.  For information
about, run lmdb_stat with the -h flag.

	lmdb_stat -h
*/
package main

import "C"

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
	"unsafe"

	"github.com/bmatsuo/lmdb-go/internal/lmdbcmd"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
)

func main() {
	opt := &Options{}
	flag.BoolVar(&opt.PrintInfo, "e", false, "Display information about the database environment")
	flag.BoolVar(&opt.PrintFree, "f", false, "Display freelist information")
	flag.BoolVar(&opt.PrintFreeSummary, "ff", false, "Display freelist information")
	flag.BoolVar(&opt.PrintFreeFull, "fff", false, "Display freelist information")
	flag.BoolVar(&opt.PrintReaders, "r", false, strings.Join([]string{
		"Display information about the environment reader table.",
		"Shows the process ID, thread ID, and transaction ID for each active reader slot.",
	}, "  "))
	flag.BoolVar(&opt.PrintReadersCheck, "rr", false, strings.Join([]string{
		"Implies -r.",
		"Check for stale entries in the reader table and clear them.",
		"The reader table is printed again after the check is performed.",
	}, "  "))
	flag.BoolVar(&opt.PrintStatAll, "a", false, "Display the status of all databases in the environment")
	flag.StringVar(&opt.PrintStatSub, "s", "", "Display the status of a specific subdatabase.")
	flag.BoolVar(&opt.Debug, "D", false, "print debug information")
	flag.Parse()

	lmdbcmd.PrintVersion()

	if opt.PrintStatAll && opt.PrintStatSub != "" {
		log.Fatal("only one of -a and -s may be provided")
	}

	if flag.NArg() > 1 {
		log.Fatalf("too many argument provided")
	}
	if flag.NArg() == 0 {
		log.Fatalf("missing argument")
	}
	opt.Path = flag.Arg(0)

	var failed bool
	defer func() {
		if e := recover(); e != nil {
			if opt.Debug {
				panic(e)
			}
			log.Print(e)
			failed = true
		}
		if failed {
			os.Exit(1)
		}
	}()

	err := doMain(opt)
	if err != nil {
		log.Print(err)
		failed = true
	}
}

// Options contains all the configuration for an lmdb_stat command including
// command line arguments.
type Options struct {
	Path string

	PrintInfo         bool
	PrintReaders      bool
	PrintReadersCheck bool
	PrintFree         bool
	PrintFreeSummary  bool
	PrintFreeFull     bool
	PrintStatAll      bool
	PrintStatSub      string

	Debug bool
}

func doMain(opt *Options) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	if opt.PrintStatAll || opt.PrintStatSub != "" {
		err = env.SetMaxDBs(1)
		if err != nil {
			return err
		}
	}
	err = env.Open(opt.Path, lmdbcmd.OpenFlag(), 0644)
	defer env.Close()
	if err != nil {
		return err
	}

	if opt.PrintInfo {
		err = doPrintInfo(env, opt)
		if err != nil {
			return err
		}
	}

	if opt.PrintReaders || opt.PrintReadersCheck {
		err = doPrintReaders(env, opt)
		if err != nil {
			return err
		}
	}

	if opt.PrintFree || opt.PrintFreeSummary || opt.PrintFreeFull {
		err = doPrintFree(env, opt)
		if err != nil {
			return err
		}
	}

	err = doPrintStatRoot(env, opt)
	if err != nil {
		return err
	}

	if opt.PrintStatAll {
		err = doPrintStatAll(env, opt)
		if err != nil {
			return err
		}
	} else if opt.PrintStatSub != "" {
		err = doPrintStatDB(env, opt.PrintStatSub, opt)
		if err != nil {
			return err
		}
	}

	return nil
}

func doPrintInfo(env *lmdb.Env, opt *Options) error {
	info, err := env.Info()
	if err != nil {
		return err
	}

	pagesize := os.Getpagesize()

	fmt.Println("Environment Info")
	fmt.Println("  Map address:", nil)
	fmt.Println("  Map size:", info.MapSize)
	fmt.Println("  Page size:", pagesize)
	fmt.Println("  Max pages:", info.MapSize/int64(pagesize))
	fmt.Println("  Number of pages used:", info.LastPNO+1)
	fmt.Println("  Last transaction ID:", info.LastTxnID)
	fmt.Println("  Max readers:", info.MaxReaders)
	fmt.Println("  Number of readers used:", info.NumReaders)

	return nil
}

func doPrintReaders(env *lmdb.Env, opt *Options) error {
	fmt.Println("Reader Table Status")
	w := bufio.NewWriter(os.Stdout)
	err := printReaders(env, w, opt)
	if err == nil {
		err = w.Flush()
	}
	if err != nil {
		return err
	}

	if opt.PrintReadersCheck {
		numstale, err := env.ReaderCheck()
		if err != nil {
			return err
		}
		fmt.Printf("  %d stale readers cleared.\n", numstale)
		err = printReaders(env, w, opt)
		if err == nil {
			err = w.Flush()
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func printReaders(env *lmdb.Env, w io.Writer, opt *Options) error {
	return env.ReaderList(func(msg string) error {
		_, err := fmt.Fprint(w, msg)
		return err
	})
}

func doPrintFree(env *lmdb.Env, opt *Options) error {
	return env.View(func(txn *lmdb.Txn) (err error) {
		txn.RawRead = true

		fmt.Println("Freelist Status")

		stat, err := txn.Stat(0)
		if err != nil {
			return err
		}
		printStat(stat, opt)

		var numpages int64
		s := lmdbscan.New(txn, 0)
		defer s.Close()
		for s.Scan() {
			key := s.Key()
			data := s.Val()
			txid := *(*C.size_t)(unsafe.Pointer(&key[0]))
			ipages := int64(*(*C.size_t)(unsafe.Pointer(&data[0])))
			numpages += ipages
			if opt.PrintFreeSummary || opt.PrintFreeFull {
				bad := ""
				hdr := reflect.SliceHeader{
					Data: uintptr(unsafe.Pointer(&data[0])),
					Len:  int(ipages) + 1,
					Cap:  int(ipages) + 1,
				}
				pages := *(*[]C.size_t)(unsafe.Pointer(&hdr))
				pages = pages[1:]
				var span C.size_t
				prev := C.size_t(1)
				for i := ipages - 1; i >= 0; i-- {
					pg := pages[i]
					if pg < prev {
						bad = " [bad sequence]"
					}
					prev = pg
					pg += span
					for i >= int64(span) && pages[i-int64(span)] == pg {
						span++
						pg++
					}
				}
				fmt.Printf("    Transaction %d, %d pages, maxspan %d%s\n", txid, ipages, span, bad)

				if opt.PrintFreeFull {
					for j := ipages - 1; j >= 0; {
						pg := pages[j]
						j--
						span := C.size_t(1)
						for j >= 0 && pages[j] == pg+span {
							j--
							span++
						}
						if span > 1 {
							fmt.Printf("     %9d[%d]\n", pg, span)
						} else {
							fmt.Printf("     %9d\n", pg)
						}
					}
				}
			}
		}
		err = s.Err()
		if err != nil {
			return err
		}

		fmt.Println("  Free pages:", numpages)

		return nil
	})
}

func doPrintStatRoot(env *lmdb.Env, opt *Options) error {
	stat, err := env.Stat()
	if err != nil {
		return err
	}

	fmt.Println("Status of Main DB")
	fmt.Println("  Tree depth:", stat.Depth)
	fmt.Println("  Branch pages:", stat.BranchPages)
	fmt.Println("  Leaf pages:", stat.LeafPages)
	fmt.Println("  Overflow pages:", stat.OverflowPages)
	fmt.Println("  Entries:", stat.Entries)

	return nil
}

func doPrintStatDB(env *lmdb.Env, db string, opt *Options) error {
	err := env.View(func(txn *lmdb.Txn) (err error) {
		return printStatDB(env, txn, db, opt)
	})
	if err != nil {
		return fmt.Errorf("%v (%s)", err, db)
	}
	return nil
}

func printStatDB(env *lmdb.Env, txn *lmdb.Txn, db string, opt *Options) error {
	dbi, err := txn.OpenDBI(db, 0)
	if err != nil {
		return err
	}
	defer env.CloseDBI(dbi)

	stat, err := txn.Stat(dbi)
	if err != nil {
		return err
	}

	fmt.Println("Status of", db)
	printStat(stat, opt)

	return err
}

func printStat(stat *lmdb.Stat, opt *Options) error {
	fmt.Println("  Tree depth:", stat.Depth)
	fmt.Println("  Branch pages:", stat.BranchPages)
	fmt.Println("  Leaf pages:", stat.LeafPages)
	fmt.Println("  Overflow pages:", stat.OverflowPages)
	fmt.Println("  Entries:", stat.Entries)

	return nil
}

func doPrintStatAll(env *lmdb.Env, opt *Options) error {
	return env.View(func(txn *lmdb.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		defer env.CloseDBI(dbi)

		s := lmdbscan.New(txn, dbi)
		defer s.Close()
		for s.Scan() {
			err = printStatDB(env, txn, string(s.Key()), opt)
			if e, ok := err.(*lmdb.OpError); ok {
				if e.Op == "mdb_dbi_open" {
					continue
				}
			}
			if err != nil {
				return fmt.Errorf("%v (%s)", err, s.Key())
			}
		}
		return s.Err()
	})
}
