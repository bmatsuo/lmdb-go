package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

func main() {
	opt := &Options{}
	flag.BoolVar(&opt.PrintVersion, "V", false, "Write the library version to standard output, and exit.")
	flag.BoolVar(&opt.PrintInfo, "e", false, "Display information about the database environment")
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

type Options struct {
	Path string

	PrintVersion      bool
	PrintInfo         bool
	PrintReaders      bool
	PrintReadersCheck bool
	PrintStatAll      bool
	PrintStatSub      string

	Debug bool
}

func doMain(opt *Options) error {
	if opt.PrintVersion {
		return doPrintVersion(opt)
	}

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
	err = env.Open(opt.Path, 0, 0644)
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

func doPrintVersion(opt *Options) error {
	_, _, _, version := lmdb.Version()
	fmt.Println(version)
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

func doPrintStatRoot(env *lmdb.Env, opt *Options) error {
	stat, err := env.Stat()
	if err != nil {
		return err
	}

	fmt.Println("Status of Main DB")
	fmt.Println("  Tree depth:", stat.Depth)
	fmt.Println("  Branch pages:", stat.BranchPages)
	fmt.Println("  Lead pages:", stat.LeafPages)
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
	fmt.Println("  Tree depth:", stat.Depth)
	fmt.Println("  Branch pages:", stat.BranchPages)
	fmt.Println("  Lead pages:", stat.LeafPages)
	fmt.Println("  Overflow pages:", stat.OverflowPages)
	fmt.Println("  Entries:", stat.Entries)

	return err
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
				if e.Op == "mdb_dbi_open" && e.Errno == lmdb.Incompatible {
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
