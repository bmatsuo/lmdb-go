package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

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
	if opt.PrintInfo {
	}
	return nil
}

func doPrintVersion(opt *Options) error {
	_, _, _, version := lmdb.Version()
	fmt.Println(version)
	return nil
}
