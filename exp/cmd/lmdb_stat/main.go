package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

func main() {
	opt := &Options{}
	flag.BoolVar(&opt.PrintVersion, "V", false, "print the LMDB library version and exit")
	flag.BoolVar(&opt.PrintInfo, "e", false, "print information about the database environment")
	flag.BoolVar(&opt.Debug, "debug", false, "print debug information")
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
	PrintVersion bool
	PrintInfo    bool
	PrintReaders bool
	PrintStatAll bool
	PrintStatSub string

	Debug bool
}

func doMain(opt *Options) error {
	if opt.PrintVersion {
		return doPrintVersion(opt)
	}
	return nil
}

func doPrintVersion(opt *Options) error {
	_, _, _, version := lmdb.Version()
	fmt.Println(version)
	return nil
}
