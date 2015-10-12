package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/bmatsuo/lmdb-go/exp/cmd/internal/lmdbcmd"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

func main() {
	opt := &Options{}
	flag.BoolVar(&opt.Compact, "c", false, "Compact while copying.")
	flag.Parse()

	lmdbcmd.PrintVersion()

	if flag.NArg() != 2 {
		log.Fatalf("exactly two arguments must be specified")
	}

	srcpath := flag.Arg(0)
	dstpath := flag.Arg(1)

	copyEnv(srcpath, dstpath, opt)
}

type Options struct {
	Compact bool
}

func copyEnv(srcpath, dstpath string, opt *Options) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	err = env.Open(srcpath, lmdbcmd.OpenFlag(), 0644)
	if err != nil {
		return err
	}
	var flags uint
	if opt != nil && opt.Compact {
		flags |= lmdb.CopyCompact
	}
	if dstpath != "" {
		return env.CopyFlag(dstpath, flags)
	} else {
		return fmt.Errorf("TODO: implement Env.CopyFD")
	}
}
