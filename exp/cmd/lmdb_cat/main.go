package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/bmatsuo/lmdb-go/exp/lmdbsync"
	"github.com/bmatsuo/lmdb-go/internal/lmdbcmd"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
)

func main() {
	opt := &Options{}
	flag.BoolVar(&opt.ReadIn, "i", false, "Read data from standard input and write it the specefied database.")
	flag.BoolVar(&opt.KeyOnly, "k", false, "Do not write value data to standard output")
	flag.BoolVar(&opt.ValOnly, "K", false, "Do not write key data to standard output")
	flag.StringVar(&opt.Sep, "F", "=", "Key-value delimiter for items written to standard output, or read from standard output.")
	flag.Parse()

	lmdbcmd.PrintVersion()

	dbs := flag.Args()
	var specs []*catSpec
	for _, db := range dbs {
		spec, err := parseCatSpec(db)
		if err != nil {
			log.Fatal(err)
		}
		specs = append(specs, spec)
	}

	if opt.ReadIn {
		if len(specs) > 1 || len(specs[0].DB) > 1 {
			log.Fatalf("only one database may be specefied when -i is given")
		}
		if len(specs[0].DB) > 0 {
			opt.DB = specs[0].DB[0]
		}
		if opt.KeyOnly || opt.ValOnly {
			log.Fatal("flags -k and -K must be omitted when -i is given")
		}
		if opt.Sep == "" {
			log.Fatal("delimiter -F cannot be empty when -i is given")
		}

		err := readIn(specs[0].Path, os.Stdin, opt)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	for _, spec := range specs {
		opt := &catOptions{DB: spec.DB}
		cat(spec.Path, opt)
	}
}

// Options contains the the configuration to perform an lmdb_cat on a single
// environment/database.
type Options struct {
	ReadIn  bool
	KeyOnly bool
	ValOnly bool
	Sep     string
	DB      string
}

func readIn(path string, r io.Reader, opt *Options) error {
	_env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	err = _env.SetMapSize(100 << 10)
	if err != nil {
		return err
	}
	if opt != nil && opt.DB != "" {
		err = _env.SetMaxDBs(1)
		if err != nil {
			return err
		}
	}
	err = _env.Open(path, lmdbcmd.OpenFlag(), 0644)
	defer _env.Close()
	if err != nil {
		return err
	}
	doubleSize := func(size int64) (int64, bool) { return size * 2, true }
	handler := lmdbsync.MapFullHandler(doubleSize)
	env, err := lmdbsync.NewEnv(_env, handler)
	if err != nil {
		return err
	}
	return env.Update(func(txn *lmdb.Txn) (err error) {
		var dbi lmdb.DBI
		if opt.DB == "" {
			dbi, err = txn.OpenRoot(0)
		} else {
			dbi, err = txn.OpenDBI(opt.DB, lmdb.Create)
		}
		if err != nil {
			return err
		}

		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		sep := []byte(opt.Sep)

		numln := 0
		s := bufio.NewScanner(r)
		for s.Scan() {
			numln++
			ln := s.Bytes()
			pieces := bytes.SplitN(ln, sep, 2)
			if len(pieces) < 2 {
				log.Printf("line %d: missing separator", numln)
				continue
			}
			err = cur.Put(pieces[0], pieces[1], 0)
			if err != nil {
				return err
			}
		}
		return s.Err()
	})
}

type catSpec struct {
	Path string
	DB   []string
}

// BUG:
// this function is shit
func parseCatSpec(s string) (*catSpec, error) {
	s = strings.TrimSpace(s)
	dbspec := strings.Index(s, "[")
	if dbspec < 0 {
		spec := &catSpec{Path: s}
		return spec, nil
	}
	if !strings.HasSuffix(s, "]") {
		return nil, fmt.Errorf("invalid db spec")
	}
	dbs := strings.Split(s[dbspec+1:len(s)-1], ":")
	spec := &catSpec{Path: s[:dbspec], DB: dbs}
	return spec, nil
}

type catOptions struct {
	DB []string
}

func cat(path string, opt *catOptions) error {
	env, err := lmdb.NewEnv()
	if err != nil {
		return err
	}
	maxdbs := 0
	if opt != nil && len(opt.DB) > 0 {
		maxdbs = len(opt.DB)
	}
	if maxdbs > 0 {
		err = env.SetMaxDBs(maxdbs)
	}
	if err != nil {
		return err
	}
	err = env.Open(path, lmdbcmd.OpenFlag(), 644)
	defer env.Close()
	if err != nil {
		return err
	}
	return env.View(func(txn *lmdb.Txn) (err error) {
		if opt == nil || len(opt.DB) == 0 {
			err := catRoot(txn)
			if err != nil {
				return err
			}
		}
		if opt != nil {
			for _, dbname := range opt.DB {
				err := catDB(txn, dbname)
				if err != nil {
					return fmt.Errorf("%v (%q)", err, dbname)
				}
			}
		}
		return nil
	})
}

func catDB(txn *lmdb.Txn, dbname string) error {
	dbi, err := txn.OpenDBI(dbname, 0)
	if err != nil {
		return err
	}
	return catDBI(txn, dbi)
}

func catRoot(txn *lmdb.Txn) error {
	dbi, err := txn.OpenRoot(0)
	if err != nil {
		return err
	}
	return catDBI(txn, dbi)
}

func catDBI(txn *lmdb.Txn, dbi lmdb.DBI) error {
	s := lmdbscan.New(txn, dbi)
	defer s.Close()
	for s.Scan() {
		fmt.Println(string(s.Val()))
	}
	return s.Err()
}
