package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/bmatsuo/lmdb-go/exp/lmdbscan"
	"github.com/bmatsuo/lmdb-go/lmdb"
)

func main() {
	flag.Parse()
	dbs := flag.Args()
	var specs []*catSpec
	for _, db := range dbs {
		spec, err := parseCatSpec(db)
		if err != nil {
			log.Fatal(err)
		}
		specs = append(specs, spec)
	}
	for _, spec := range specs {
		opt := &Options{DB: spec.DB}
		cat(spec.Path, opt)
	}
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

type Options struct {
	DB []string
}

func cat(path string, opt *Options) error {
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
	err = env.Open(path, 0, 644)
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
