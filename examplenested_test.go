package lmdb_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"text/tabwriter"

	"github.com/bmatsuo/lmdb.exp"
)

// This complete example demonstrates use of nested transactions.  Parent
// transactions must not be used while children are alive.  Helper functions
// are used in this example to shadow parent transactions and prevent such
// invalid use.
func Example_nested() {
	// Open an environment.
	env, err := lmdb.NewEnv()
	if err != nil {
		log.Panic(err)
	}
	path, err := ioutil.TempDir("", "mdb_test")
	if err != nil {
		log.Panic(err)
	}
	defer os.RemoveAll(path)
	err = env.SetMaxDBs(2)
	if err != nil {
		log.Panic(err)
	}
	err = env.Open(path, 0, 0644)
	defer env.Close()
	if err != nil {
		log.Panic(err)
	}

	// Create a transaction to serve as the root of all other changes.
	txnroot, err := env.BeginUpdate()
	if err != nil {
		panic(err)
	}

	// Create employee and department databases.  These operations must succeed
	// for the transaction to be committed.
	var empldb, deptdb lmdb.DBI
	err = txnroot.Do(AllOps(
		CreateDB("employees", &empldb),
		CreateDB("departments", &deptdb),
	))
	if err != nil {
		panic(err)
	}

	// For the rest of the example the subtransactions prevent the database
	// from entering an inconsistent state and we can always commit.
	defer func() {
		if err := txnroot.Commit(); err != nil {
			panic(err)
		}
	}()

	// populate the department database.
	depts := []*Dept{
		{ID: "eng", Name: "engineering"},
		{ID: "mkt", Name: "marketing"},
	}
	err = txnroot.Do(lmdb.SubTxn(func(txn *lmdb.Txn) error {
		for _, dept := range depts {
			p, err := json.Marshal(dept)
			if err != nil {
				return fmt.Errorf("json: %v", err)
			}
			err = txn.Put(deptdb, []byte(dept.ID), p, 0)
			if err != nil {
				return fmt.Errorf("put: %v", err)
			}
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	// populate the employee database
	empls := []*Employee{
		{ID: "e1341", DeptID: "eng"},
		{ID: "e3251", DeptID: "mkt"},
		{ID: "e7371", DeptID: "mkt"},
	}
	err = txnroot.Do(lmdb.SubTxn(func(txn *lmdb.Txn) error {
		for _, empl := range empls {
			p, err := json.Marshal(empl)
			if err != nil {
				return fmt.Errorf("json: %v", err)
			}
			err = txn.Put(empldb, []byte(empl.ID), p, 0)
			if err != nil {
				return fmt.Errorf("put: %v", err)
			}
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}

	// delete the marketing department and its employees.
	if err = txnroot.Do(lmdb.SubTxn(DeleteDept(empldb, deptdb, "mkt"))); err != nil {
		panic(err)
	}

	// dump the databases to stdout.
	err = txnroot.Do(lmdb.SubTxn(AllOps(
		DumpDB(deptdb, "deptdb"),
		DumpDB(empldb, "empldb"),
	)))
	if err != nil {
		panic(err)
	}

	// Output:
	// deptdb
	// eng     {"Name":"engineering"}
	// empldb
	// e1341   {"DeptID":"eng"}
}

type Employee struct {
	ID     string `json:"-"`
	DeptID string
}

type Dept struct {
	ID   string `json:"-"`
	Name string
}

// CreateDB prepares to creates name database in a transaction and assign its
// index to dbi so it may be reused.
func CreateDB(name string, db *lmdb.DBI) lmdb.TxnOp {
	return func(txn *lmdb.Txn) (err error) {
		*db, err = txn.OpenDBI(name, lmdb.Create)
		return
	}
}

// DeleteDept prepares to delete a department and all its employees.
func DeleteDept(empldb, deptdb lmdb.DBI, deptid string) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		err := txn.Del(deptdb, []byte(deptid), nil)
		if err != nil {
			return fmt.Errorf("dept: %v", err)
		}
		err = txn.Sub(DeleteEmployeesByDept(empldb, deptid))
		if err != nil {
			return fmt.Errorf("empl: %v", err)
		}
		return nil
	}
}

// DeleteEmployeesByDept prepares to scan db and delete employee entries with
// DeptID equal to dept.
func DeleteEmployeesByDept(db lmdb.DBI, dept string) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		c, err := txn.OpenCursor(db)
		if err != nil {
			return fmt.Errorf("cursor: %v", err)
		}
		defer c.Close()

		for {
			_, v, err := c.Get(nil, nil, lmdb.Next)
			if err == lmdb.ErrNotFound {
				return nil
			}
			if err != nil {
				return fmt.Errorf("get: %v", err)
			}
			empl := new(Employee)
			err = json.Unmarshal(v, empl)
			if err != nil {
				return fmt.Errorf("json: %v", err)
			}
			if empl.DeptID == dept {
				err := c.Del(0)
				if err != nil {
					return fmt.Errorf("del: %v", err)
				}
			}
		}
	}
}

// DumpDB prepares to dump db to standard out.  Immediately prior to iterating
// the database name is printed.
func DumpDB(db lmdb.DBI, name string) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		fmt.Println(name)
		c, err := txn.OpenCursor(db)
		if err != nil {
			return fmt.Errorf("%s cursor: %v", name, err)
		}
		defer c.Close()

		tw := tabwriter.NewWriter(os.Stdout, 8, 2, 2, ' ', 0)
		defer tw.Flush()
		for {
			k, v, err := c.Get(nil, nil, lmdb.Next)
			if err == lmdb.ErrNotFound {
				return nil
			}
			if err != nil {
				return fmt.Errorf("%s next: %v", name, err)
			}
			fmt.Fprintf(tw, "%s\t%s\n", k, v)
		}
	}
}

// AllOps returns a TxnOp that executes each of fn in sequence and returns an
// error if an error is encountered.
func AllOps(fn ...lmdb.TxnOp) lmdb.TxnOp {
	return func(txn *lmdb.Txn) error {
		for _, fn := range fn {
			err := fn(txn)
			if err != nil {
				return err
			}
		}
		return nil
	}
}
