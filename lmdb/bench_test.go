package lmdb

import (
	crand "crypto/rand"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
)

func BenchmarkEnv_ReaderList(b *testing.B) {
	env := setup(b)
	defer clean(env, b)

	var txns []*Txn
	defer func() {
		for i, txn := range txns {
			if txn != nil {
				txn.Abort()
				txns[i] = nil
			}
		}
	}()

	const numreaders = 100
	for i := 0; i < numreaders; i++ {
		txn, err := env.BeginTxn(nil, Readonly)
		if err != nil {
			b.Error(err)
			return
		}
		txns = append(txns, txn)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list := new(readerList)
		err := env.ReaderList(list.Next)
		if err != nil {
			b.Error(err)
			return
		}
		if list.Len() != numreaders+1 {
			b.Errorf("reader list length: %v", list.Len())
		}
	}
}

type readerList struct {
	ln []string
}

func (r *readerList) Len() int {
	return len(r.ln)
}

func (r *readerList) Next(ln string) error {
	r.ln = append(r.ln, ln)
	return nil
}

// repeatedly put (overwrite) keys.
func BenchmarkTxn_Put(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			err := txn.Put(dbi, k, v, 0)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly put (overwrite) keys using the PutReserve method.
func BenchmarkTxn_PutReserve(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			buf, err := txn.PutReserve(dbi, k, len(v), 0)
			if err != nil {
				return err
			}
			copy(buf, v)
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly put (overwrite) keys using the PutReserve method on an
// environment with WriteMap.
func BenchmarkTxn_PutReserve_writemap(b *testing.B) {
	initRandSource(b)
	env := setupFlags(b, WriteMap)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			buf, err := txn.PutReserve(dbi, k, len(v), 0)
			if err != nil {
				return err
			}
			copy(buf, v)
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly put (overwrite) keys.
func BenchmarkTxn_Put_writemap(b *testing.B) {
	initRandSource(b)
	env := setupFlags(b, WriteMap)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	var ps [][]byte

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.Update(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			k := ps[rand.Intn(len(ps)/2)*2]
			v := makeBenchDBVal(&rc)
			err := txn.Put(dbi, k, v, 0)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		b.Error(err)
	}
}

// repeatedly get random keys.
func BenchmarkTxn_Get_ro(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			_, err := txn.Get(dbi, ps[rand.Intn(len(ps))])
			if IsNotFound(err) {
				continue
			}
			if err != nil {
				b.Fatalf("error getting data: %v", err)
			}
		}

		return nil
	})
	if err != nil {
		b.Error(err)
	}
}

// like BenchmarkTxnGetReadonly but txn.RawRead is set to true.
func BenchmarkTxn_Get_raw_ro(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	ps, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		txn.RawRead = true
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			_, err := txn.Get(dbi, ps[rand.Intn(len(ps))])
			if IsNotFound(err) {
				continue
			}
			if err != nil {
				b.Fatalf("error getting data: %v", err)
			}
		}
		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// repeatedly scan all the values in a database.
func BenchmarkScan_ro(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	_, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkScanDBI(txn, dbi)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		b.Error(err)
		return
	}
}

// like BenchmarkCursoreScanReadonly but txn.RawRead is set to true.
func BenchmarkScan_raw_ro(b *testing.B) {
	initRandSource(b)
	env := setup(b)
	defer clean(env, b)

	dbi := openBenchDBI(b, env)

	rc := newRandSourceCursor()
	_, err := populateBenchmarkDB(env, dbi, &rc)
	if err != nil {
		b.Errorf("populate db: %v", err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		txn.RawRead = true

		b.ResetTimer()
		defer b.StopTimer()
		for i := 0; i < b.N; i++ {
			err := benchmarkScanDBI(txn, dbi)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		b.Errorf("benchmark: %v", err)
		return
	}
}

// populateBenchmarkDB fills env with data.
//
// populateBenchmarkDB calls env.SetMapSize and must not be called concurrent
// with other transactions.
func populateBenchmarkDB(env *Env, dbi DBI, rc *randSourceCursor) ([][]byte, error) {
	var ps [][]byte

	err := env.SetMapSize(benchDBMapSize)
	if err != nil {
		return nil, err
	}

	err = env.Update(func(txn *Txn) (err error) {
		for i := 0; i < benchDBNumKeys; i++ {
			k := makeBenchDBKey(rc)
			v := makeBenchDBVal(rc)
			err := txn.Put(dbi, k, v, 0)
			ps = append(ps, k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ps, nil
}

func benchmarkScanDBI(txn *Txn, dbi DBI) error {
	cur, err := txn.OpenCursor(dbi)
	if err != nil {
		return err
	}
	defer cur.Close()

	var count int64
	for {
		_, _, err := cur.Get(nil, nil, Next)
		if IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		count++
	}
}

func openBenchDBI(b *testing.B, env *Env) DBI {
	var dbi DBI
	err := env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenDBI("benchmark", Create)
		return err
	})
	if err != nil {
		b.Errorf("unable to open benchmark database")
	}
	return dbi
}

func teardownBenchDB(b *testing.B, env *Env, path string) {
	env.Close()
	os.RemoveAll(path)
}

func randBytes(n int) []byte {
	p := make([]byte, n)
	crand.Read(p)
	return p
}

func bMust(b *testing.B, err error, action string) {
	if err != nil {
		b.Fatalf("error %s: %v", action, err)
	}
}

const randSourceSize = 10 << 20  // size of the 'entropy pool' for random byte generation.
const benchDBMapSize = 100 << 20 // size of a benchmark db memory map
const benchDBNumKeys = 1 << 12   // number of keys to store in benchmark databases
const benchDBMaxKeyLen = 30      // maximum length for database keys (size is limited by MDB)
const benchDBMaxValLen = 4096    // maximum lengh for database values

func makeBenchDBKey(c *randSourceCursor) []byte {
	return c.NBytes(rand.Intn(benchDBMaxKeyLen) + 1)
}

func makeBenchDBVal(c *randSourceCursor) []byte {
	return c.NBytes(rand.Intn(benchDBMaxValLen) + 1)
}

// holds a bunch of random bytes so repeated generation of 'random' slices is
// cheap.  acts as a ring which can be read from (although doesn't implement io.Reader).
var _initRand int32
var randSource [randSourceSize]byte

func initRandSource(b *testing.B) {
	if atomic.AddInt32(&_initRand, 1) > 1 {
		return
	}
	b.Logf("initializing random source data")
	n, err := crand.Read(randSource[:])
	bMust(b, err, "initializing random source")
	if n < len(randSource) {
		b.Fatalf("unable to read enough random source data %d", n)
	}
}

// acts as a simple byte slice generator.
type randSourceCursor int

func newRandSourceCursor() randSourceCursor {
	i := rand.Intn(randSourceSize)
	return randSourceCursor(i)
}

func (c *randSourceCursor) NBytes(n int) []byte {
	i := int(*c)
	if n >= randSourceSize {
		panic("rand size too big")
	}
	*c = (*c + randSourceCursor(n)) % randSourceSize
	_n := i + n - randSourceSize
	if _n > 0 {
		p := make([]byte, n)
		m := copy(p, randSource[i:])
		copy(p[m:], randSource[:])
		return p
	}
	return randSource[i : i+n]
}
