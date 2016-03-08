package lmdb

import (
	crand "crypto/rand"
	"encoding/binary"
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

type BenchOpt struct {
	RandSeed int64
	NumEntry uint64
	MaxVal   uint64
	MaxKey   uint64
	EnvFlags uint
	DBIFlags uint
	Put      func(*Txn, DBI, uint64, uint64) error
	Get      func(*Txn, DBI, uint64) ([]byte, error)
}

func (opt *BenchOpt) randSeed() int64 {
	if opt.RandSeed == 0 {
		return 0xDEADC0DE
	}
	return opt.RandSeed
}

func (opt *BenchOpt) maxkey() uint64 {
	if opt.MaxKey == 0 {
		return 10000
	}
	return opt.MaxKey
}

func (opt *BenchOpt) maxval() uint64 {
	if opt.MaxVal == 0 {
		return 1000
	}
	return opt.MaxVal
}

func (opt *BenchOpt) numentry() uint64 {
	if opt.NumEntry == 0 {
		return 2 * opt.maxkey()
	}
	return opt.NumEntry
}

func (opt *BenchOpt) SeedRand(r *rand.Rand) {
	if r != nil {
		r.Seed(opt.randSeed())
		return
	}
	rand.Seed(opt.randSeed())
}

// arguments to put are guaranteed to be less then math.MaxUint32
func benchTxnPutUint64(b *testing.B, opt *BenchOpt) {
	env := setupFlags(b, NoSync)
	defer clean(env, b)

	r := rand.New(rand.NewSource(opt.randSeed()))
	opt.SeedRand(r)

	dbi, err := loadUint64(env, r, opt)
	if err != nil {
		b.Error(err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		defer b.StopTimer()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := uint64(int(r.Intn(int(opt.maxkey()))))
			v := uint64(int(r.Intn(int(opt.maxval()))))
			err = opt.Put(txn, dbi, k, v)
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

func benchTxnGetUint64(b *testing.B, opt *BenchOpt) {
	env := setupFlags(b, NoSync)
	defer clean(env, b)

	r := rand.New(rand.NewSource(opt.randSeed()))
	opt.SeedRand(r)

	dbi, err := loadUint64(env, r, opt)
	if err != nil {
		b.Error(err)
		return
	}

	err = env.View(func(txn *Txn) (err error) {
		defer b.StopTimer()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := uint64(r.Intn(int(opt.maxkey())))
			_, err = opt.Get(txn, dbi, k)
			if err != nil && !IsNotFound(err) {
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

func loadUint64(env *Env, r *rand.Rand, opt *BenchOpt) (DBI, error) {
	err := env.SetMapSize(100 << 20)
	if err != nil {
		return 0, err
	}

	var dbi DBI
	err = env.Update(func(txn *Txn) (err error) {
		dbi, err = txn.OpenDBI("benchmark", opt.DBIFlags|Create)
		return err
	})
	if err != nil {
		return 0, err
	}

	err = env.Update(func(txn *Txn) (err error) {
		n := int(opt.numentry())
		for i := 0; i < n; i++ {
			k := uint64(r.Intn(int(opt.maxkey())))
			v := uint64(r.Intn(int(opt.maxval())))
			err = opt.Put(txn, dbi, k, v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return dbi, nil
}

func BenchmarkTxn_GetValue_U_(b *testing.B) {
	type UintValue interface {
		Value
		SetUint(uint)
	}
	key := Uint(0).(UintValue)
	val := Uint(0).(UintValue)
	benchTxnGetUint64(b, &BenchOpt{
		DBIFlags: IntegerKey,
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			key.SetUint(uint(k))
			val.SetUint(uint(v))
			return txn.PutValue(dbi, key, val, 0)
		},
		Get: func(txn *Txn, dbi DBI, k uint64) ([]byte, error) {
			key.SetUint(uint(k))
			return txn.GetValue(dbi, key)
		},
	})
}

func BenchmarkTxn_GetValue_Z_(b *testing.B) {
	type UintptrValue interface {
		Value
		SetUintptr(uintptr)
	}
	key := Uintptr(0).(UintptrValue)
	val := Uintptr(0).(UintptrValue)
	benchTxnGetUint64(b, &BenchOpt{
		DBIFlags: IntegerKey,
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			key.SetUintptr(uintptr(k))
			val.SetUintptr(uintptr(v))
			return txn.PutValue(dbi, key, val, 0)
		},
		Get: func(txn *Txn, dbi DBI, k uint64) ([]byte, error) {
			key.SetUintptr(uintptr(k))
			return txn.GetValue(dbi, key)
		},
	})
}

func BenchmarkTxn_GetValue_B_(b *testing.B) {
	key := make([]byte, 8)
	val := make([]byte, 8)
	benchTxnGetUint64(b, &BenchOpt{
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			binary.BigEndian.PutUint64(key, k)
			binary.BigEndian.PutUint64(val, v)
			return txn.Put(dbi, key, val, 0)
		},
		Get: func(txn *Txn, dbi DBI, k uint64) ([]byte, error) {
			binary.BigEndian.PutUint64(key, k)
			return txn.Get(dbi, key)
		},
	})
}

func BenchmarkTxn_PutValue_u_(b *testing.B) {
	benchTxnPutUint64(b, &BenchOpt{
		DBIFlags: IntegerKey,
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			return txn.PutValue(dbi, Uint(uint(k)), Uint(uint(v)), 0)
		},
	})
}

func BenchmarkTxn_PutValue_z_(b *testing.B) {
	benchTxnPutUint64(b, &BenchOpt{
		DBIFlags: IntegerKey,
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			return txn.PutValue(dbi, Uintptr(uintptr(k)), Uintptr(uintptr(v)), 0)
		},
	})
}

func BenchmarkTxn_PutValue_b_(b *testing.B) {
	benchTxnPutUint64(b, &BenchOpt{
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			key := make([]byte, 8)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(key, k)
			binary.BigEndian.PutUint64(val, v)
			return txn.Put(dbi, key, val, 0)
		},
	})
}

func BenchmarkTxn_PutValue_U_(b *testing.B) {
	type UintValue interface {
		Value
		SetUint(uint)
	}
	key := Uint(0).(UintValue)
	val := Uint(0).(UintValue)
	benchTxnPutUint64(b, &BenchOpt{
		DBIFlags: IntegerKey,
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			key.SetUint(uint(k))
			val.SetUint(uint(v))
			return txn.PutValue(dbi, key, val, 0)
		},
	})
}

func BenchmarkTxn_PutValue_Z_(b *testing.B) {
	type UintptrValue interface {
		Value
		SetUintptr(uintptr)
	}
	key := Uintptr(0).(UintptrValue)
	val := Uintptr(0).(UintptrValue)
	benchTxnPutUint64(b, &BenchOpt{
		DBIFlags: IntegerKey,
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			key.SetUintptr(uintptr(k))
			val.SetUintptr(uintptr(v))
			return txn.PutValue(dbi, key, val, 0)
		},
	})
}

func BenchmarkTxn_PutValue_B_(b *testing.B) {
	key := make([]byte, 8)
	val := make([]byte, 8)
	benchTxnPutUint64(b, &BenchOpt{
		Put: func(txn *Txn, dbi DBI, k uint64, v uint64) error {
			binary.BigEndian.PutUint64(key, k)
			binary.BigEndian.PutUint64(val, v)
			return txn.Put(dbi, key, val, 0)
		},
	})
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

// repeatedly put (overwrite) keys.
func BenchmarkTxn_PutValue(b *testing.B) {
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
			err := txn.PutValue(dbi, Bytes(k), Bytes(v), 0)
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
