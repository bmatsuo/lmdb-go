package lmdb

/*
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"unsafe"
)

// success is a value returned from the LMDB API to indicate a successful call.
// The functions in this API this behavior and its use is not required.
const success = C.MDB_SUCCESS

// Env flags.
//
// See mdb_env_open
const (
	FixedMap    = C.MDB_FIXEDMAP   // Danger zone. Map memory at a fixed address.
	NoSubdir    = C.MDB_NOSUBDIR   // Argument to Open is a file, not a directory.
	Readonly    = C.MDB_RDONLY     // Used in several functions to denote an object as readonly.
	WriteMap    = C.MDB_WRITEMAP   // Use a writable memory map
	NoMetaSync  = C.MDB_NOMETASYNC // Don't fsync metapage after commit
	NoSync      = C.MDB_NOSYNC     // Don't fsync after commit
	MapAsync    = C.MDB_MAPASYNC   // Flush asynchronously when using the WriteMap flag.
	NoTLS       = C.MDB_NOTLS      // Danger zone. Tie reader locktable slots to Txn objects instead of threads.
	NoLock      = C.MDB_NOLOCK     // Danger zone. LMDB does not use any locks. All transactions must serialize.
	NoReadahead = C.MDB_NORDAHEAD  // Disable readahead. Requires OS support.
	NoMemInit   = C.MDB_NOMEMINIT  // Disable LMDB memory initialization.
)

// DBI is a handle for a database in an Env.
//
// See MDB_dbi
type DBI C.MDB_dbi

// Env is opaque structure for a database environment.  A DB environment
// supports multiple databases, all residing in the same shared-memory map.
//
// See MDB_env.
type Env struct {
	_env *C.MDB_env
}

// NewEnv allocates and initialized an new Env.
//
// See mdb_env_create.
func NewEnv() (*Env, error) {
	var _env *C.MDB_env
	ret := C.mdb_env_create(&_env)
	if ret != success {
		return nil, errno(ret)
	}
	return &Env{_env}, nil
}

// Open an environment handle. If this function fails Close() must be called to
// discard the Env handle.  Open passes flags|NoTLS to mdb_env_open.
//
// See mdb_env_open.
func (env *Env) Open(path string, flags uint, mode os.FileMode) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_open(env._env, cpath, C.uint(NoTLS|flags), C.mdb_mode_t(mode))
	return errno(ret)
}

// Close shuts down the environment and releases the memory map.
//
// See mdb_env_close.
func (env *Env) Close() error {
	if env._env == nil {
		return errors.New("Environment already closed")
	}
	C.mdb_env_close(env._env)
	env._env = nil
	return nil
}

// Copy copies the data in env to an environment at path.
//
// See mdb_env_copy.
func (env *Env) Copy(path string) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_copy(env._env, cpath)
	return errno(ret)
}

// CopyFlag copies the data in env to an environment at path created with flags.
//
// See mdb_env_copy2.
func (env *Env) CopyFlag(path string, flags uint) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_copy2(env._env, cpath, C.uint(flags))
	return errno(ret)
}

// Statistics for a database in the environment
//
// See MDB_stat.
type Stat struct {
	PSize         uint   // Size of a database page. This is currently the same for all databases.
	Depth         uint   // Depth (height) of the B-tree
	BranchPages   uint64 // Number of internal (non-leaf) pages
	LeafPages     uint64 // Number of leaf pages
	OverflowPages uint64 // Number of overflow pages
	Entries       uint64 // Number of data items
}

// Stat returns statistics about the environment.
//
// See mdb_env_stat.
func (env *Env) Stat() (*Stat, error) {
	var _stat C.MDB_stat
	ret := C.mdb_env_stat(env._env, &_stat)
	if ret != success {
		return nil, errno(ret)
	}
	stat := Stat{PSize: uint(_stat.ms_psize),
		Depth:         uint(_stat.ms_depth),
		BranchPages:   uint64(_stat.ms_branch_pages),
		LeafPages:     uint64(_stat.ms_leaf_pages),
		OverflowPages: uint64(_stat.ms_overflow_pages),
		Entries:       uint64(_stat.ms_entries)}
	return &stat, nil
}

// Information about the environment.
//
// See MDB_envinfo.
type EnvInfo struct {
	MapSize    int64 // Size of the data memory map
	LastPNO    int64 // ID of the last used page
	LastTxnID  int64 // ID of the last committed transaction
	MaxReaders uint  // maximum number of threads for the environment
	NumReaders uint  // maximum number of threads used in the environment
}

// Info returns information about the environment.
//
// See mdb_env_info.
func (env *Env) Info() (*EnvInfo, error) {
	var _info C.MDB_envinfo
	ret := C.mdb_env_info(env._env, &_info)
	if ret != success {
		return nil, errno(ret)
	}
	info := EnvInfo{
		MapSize:    int64(_info.me_mapsize),
		LastPNO:    int64(_info.me_last_pgno),
		LastTxnID:  int64(_info.me_last_txnid),
		MaxReaders: uint(_info.me_maxreaders),
		NumReaders: uint(_info.me_numreaders),
	}
	return &info, nil
}

// Sync flushes buffers to disk.
//
// See mdb_env_sync.
func (env *Env) Sync(force bool) error {
	ret := C.mdb_env_sync(env._env, cbool(force))
	return errno(ret)
}

// SetFlags enables/disables flags in the environment.
//
// See mdb_env_set_flags.
func (env *Env) SetFlags(flags uint, onoff int) error {
	ret := C.mdb_env_set_flags(env._env, C.uint(flags), C.int(onoff))
	return errno(ret)
}

// Flags returns the flags set in the environment.
//
// See mdb_env_get_flags.
func (env *Env) Flags() (uint, error) {
	var _flags C.uint
	ret := C.mdb_env_get_flags(env._env, &_flags)
	if ret != success {
		return 0, errno(ret)
	}
	return uint(_flags), nil
}

// Path returns the path argument passed to Open.  Path returns an error if the
// Env was not opened previously.  Calling Path on a closed Env has undefined
// results.
//
// See mdb_env_path.
func (env *Env) Path() (string, error) {
	var path string
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.mdb_env_get_path(env._env, &cpath)
	if ret != success {
		return "", errno(ret)
	}
	if cpath == nil {
		return "", fmt.Errorf("not open")
	}
	return C.GoString(cpath), nil
}

// SetMapSize sets the size of the environment memory map.
//
// See mdb_env_set_map_size.
func (env *Env) SetMapSize(size int64) error {
	if size < 0 {
		return fmt.Errorf("negative size")
	}
	ret := C.mdb_env_set_mapsize(env._env, C.size_t(size))
	return errno(ret)
}

// SetMaxReaders sets the maximum number of reader slots in the environment.
//
// See mdb_env_set_max_readers.
func (env *Env) SetMaxReaders(size int) error {
	if size < 0 {
		return fmt.Errorf("negative size")
	}
	ret := C.mdb_env_set_maxreaders(env._env, C.uint(size))
	return errno(ret)
}

// SetMaxDBs sets the maximum number of named databases for the environment.
//
// See mdb_env_set_maxdbs.
func (env *Env) SetMaxDBs(size int) error {
	if size < 0 {
		return fmt.Errorf("negative size")
	}
	ret := C.mdb_env_set_maxdbs(env._env, C.MDB_dbi(size))
	return errno(ret)
}

func (env *Env) beginTxn(parent *Txn, flags uint) (*Txn, error) {
	return beginTxn(env, parent, flags)
}

// Begin is a low-level (potentially dangerous) method to initialize a new
// transaction on env.  BeginTxn does not attempt to serialize operations on
// write transactions to the same OS thread and its use, without care, can
// cause undefined results.
//
// Instead of Begin users should call the View, Update, BeginView, BeginUpdate
// or RunTxn methods.
//
// See mdb_txn_begin.
func (env *Env) Begin(parent *Txn, flags uint) (*Txn, error) {
	txn, err := beginTxn(env, parent, flags)
	return txn, err
}

// BeginView starts a readonly transaction on env.  BeginRead implies the
// Readonly flag and its present is not required in the flags argument.
//
// See mdb_txn_begin.
func (env *Env) BeginView() (*Txn, error) {
	return env.beginViewFlag(0)
}

// BeginViewFlag is like BeginView but allows transaction flags to be
// specified.
//
// This method is not exported because at the moment (0.9.14) Readonly is the
// only flag and that is implied here.
func (env *Env) beginViewFlag(flags uint) (*Txn, error) {
	return beginTxn(env, nil, flags|Readonly)
}

// BeginUpdate starts a write transaction on env.  BeginUpdate returns an
// Update instead of a *Txn Because LMDB write transactions must serialize all
// actions on the same OS thread.
//
// See mdb_txn_begin.
func (env *Env) BeginUpdate() (*WriteTxn, error) {
	return env.beginUpdateFlag(0)
}

// beginUpdateFlag takes txn flags.
//
// this method is not exported because the only flag at the moment (0.9.14) is
// Readonly and that makes no sense here.
func (env *Env) beginUpdateFlag(flags uint) (*WriteTxn, error) {
	return beginWriteTxn(env, flags)
}

// Run creates a new Txn and calls fn with it as an argument.  Run commits the
// transaction if fn returns nil otherwise the transaction is aborted.
//
// Because Run terminates the transaction goroutines should not retain
// references to it after fn returns.  Writable transactions (without the
// Readonly flag) must not be used from any goroutines other than the one
// running fn.
//
// See mdb_txn_begin.
func (env *Env) RunTxn(flags uint, fn TxnOp) error {
	if isReadonly(flags) {
		return env.runReadonly(flags, fn)
	}
	return env.runUpdate(flags, fn)
}

// View creates a readonly transaction with a consistent view of the
// environment and passes it to fn.  View terminates its transaction after fn
// returns.  Any error encountered by View is returned.
func (env *Env) View(fn TxnOp) error {
	return env.RunTxn(Readonly, fn)
}

// Update creates a writable transaction and passes it to fn.  Update commits
// the transaction if fn returns without error otherwise Update aborts the
// transaction and returns the error.
//
// The Txn passed to fn must not be used from multiple goroutines, even with
// synchronization.
func (env *Env) Update(fn TxnOp) error {
	return env.RunTxn(0, fn)
}

func (env *Env) runReadonly(flags uint, fn TxnOp) error {
	txn, err := beginTxn(env, nil, flags)
	if err != nil {
		return err
	}
	defer txn.Abort()
	err = fn(txn)
	return err
}

func (env *Env) runUpdate(flags uint, fn TxnOp) error {
	errc := make(chan error)
	go func() {
		defer close(errc)
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		txn, err := beginTxn(env, nil, flags)
		if err != nil {
			errc <- err
			return
		}
		err = fn(txn)
		if err != nil {
			txn.Abort()
			errc <- err
			return
		}
		errc <- txn.Commit()
	}()
	return <-errc
}

// CloseDBI closes the database handle, db.  Normally calling CloseDBI
// explicitly is not necessary.
//
// It is the caller's responsibility to serialize calls to CloseDBI.
//
// See mdb_dbi_close.
func (env *Env) CloseDBI(db DBI) {
	C.mdb_dbi_close(env._env, C.MDB_dbi(db))
}

func isReadonly(flags uint) bool {
	return flags&Readonly != 0
}
