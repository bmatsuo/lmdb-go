package lmdb

/*
#include "lmdbgo.h"
*/
import "C"
import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// lmdbgoMDBMsgFuncBridge provides a static C function for handling MDB_msgfunc
// callbacks.  It performs string conversion and dynamic dispatch to a msgfunc
// provided to Env.ReaderList.  Any error returned by the msgfunc is cached and
// -1 is returned to terminate the iteration.

//export lmdbgoMDBMsgFuncBridge
func lmdbgoMDBMsgFuncBridge(cmsg C.lmdbgo_ConstCString, _ctx unsafe.Pointer) C.int {
	ctx := msgctx(_ctx)
	fn := ctx.fn()
	if fn == nil {
		return 0
	}
	msg := C.GoString(cmsg.p)
	err := fn(msg)
	if err != nil {
		ctx.seterr(err)
		return -1
	}
	return 0
}

type msgfunc func(string) error

// msgctx is the type used for context pointers passed to mdb_reader_list.  A
// msgctx stores its corresponding msgfunc, and any error encountered in an
// external map.  The corresponding function is called once for each
// mdb_reader_list entry using the msgctx.
//
// External maps are required because struct pointers passed to C functions
// must not contain pointers in their struct fields.  See the following
// language proposal which discusses the restrictions on passing pointers to C.
//
//		https://github.com/golang/proposal/blob/master/design/12416-cgo-pointers.md
//
// NOTE:
// The underlying type must have a non-zero size to ensure that the value
// returned by new(msgctx) does not conflict with other live *msgctx values.
type msgctx uintptr
type _msgctx struct {
	fn  msgfunc
	err error
}

var msgctxn uint32
var msgctxm = map[msgctx]*_msgctx{}
var msgctxmlock sync.RWMutex

func newMsgFunc(fn msgfunc) (ctx msgctx, done func()) {
	ctx = msgctx(atomic.AddUint32(&msgctxn, 1))
	ctx.register(fn)
	return ctx, ctx.deregister
}

func (ctx msgctx) register(fn msgfunc) {
	msgctxmlock.Lock()
	if _, ok := msgctxm[ctx]; ok {
		panic("msgfunc conflict")
	}
	msgctxm[ctx] = &_msgctx{fn: fn}
	msgctxmlock.Unlock()
}

func (ctx msgctx) deregister() {
	msgctxmlock.Lock()
	delete(msgctxm, ctx)
	msgctxmlock.Unlock()
}

func (ctx msgctx) get() *_msgctx {
	msgctxmlock.RLock()
	_ctx := msgctxm[ctx]
	msgctxmlock.RUnlock()
	return _ctx
}

func (ctx msgctx) fn() msgfunc {
	return ctx.get().fn
}

func (ctx msgctx) err() error {
	return ctx.get().err
}

func (ctx msgctx) seterr(err error) {
	// a read-lock is fine here because the map is not modified
	ctx.get().err = err
}
