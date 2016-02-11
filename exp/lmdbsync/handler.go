package lmdbsync

import (
	"errors"
	"time"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Handler can intercept errors returned by a transaction and handle them in an
// application-specific way, including by resizing the environment and retrying
// the transaction by returning ErrTxnRetry.
type Handler interface {
	HandleTxnErr(c Bag, err error) (Bag, error)
}

// HandlerChain is a Handler implementation that iteratively calls each handler
// in the underlying slice when handling an error.
type HandlerChain []Handler

// HandleTxnErr implements the Handler interface.  Each handler in c processes
// the Bag and error returned by the previous handler.
func (c HandlerChain) HandleTxnErr(b Bag, err error) (Bag, error) {
	for _, h := range c {
		b, err = h.HandleTxnErr(b, err)
	}
	return b, err
}

// Append returns a new HandlerChain that will evaluate h in sequence after the
// Handlers already in C are evaluated.  Append does not modify the storage
// undelying c.
func (c HandlerChain) Append(h ...Handler) HandlerChain {
	_c := make(HandlerChain, len(c)+len(h))
	copy(_c, c)
	copy(_c[len(c):], h)
	return _c
}

// MapResizedHandler returns a Handler that transparently retries Txns that
// failed to start due to MapResized errors.
//
// When MapResizeHandler is in use transactions must not be nested inside other
// transactions.  Adopting the new map size requires all transactions to
// terminate first.  If any transactions wait for other transactions to
// complete they may deadlock in the presence of a MapResized error.
//
// If maxRetry consecutive transactions fail due to MapResized then the Handler
// returned by MapResizedHandler will give up and the transaction will fail,
// returning MapResized to the caller.
func MapResizedHandler(maxRetry int, repeatDelay func(retry int) time.Duration) Handler {
	return &resizedHandler{
		RetryResize:       maxRetry,
		DelayRepeatResize: repeatDelay,
	}
}

// MapFullFunc is a function for resizing a memory map after it has become
// full.  The function receives the current map size as its argument and
// returns a new map size.  The new size will only be applied if the second
// return value is true.
type MapFullFunc func(size int64) (int64, bool)

// MapFullHandler returns a Handler that retries Txns that failed due to
// MapFull errors by increasing the environment map size according to fn.
//
// When using MapFullHandler it is important that transactions are idempotent.
// A lmdb.TxnOp func which returns MapFull will execute multiple times due to
// MapFullHandler.
//
// When MapFullHandler is in use update transactions must not be nested inside
// view transactions.  Resizing the database requires all transactions to
// terminate first.  If any transactions wait for update transactions to
// complete they may deadlock in the presence of a MapFull error.
func MapFullHandler(fn MapFullFunc) Handler {
	return &mapFullHandler{fn}
}

// DefaultRetryResize is the default number of times to retry a transaction
// that is returning repeatedly MapResized. This signifies rapid database
// growth from another process or some bug/corruption in memory.
//
// If DefaultRetryResize is less than zero the transaction will be retried
// indefinitely.
var DefaultRetryResize = 2

// DefaultDelayRepeatResize is the maximum number of consecutive MapResize will
// be handled by MapResizedHandler before it stops attempting to handle it and
// returns MapResize to the caller.
var DefaultDelayRepeatResize = time.Millisecond

// ErrTxnRetry is returned by a Handler to have the Env retry the transaction.
var ErrTxnRetry = errors.New("lmdbsync: retry failed txn")

// TxnRunner is an interface for types that can run lmdb transactions.
// TxnRunner is satisfied by Env.
type TxnRunner interface {
	RunTxn(flags uint, op lmdb.TxnOp) error
	View(op lmdb.TxnOp) error
	Update(op lmdb.TxnOp) error
	UpdateLocked(op lmdb.TxnOp) error
	WithHandler(h Handler) TxnRunner
}

type handlerRunner struct {
	env *Env
	h   Handler
}

func (r *handlerRunner) WithHandler(h Handler) TxnRunner {
	return &handlerRunner{
		env: r.env,
		h:   HandlerChain{r.h, h},
	}
}

func (r *handlerRunner) RunTxn(flags uint, op lmdb.TxnOp) error {
	readonly := flags&lmdb.Readonly != 0
	return r.env.runHandler(readonly, func() error { return r.env.RunTxn(flags, op) }, r.h)
}

func (r *handlerRunner) View(op lmdb.TxnOp) error {
	return r.env.runHandler(true, func() error { return r.env.View(op) }, r.h)
}

func (r *handlerRunner) Update(op lmdb.TxnOp) error {
	return r.env.runHandler(false, func() error { return r.env.Update(op) }, r.h)
}

func (r *handlerRunner) UpdateLocked(op lmdb.TxnOp) error {
	return r.env.runHandler(false, func() error { return r.env.UpdateLocked(op) }, r.h)
}

type mapFullHandler struct {
	fn MapFullFunc
}

func (h *mapFullHandler) HandleTxnErr(b Bag, err error) (Bag, error) {
	if !lmdb.IsMapFull(err) {
		return b, err
	}

	env := BagEnv(b)

	newsize, ok := h.getNewSize(env)
	if !ok {
		return b, err
	}
	if env.setMapSize(newsize, 0) != nil {
		return b, err
	}

	return b, ErrTxnRetry
}

func (h *mapFullHandler) getNewSize(env *Env) (int64, bool) {
	info, err := env.Info()
	if err != nil {
		return 0, false
	}
	newsize, ok := h.fn(info.MapSize)
	if !ok || newsize <= info.MapSize {
		return 0, false
	}
	return newsize, true
}

type resizedHandlerBagKey int

type resizeRetryCount struct {
	n int
}

func (r *resizeRetryCount) Get() int {
	if r == nil {
		return 0
	}
	return r.n
}

func (r *resizeRetryCount) Add(n int) *resizeRetryCount {
	if r == nil {
		return &resizeRetryCount{1}
	}
	return &resizeRetryCount{r.n + 1}
}

func bagResizedRetryCount(b Bag) *resizeRetryCount {
	v, _ := b.Value(resizedHandlerBagKey(0)).(*resizeRetryCount)
	return v
}

func bagWithResizedRetryCount(b Bag, count *resizeRetryCount) Bag {
	return BagWith(b, resizedHandlerBagKey(0), count)
}

type resizedHandler struct {
	// RetryResize overrides DefaultRetryResize for the Env.
	RetryResize int
	// DelayRepeateResize overrides DefaultDelayRetryResize for the Env.
	DelayRepeatResize func(retry int) time.Duration
}

func (h *resizedHandler) getRetryResize() int {
	if h.RetryResize != 0 {
		return h.RetryResize
	}
	return DefaultRetryResize
}

func (h *resizedHandler) getDelayRepeatResize(i int) time.Duration {
	if h.DelayRepeatResize != nil {
		return h.DelayRepeatResize(i)
	}
	return DefaultDelayRepeatResize
}

func (h *resizedHandler) HandleTxnErr(b Bag, err error) (Bag, error) {
	if !lmdb.IsMapResized(err) {
		b := BagWith(b, resizedHandlerBagKey(0), nil)
		return b, err
	}

	env := BagEnv(b)
	count := bagResizedRetryCount(b)
	numRetry := count.Get()

	// fail the transaction with MapResized error when too many attempts have
	// been made.
	maxRetry := h.getRetryResize()
	if maxRetry == 0 {
		b := bagWithResizedRetryCount(b, nil)
		return b, err
	}
	if maxRetry > 0 && numRetry >= maxRetry {
		b := bagWithResizedRetryCount(b, nil)
		return b, err
	}

	b = bagWithResizedRetryCount(b, count.Add(1))

	var delay time.Duration
	if numRetry > 0 {
		delay = h.getDelayRepeatResize(numRetry)
	}

	err = env.setMapSize(0, delay)
	if err != nil {
		return b, err
	}
	return b, ErrTxnRetry
}
