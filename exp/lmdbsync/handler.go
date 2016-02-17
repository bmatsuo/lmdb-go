package lmdbsync

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"golang.org/x/net/context"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Handler can intercept errors returned by a transaction and handle them in an
// application-specific way, including by resizing the environment and retrying
// the transaction by returning ErrTxnRetry.
type Handler interface {
	HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error)
}

// HandlerChain is a Handler implementation that iteratively calls each handler
// in the underlying slice when handling an error.
type HandlerChain []Handler

// HandleTxnErr implements the Handler interface.  Each handler in c processes
// the context.Context and error returned by the previous handler.
func (c HandlerChain) HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error) {
	for _, h := range c {
		ctx, err = h.HandleTxnErr(ctx, env, err)
	}
	return ctx, err
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

// MapResizedHandler returns a Handler that transparently adopts map sizes set
// by external processes and retries any transactions that failed to start
// because of lmdb.MapResized.
//
// If the database is growing too rapidly and maxRetry consecutive transactions
// fail due to lmdb.MapResized then the Handler returned by MapResizedHandler
// gives up and returns the lmdb.MapResized error to the caller.  Delay will be
// called before each call to Env.SetMapSize to insert an optional delay.
//
// Open transactions must not directly create new (non-child) transactions when
// using MapResizedHandler or the environment will deadlock.
func MapResizedHandler(maxRetry int, delay DelayFunc) Handler {
	if maxRetry == 0 {
		maxRetry = MapResizedDefaultRetry
	}
	if delay == nil {
		delay = MapResizedDefaultDelay
	}
	return &resizedHandler{
		MaxRetry: maxRetry,
		Delay:    delay,
	}
}

// MapResizedDefaultRetry is the default number of attempts MapResizedHandler
// will make adopt a new map size when lmdb.MapResized is encountered
// repeatedly.
var MapResizedDefaultRetry = 2

// DelayFunc takes as input the number of previous attempts and returns the
// delay before making another attempt.
type DelayFunc func(attempt int) time.Duration

// ExponentialBackoff returns a function that delays each attempt by random
// number between 0 and the minimum of max and base*factor^attempt.
func ExponentialBackoff(base time.Duration, max time.Duration, factor float64) DelayFunc {
	return func(attempt int) time.Duration {
		_max := float64(base) * math.Pow(factor, float64(attempt))
		_max = math.Min(_max, float64(max))
		n := rand.Int63n(int64(_max))
		return time.Duration(n)
	}
}

// MapResizedDefaultDelay is the default DelayFunc when MapResizedHandler is
// passed a nil value.
var MapResizedDefaultDelay = ExponentialBackoff(time.Millisecond, 5*time.Millisecond, 2)

// MapFullFunc is a function for resizing a memory map after it has become
// full.  The function receives the current map size as its argument and
// returns a new map size.  The new size will only be applied if the second
// return value is true.
type MapFullFunc func(size int64) (int64, bool)

// MapFullHandler returns a Handler that retries updates which failed to commit
// due to lmdb.MapFull errors.  When lmdb.MapFull is encountered fn is used to
// set a new new map size before opening a new transaction and executing the
// lmdb.TxnOp again.
//
// When using MapFullHandler it is important that updates are idempotent.  An
// Env.Update that encounters lmdb.MapFull may execute its lmdb.TxnOp function
// multiple times before successfully committing it (or aborting).
//
// Open view transactions must not wait for updates to complete when using
// MapFullHandler or the environment will deadlock.
func MapFullHandler(fn MapFullFunc) Handler {
	return &mapFullHandler{fn}
}

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

func (h *mapFullHandler) HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error) {
	if !lmdb.IsMapFull(err) {
		return ctx, err
	}

	newsize, ok := h.getNewSize(env)
	if !ok {
		return ctx, err
	}
	if env.setMapSize(newsize, 0) != nil {
		return ctx, err
	}

	return ctx, ErrTxnRetry
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

type resizedHandlerKey int

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

func getResizedRetryCount(ctx context.Context) *resizeRetryCount {
	v, _ := ctx.Value(resizedHandlerKey(0)).(*resizeRetryCount)
	return v
}

func withResizedRetryCount(ctx context.Context, count *resizeRetryCount) context.Context {
	return context.WithValue(ctx, resizedHandlerKey(0), count)
}

type resizedHandler struct {
	MaxRetry int
	Delay    DelayFunc
}

func (h *resizedHandler) HandleTxnErr(ctx context.Context, env *Env, err error) (context.Context, error) {
	if !lmdb.IsMapResized(err) {
		ctx := context.WithValue(ctx, resizedHandlerKey(0), nil)
		return ctx, err
	}

	count := getResizedRetryCount(ctx)
	numRetry := count.Get()

	// fail the transaction with MapResized error when too many attempts have
	// been made.
	maxRetry := h.MaxRetry
	if maxRetry > 0 && numRetry >= maxRetry {
		ctx := withResizedRetryCount(ctx, nil)
		return ctx, err
	}

	ctx = withResizedRetryCount(ctx, count.Add(1))

	delay := h.Delay(numRetry)
	err = env.setMapSize(0, delay)
	if err != nil {
		return ctx, err
	}
	return ctx, ErrTxnRetry
}
