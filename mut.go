package lmdb

import "fmt"

var errInvalidCursor = fmt.Errorf("invalid cursor")

type txnMutex struct {
	t       *unsafeTxn
	cs      map[*unsafeCursor]bool
	_closed chan struct{}
	topenc  chan *topenc
	topend  chan *topend
	tgetval chan *tgetval
	cgetval chan *cgetval
	tput    chan *tput
	cput    chan *cput
	treset  chan *treset
	trenew  chan *trenew
	crenew  chan *crenew
	tdel    chan *tdel
	cdel    chan *cdel
	ccount  chan *ccount
	cclose  chan *cclose
	tabort  chan *tabort
	tcommit chan *tcommit
	tdrop   chan *tdrop
}

// newTxnMutex creates a mutex to wrap txn.  the caller of newTxnMutex must be
// locked to the thread that created txn.
func newTxnMutex(txn *unsafeTxn) *txnMutex {
	mut := &txnMutex{
		t:       txn,
		cs:      make(map[*unsafeCursor]bool),
		_closed: make(chan struct{}),
		topenc:  make(chan *topenc),
		topend:  make(chan *topend),
		tgetval: make(chan *tgetval),
		cgetval: make(chan *cgetval),
		tput:    make(chan *tput),
		cput:    make(chan *cput),
		treset:  make(chan *treset),
		trenew:  make(chan *trenew),
		crenew:  make(chan *crenew),
		tdel:    make(chan *tdel),
		cdel:    make(chan *cdel),
		ccount:  make(chan *ccount),
		cclose:  make(chan *cclose),
		tabort:  make(chan *tabort),
		tcommit: make(chan *tcommit),
		tdrop:   make(chan *tdrop),
	}
	return mut
}

func (txn *txnMutex) Drop(db DBI, del bool) error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.tdrop <- &tdrop{db, del, resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) GetVal(db DBI, key []byte) ([]byte, error) {
	resp := make(chan *data)
	select {
	case <-txn._closed:
		return nil, fmt.Errorf("the transaction was closed")
	case txn.tgetval <- &tgetval{db, key, resp}:
	}
	select {
	case <-txn._closed:
		return nil, fmt.Errorf("the transaction was closed")
	case r := <-resp:
		if r.err != nil {
			return nil, r.err
		}
		return r.v.Bytes(), nil
	}
}

func (txn *txnMutex) CGetVal(c *unsafeCursor, setkey, setval []byte, op uint) (key, val *Val, err error) {
	resp := make(chan *valitem)
	select {
	case <-txn._closed:
		return nil, nil, fmt.Errorf("the transaction was closed")
	case txn.cgetval <- &cgetval{c, setkey, setval, op, resp}:
	}
	select {
	case <-txn._closed:
		return nil, nil, fmt.Errorf("the transaction was closed")
	case r := <-resp:
		if r.err != nil {
			return nil, nil, r.err
		}
		return r.k, r.v, nil
	}
}

func (txn *txnMutex) Put(db DBI, key []byte, val []byte, flags uint) error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.tput <- &tput{db, key, val, flags, resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) CPut(c *unsafeCursor, key, val []byte, flags uint) error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.cput <- &cput{c, key, val, flags, resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) Del(db DBI, key, val []byte) error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.tdel <- &tdel{db, key, val, resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) CDel(c *unsafeCursor, flags uint) error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.cdel <- &cdel{c, flags, resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) CCount(c *unsafeCursor) (uint64, error) {
	resp := make(chan *count)
	select {
	case <-txn._closed:
		return 0, fmt.Errorf("the transaction was closed")
	case txn.ccount <- &ccount{c, resp}:
	}
	select {
	case <-txn._closed:
		return 0, fmt.Errorf("the transaction was closed")
	case r := <-resp:
		if r.err != nil {
			return 0, r.err
		}
		return r.n, nil
	}
}

func (txn *txnMutex) OpenCursor(db DBI) (*unsafeCursor, error) {
	resp := make(chan *cursor)
	select {
	case <-txn._closed:
		return nil, fmt.Errorf("the transaction was closed")
	case txn.topenc <- &topenc{db, resp}:
	}
	select {
	case <-txn._closed:
		return nil, fmt.Errorf("the transaction was closed")
	case r := <-resp:
		if r.err != nil {
			return nil, r.err
		}
		return r.c, nil
	}
}

func (txn *txnMutex) OpenDBI(name string, flags uint) (DBI, error) {
	resp := make(chan *dbi)
	select {
	case <-txn._closed:
		return 0, fmt.Errorf("the transaction was closed")
	case txn.topend <- &topend{name, flags, resp}:
	}
	select {
	case <-txn._closed:
		return 0, fmt.Errorf("the transaction was closed")
	case r := <-resp:
		return r.db, r.err
	}
}

func (txn *txnMutex) CClose(c *unsafeCursor) error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.cclose <- &cclose{c, resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) Commit() error {
	resp := make(chan error)
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case txn.tcommit <- &tcommit{resp}:
	}
	select {
	case <-txn._closed:
		return fmt.Errorf("the transaction was closed")
	case err := <-resp:
		return err
	}
}

func (txn *txnMutex) Abort() {
	resp := make(chan struct{})
	select {
	case <-txn._closed:
		panic("abort on a closed transaction")
	case txn.tabort <- &tabort{resp}:
	}
	select {
	case <-txn._closed:
		panic("abort on a closed transaction")
	case <-resp:
	}
}

func (mut *txnMutex) Loop() {
	defer close(mut._closed)
	for {
		select {
		case req := <-mut.topend:
			db, err := mut.t.OpenDBI(req.name, req.flags)
			if err != nil {
				req.resp <- &dbi{0, err}
				continue
			}
			req.resp <- &dbi{db, nil}
		case req := <-mut.topenc:
			unsafe, err := openUnsafeCursor(mut.t, req.db)
			if err != nil {
				req.resp <- &cursor{nil, err}
				continue
			}
			mut.cs[unsafe] = true
			req.resp <- &cursor{unsafe, nil}
		case req := <-mut.tgetval:
			v, err := mut.t.GetVal(req.db, req.k)
			req.resp <- &data{v, err}
		case req := <-mut.cgetval:
			if !mut.cs[req.c] {
				req.resp <- &valitem{nil, nil, errInvalidCursor}
				continue
			}
			k, v, err := req.c.GetVal(req.k, req.v, req.op)
			req.resp <- &valitem{k, v, err}
		case req := <-mut.tput:
			err := mut.t.Put(req.db, req.k, req.v, req.flags)
			req.resp <- err
		case req := <-mut.cput:
			if !mut.cs[req.c] {
				req.resp <- errInvalidCursor
				continue
			}
			err := req.c.Put(req.k, req.v, req.flags)
			req.resp <- err
		case _ = <-mut.treset:
			panic("unsupported")
		case _ = <-mut.trenew:
			panic("unsupported")
		case _ = <-mut.crenew:
			panic("unsupported")
		case req := <-mut.tdel:
			err := mut.t.Del(req.db, req.k, req.v)
			req.resp <- err
		case req := <-mut.cdel:
			if !mut.cs[req.c] {
				req.resp <- errInvalidCursor
				continue
			}
			err := req.c.Del(req.flags)
			req.resp <- err
		case req := <-mut.ccount:
			if !mut.cs[req.c] {
				req.resp <- &count{0, errInvalidCursor}
				continue
			}
			n, err := req.c.Count()
			req.resp <- &count{n, err}
		case req := <-mut.cclose:
			if !mut.cs[req.c] {
				req.resp <- errInvalidCursor
				continue
			}
			err := req.c.Close()
			req.resp <- err
		case req := <-mut.tabort:
			mut.t.Abort()
			req.resp <- struct{}{}
			mut.cs = make(map[*unsafeCursor]bool)
		case req := <-mut.tcommit:
			err := mut.t.Commit()
			req.resp <- err
			mut.cs = make(map[*unsafeCursor]bool)
		case req := <-mut.tdrop:
			err := mut.t.Drop(req.db, req.del)
			req.resp <- err
		}
	}
}

type valitem struct {
	k, v *Val
	err  error
}
type data struct {
	v   *Val
	err error
}
type cursor struct {
	c   *unsafeCursor
	err error
}
type stat struct {
	s   *Stat
	err error
}
type opendbi struct {
	db  DBI
	err error
}
type count struct {
	n   uint64
	err error
}
type dbi struct {
	db  DBI
	err error
}

type tgetval struct {
	db   DBI
	k    []byte
	resp chan<- *data
}
type cgetval struct {
	c    *unsafeCursor
	k, v []byte
	op   uint
	resp chan<- *valitem
}

type tput struct {
	db    DBI
	k, v  []byte
	flags uint
	resp  chan<- error
}
type cput struct {
	c     *unsafeCursor
	k, v  []byte
	flags uint
	resp  chan<- error
}
type crenew struct {
	c    *unsafeCursor
	resp chan<- error
}
type tdel struct {
	db   DBI
	k, v []byte
	resp chan<- error
}
type topend struct {
	name  string
	flags uint
	resp  chan<- *dbi
}
type topenc struct {
	db   DBI
	resp chan<- *cursor
}
type cdel struct {
	c     *unsafeCursor
	flags uint
	resp  chan<- error
}
type ccount struct {
	c    *unsafeCursor
	resp chan<- *count
}
type cclose struct {
	c    *unsafeCursor
	resp chan<- error
}
type tstat struct {
	db   DBI
	resp chan<- *stat
}
type treset struct {
	resp chan<- struct{}
}
type trenew struct {
	resp chan<- error
}
type tabort struct {
	resp chan<- struct{}
}
type tcommit struct {
	resp chan<- error
}
type tdrop struct {
	db   DBI
	del  bool
	resp chan<- error
}
