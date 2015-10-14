package lmdbsync

type testHandler struct {
	called bool
	bag    Bag
	err    error
}

func (h *testHandler) HandleTxnErr(b Bag, err error) (Bag, error) {
	h.called = true
	h.bag = b
	h.err = err
	return b, err
}
