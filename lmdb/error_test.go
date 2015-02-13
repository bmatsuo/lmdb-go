package lmdb

import (
	"syscall"
	"testing"
)

func TestErrno(t *testing.T) {
	zeroerr := operrno("testop", 0)
	if zeroerr != nil {
		t.Errorf("errno(0) != nil: %#v", zeroerr)
	}
	syserr := _operrno("testop", int(syscall.EINVAL))
	if syserr.(*OpError).Errno != syscall.EINVAL { // fails if error is Errno(syscall.EINVAL)
		t.Errorf("errno(syscall.EINVAL) != syscall.EINVAL: %#v", syserr)
	}
	mdberr := _operrno("testop", int(KeyExist))
	if mdberr.(*OpError).Errno != KeyExist {
		t.Errorf("errno(ErrKeyExist) != ErrKeyExist: %#v", syserr)
	}
}

func TestIsErrno(t *testing.T) {
	err := NotFound
	if !IsErrno(err, err) {
		t.Errorf("expected match: %v", err)
	}

	operr := &OpError{
		Op:    "testop",
		Errno: err,
	}
	if !IsErrno(operr, err) {
		t.Errorf("expected match: %v", operr)
	}
}
