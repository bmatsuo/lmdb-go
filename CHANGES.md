#Release Change Log

##v1.3.0

- all: Builds on Windows with passing tests. Fixes #33.
- lmdb: Cursor.DBI returns "invalid" DBI if the cursor is closed. Fixes #31.
- lmdb: Finalizers to prevent resource leaks. Fixes #20.
- all: Internal test package for setting up, populating, and tearing down environments.
- lmdbscan: Fix panic in Scanner.Scan after Txn.OpenCursor fails. Fixes #21.
- lmdbscan: Scanner.Set[Next] methods move the cursor and make the next
  Scanner.Scan a noop.  The changes should be backwards compatible. Fixes #17.
- lmdb: Cgo calling convention meets rules set forth for go1.6. Fixes #10.
- lmdb: add a "Package" code example that shows a complete workflow

##v1.2.0

- Many example tests replaced with simpler code examples.
- Lots of documentation fixes
- internal/lmdbcmd: simplify version printing
- lmdbscan: add method Scanner.Cursor() to deprecate Scanner.Del()
- lmdbscan: add tests for Scanner.Set and Scanner.SetNext
- lmdb: Implement Env.FD() method returning an open file descriptor
- lmdbgo.c: remove unnecessary `#include <string.h>`

##v1.1.1

- Lots of code examples.
- Lots of tests.
- Travis-CI enforcing code style using [`golint`](https://github.com/golang/lint)
- exp/lmdbscan: removed the scanner.Func type because it was unnecessary bloat.
- exp/lmdbsync: Tweak lmdbsync.HandlerChain semantics
- exp/lmdbsync: Rename type RetryTxn to ErrTxnRetry
- Move exp/cmd/lmdb_stat to path cmd/lmdb_stat because its purpose is know and
  it is essentially complete.
- Move exp/cmd/lmdb_copy to path cmd/lmdb_stat because its purpose is know and
  it is essentially complete.
- Add method Env.ReaderList using C shim.
- exp/lmdbsync: Simplified interface and behavior after tests.
- exp/lmdbsync: No longer restrict implementations of lmdbsync.Bag with an
  unexported method.
- exp/lmdbsync: Do not let users call Env.BeginTxn because it is
  unsynchronized.
- lmdb: methods Env.CopyFD and Env.CopyFDFlags
- lmdb: clean up Multi.Vals by using Multi.Val internally
- exp/lmdbsync: clean up lmdbsync.MapFullHandler and lmdbsync.MapResizedHandler
  godoc.
- exp/lmdbsync: document possible deadlocks with MapFullHandler and MapResizedHandler
- exp/cmp/lmdb_example_resize: simple program that auto-resizes a database
- exp/lmdbsync: fix infinite loop
- README.md: link fixes
