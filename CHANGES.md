#Release Change Log

##v1.8.0

- lmdbscan: The package was moved out of the exp/ subtree and can now be
  considered stable and suitable for general use.
- lmdb: Update LMDB C library to version 0.9.19 (#92).

```
	Fix mdb_env_cwalk cursor init (ITS#8424)
	Fix robust mutexes on Solaris 10/11 (ITS#8339)
	Tweak Win32 error message buffer
	Fix MDB_GET_BOTH on non-dup record (ITS#8393)
	Optimize mdb_drop
	Fix xcursors after mdb_cursor_del (ITS#8406)
	Fix MDB_NEXT_DUP after mdb_cursor_del (ITS#8412)
	Fix mdb_cursor_put resetting C_EOF (ITS#8489)
	Fix mdb_env_copyfd2 to return EPIPE on SIGPIPE (ITS#8504)
	Fix mdb_env_copy with empty DB (ITS#8209)
	Fix behaviors with fork (ITS#8505)
	Fix mdb_dbi_open with mainDB cursors (ITS#8542)
	Fix robust mutexes on kFreeBSD (ITS#8554)
	Fix utf8_to_utf16 error checks (ITS#7992)
	Fix F_NOCACHE on MacOS, error is non-fatal (ITS#7682)
	Build
		Make shared lib suffix overridable (ITS#8481)
	Documentation
		Cleanup doxygen nits
		Note reserved vs actual mem/disk usage
```

- lmdb: Fix resource leak in cursor tests (bcf4e9f).
- lmdb: Fix panic in Cursor.Get when using the Set op (#96).
- docs: Improve documentation about when runtime.LockOSThread is required

##v1.7.0

- lmdb: Removed unnecessary import of the "math" package (#70).
- lmdb: Removed direct dependency on the "fmt" package and reduced error
  related allocation (#73).
- cmd/lmdb_stat: Fix transaction ID decoding and match output of `mdb_stat`
  1-to-1 (#78).
- lmdb: fix compilation for 32-bit architectures (#83).

##v1.6.0 (2016-04-07)

- lmdb: method Txn.ID() exposing mdb_txn_id. (#47)
- lmdb: Env.ReaderList() returns an error if passed a nil function. (#48)
- lmdbsync: realistic test of resizing functionality (#7)
- lmdbsync: use context.Context instead of a hand-rolled Bag (#51)
- lmdbsync: Handler Env is now an argument instead of a context value (#52)
- lmdbsync: Changes to MapResizedHandler and its default values (#54)
- lmdb: Fix CGO argument check panic for certain []byte values produced from a
  bytes.Buffer (#56)
- lmdb: Support building the C library with support for the pwritev(2) system
  call (#58)
- lmdb: Reuse MDB_val values within transactions to reduce allocations in
  transactions issuing multiple Get operations (#61).
- lmdb: Avoid allocation and linear scan overhead on the cgo boundary for
  transaction operations (Get/Put and variants) (#63).
- lmdb: Use a more portable internal conversion from C pointers to slices
  (#67).

##v1.5.0

- lmdb: fix crash from bad interaction with Txn finalizer and Txn.Reset/.Renew.
- lmdb: Update the LMDB C library to 0.9.18

```
    Fix robust mutex detection on glibc 2.10-11 (ITS#8330)
    Fix page_search_root assert on FreeDB (ITS#8336)
    Fix MDB_APPENDDUP vs. rewrite(single item) (ITS#8334)
    Fix mdb_copy of large files on Windows
    Fix subcursor move after delete (ITS#8355)
    Fix mdb_midl_shirnk off-by-one (ITS#8363)
    Check for utf8_to_utf16 failures (ITS#7992)
    Catch strdup failure in mdb_dbi_open
    Build
        Additional makefile var tweaks (ITS#8169)
    Documentation
        Add Getting Started page
        Update WRITEMAP description
```

##v1.4.0

- development: The LMDB C library can be cloned under /lmdb -- it will be
  ignored.
- lmdb: Pass CFLAGS -Wno-format-extra-args to silence compilation warning (OS
  X).
- lmdb: Update the LMDB C library to 0.9.17

```
    Fix ITS#7377 catch calloc failure
    Fix ITS#8237 regression from ITS#7589
    Fix ITS#8238 page_split for DUPFIXED pages
    Fix ITS#8221 MDB_PAGE_FULL on delete/rebalance
    Fix ITS#8258 rebalance/split assert
    Fix ITS#8263 cursor_put cursor tracking
    Fix ITS#8264 cursor_del cursor tracking
    Fix ITS#8310 cursor_del cursor tracking
    Fix ITS#8299 mdb_del cursor tracking
    Fix ITS#8300 mdb_del cursor tracking
    Fix ITS#8304 mdb_del cursor tracking
    Fix ITS#7771 fakepage cursor tracking
    Fix ITS#7789 ensure mapsize >= pages in use
    Fix ITS#7971 mdb_txn_renew0() new reader slots
    Fix ITS#7969 use __sync_synchronize on non-x86
    Fix ITS#8311 page_split from update_key
    Fix ITS#8312 loose pages in nested txn
    Fix ITS#8313 mdb_rebalance dummy cursor
    Fix ITS#8315 dirty_room in nested txn
    Fix ITS#8323 dirty_list in nested txn
    Fix ITS#8316 page_merge cursor tracking
    Fix ITS#8321 cursor tracking
    Fix ITS#8319 mdb_load error messages
    Fix ITS#8320 mdb_load plaintext input
    Added mdb_txn_id() (ITS#7994)
    Added robust mutex support
    Miscellaneous cleanup/simplification
    Build
        Create install dirs if needed (ITS#8256)
        Fix ThreadProc decl on Win32/MSVC (ITS#8270)
        Added ssize_t typedef for MSVC (ITS#8067)
        Use ANSI apis on Windows (ITS#8069)
        Use O_SYNC if O_DSYNC,MDB_DSYNC are not defined (ITS#7209)
        Allow passing AR to make (ITS#8168)
        Allow passing mandir to make install (ITS#8169)
```


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
