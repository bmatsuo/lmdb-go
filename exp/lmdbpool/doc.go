/*
Package lmdbpool provides a TxnPool type that allows lmdb.Readonly transactions
to safely be reused by other goroutines when the goroutine that created the
transaction no longer has a use for it.  The TxnPool type has benefits that
would be absent in a naive use of sync.Pool with lmdb.Txn types.

Naively reusing lmdb.Readonly transactions can cause updates to continually
allocate more pages for the database instead of reusing stale pages.  The
TxnPool type tracks transaction ids to make sure that lmdb.Readonly
transactions are not reused when they are known to be holding stale pages which
could be reclaimed by LMDB.

A general downside of pooling lmdb.Readonly transactions using a sync.Pool in
applications with a very high rate of transacions is that the number of readers
in an environment can be significantly higher than the number of goroutines
actively trying to read from that environment.  Because of this it is possible
that applications may need to increase the maximum number of readers allowed in
the environment at initialization time.

	err := env.SetMaxReaders(maxReaders)

In a naive pooling implementation an application compiled with the -race flag
may require a large number of open readers.  The TxnPool type attempts to keep
the value required for Env.SetMaxReaders as low as possible in the presence of
-race but there is a limited amount that can be done for a concurrent workload
with a rapid enough rate of transactions.
*/
package lmdbpool
