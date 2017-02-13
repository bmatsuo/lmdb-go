// +build !race

package lmdbpool

// In general we want Txn objects to be returned to the sync.Pool. But because
// the default behavior of Pool.Put during race detection is to drop everything
// on the floor.  This isn't the end of world, but if the Txn finalizers don't
// don't run fast enough you can end up hitting the environment's limit on
// readers.  This is still not terrible unless you run your benchmarks with
// race detection enabled.  In such cases benchmarks issuing repeated reads
// will quickly blow the environments reader limit.
const returnTxnToPool = true
