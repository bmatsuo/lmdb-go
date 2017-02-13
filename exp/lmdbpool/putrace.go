//+build race

package lmdbpool

// transactions abort immediately instead of "being put in the pool" when race
// detection is enabled to prevent benchmarks with race enabled from forcing
// applications to allow ridiculously large maximum numbers of readers.
//
// As of go1.8 the sync.Pool implementation never reuses objects during race
// detection.  The special logic which bypasses this requires a similar bypass
// here, unfortunately.
const returnTxnToPool = false
