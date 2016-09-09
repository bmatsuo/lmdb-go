#include "compare.h"

int lmdb_cmp_dyn(const MDB_val *a, const MDB_val *b) {
	return lmdbCmpDyn(((lmdb_cmp_t) {a, b}), 2);
}

int lmdb_cmp_go(const MDB_val *a, const MDB_val *b) {
	return lmdbCmp(((lmdb_cmp_t) {a, b}));
}

int lmdb_cmp_c(const MDB_val *a, const MDB_val *b) {
	return -_cmp_baseline(a, b);
}
