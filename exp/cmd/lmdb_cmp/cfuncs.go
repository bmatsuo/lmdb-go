package main

/*
#include <stdlib.h>
#include "lmdb.h"

typedef struct{const MDB_val *a; const MDB_val *b;} lmdb_cmp_t;

int lmdb_cmp_dyn(const MDB_val *a, const MDB_val *b) {
	lmdb_cmp_t c = {a, b};
	return lmdbCmpDyn(c);
}

int lmdb_cmp_go(const MDB_val *a, const MDB_val *b) {
	lmdb_cmp_t c = {a, b};
	return lmdbCmp(c);
}

static inline int _cmp_baseline(const MDB_val *a, const MDB_val *b) {
	int result;
	if (result = memcmp((a->mv_data), (b->mv_data), (a->mv_size) < (b->mv_size) ? (a->mv_size) : (b->mv_size))) {
		return result;
	}
	return a->mv_size - b->mv_size;
}

int lmdb_cmp_c(const MDB_val *a, const MDB_val *b) {
	return -_cmp_baseline(a, b);
}
*/
import "C"
