#ifndef _MDB_CUSTOM_COMPARE_H
#define _MDB_CUSTOM_COMPARE_H

#include <stdlib.h>
#include <string.h>
#include "lmdb.h"

typedef struct{const MDB_val *a; const MDB_val *b;} lmdb_cmp_t;

extern int lmdbCmp(lmdb_cmp_t cmp);
extern int lmdbCmpDyn(lmdb_cmp_t cmp, size_t ctx);

int lmdb_cmp_dyn(const MDB_val *a, const MDB_val *b);
int lmdb_cmp_go(const MDB_val *a, const MDB_val *b);
int lmdb_cmp_c(const MDB_val *a, const MDB_val *b);

static inline int _cmp_baseline(const MDB_val *a, const MDB_val *b) {
	int result;
	if ((result = memcmp((a->mv_data), (b->mv_data), (a->mv_size) < (b->mv_size) ? (a->mv_size) : (b->mv_size)))) {
		return result;
	}
	return a->mv_size - b->mv_size;
}

#endif
