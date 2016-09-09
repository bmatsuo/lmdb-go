#ifndef _MDB_CUSTOM_COMPARE_H
#define _MDB_CUSTOM_COMPARE_H

#include "lmdb.h"

/* lmdb_cmp_c can be used by Go (after including this header) to set a
 * comparison function for a database.  For the following function to typecheck
 * a copy (or link) of the lmdb.h file used by lmdb-go is needed in the local
 * directory so it may be included.
 * */
int lmdb_cmp_c(const MDB_val *a, const MDB_val *b);

#endif
