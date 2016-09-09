#include <stdlib.h>
#include <string.h>
#include "compare.h"

/* lmdb_cmp_c
 * This function performs a simple reverse-string-comparison of two MDB_val
 * data.  This is obviously more complicated them simply using the MDB_REVERSE
 * flag, but it demonstrates how to write a simple custom comparison function.
 */
int lmdb_cmp_c(const MDB_val *a, const MDB_val *b) {
	int result;
	if ((result = memcmp((a->mv_data), (b->mv_data), (a->mv_size) < (b->mv_size) ? (a->mv_size) : (b->mv_size)))) {
		return result;
	}
	return a->mv_size - b->mv_size;
}
