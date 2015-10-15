/* lmdbgo.h
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb.
 * */
#ifndef _LMDBGO_H_
#define _LMDBGO_H_

#include "lmdb.h"

/* ConstCString wraps a null-terminated (const char *) because Go's type system
 * does not represent the 'cosnt' qualifier directly on a function argument and
 * causes warnings to be emitted during linking.
 * */
typedef struct{ const char *p; } lmdbgo_ConstCString;

/* lmdbgo_mdb_reader_list is a proxy for mdb_reader_list that uses a special
 * mdb_msg_func proxy function to relay messages over the
 * lmdbgo_mdb_reader_list_bridge external Go func.
 * */
int lmdbgo_mdb_reader_list(MDB_env *env, void *ctx);

#endif
