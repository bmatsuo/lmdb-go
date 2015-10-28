/* lmdbgo.h
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb.
 * */
#ifndef _LMDBGO_H_
#define _LMDBGO_H_

#include "lmdb.h"

int lmdbgo_mdb_get(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, MDB_val *val);
int lmdbgo_mdb_put1(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, MDB_val *val, unsigned int flags);
int lmdbgo_mdb_put2(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags);
int lmdbgo_mdb_cursor_put1(MDB_cursor *cur, void *kdata, size_t kn, MDB_val *val, unsigned int flags);
int lmdbgo_mdb_cursor_put2(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags);
int lmdbgo_mdb_cursor_putmulti(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, size_t vstride, unsigned int flags);
int lmdbgo_mdb_cursor_get1(MDB_cursor *cur, void *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op);
int lmdbgo_mdb_cursor_get2(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, MDB_val *key, MDB_val *val, MDB_cursor_op op);

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
