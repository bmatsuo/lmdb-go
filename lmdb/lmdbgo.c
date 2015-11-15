/* lmdbgo.c
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb
 * */
#include "lmdb.h"
#include "lmdbgo.h"
#include "_cgo_export.h"

int lmdbgo_mdb_msg_func_proxy(const char *msg, void *ctx) {
    //  wrap msg and call the bridge function exported from lmdb.go.
    lmdbgo_ConstCString s;
    s.p = msg;
    return lmdbgoMDBMsgFuncBridge(s, (size_t)ctx);
}

int lmdbgo_mdb_reader_list(MDB_env *env, size_t ctx) {
    // list readers using a static proxy function that does dynamic dispatch on
    // ctx.
    return mdb_reader_list(env, &lmdbgo_mdb_msg_func_proxy, (void *)ctx);
}

int lmdbgo_mdb_del(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, void *vdata, size_t vn) {
    MDB_val key, val;
    key.mv_size = kn;
    key.mv_data = kdata;
    val.mv_size = vn;
    val.mv_data = vdata;
    return mdb_del(txn, dbi, &key, &val);
}

int lmdbgo_mdb_get(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, MDB_val *val) {
    MDB_val key;
    key.mv_size = kn;
    key.mv_data = kdata;
    return mdb_get(txn, dbi, &key, val);
}

int lmdbgo_mdb_put2(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    key.mv_size = kn;
    key.mv_data = kdata;
    val.mv_size = vn;
    val.mv_data = vdata;
    return mdb_put(txn, dbi, &key, &val, flags);
}

int lmdbgo_mdb_put1(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, MDB_val *val, unsigned int flags) {
    MDB_val key;
    key.mv_size = kn;
    key.mv_data = kdata;
    return mdb_put(txn, dbi, &key, val, flags);
}

int lmdbgo_mdb_cursor_put2(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    key.mv_size = kn;
    key.mv_data = kdata;
    val.mv_size = vn;
    val.mv_data = vdata;
    return mdb_cursor_put(cur, &key, &val, flags);
}

int lmdbgo_mdb_cursor_put1(MDB_cursor *cur, void *kdata, size_t kn, MDB_val *val, unsigned int flags) {
    MDB_val key;
    key.mv_size = kn;
    key.mv_data = kdata;
    return mdb_cursor_put(cur, &key, val, flags);
}

int lmdbgo_mdb_cursor_putmulti(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, size_t vstride, unsigned int flags) {
    MDB_val key, val[2];
    key.mv_size = kn;
    key.mv_data = kdata;
    val[0].mv_size = vstride;
    val[0].mv_data = vdata;
	val[1].mv_data = 0;
	val[1].mv_size = vn;
    return mdb_cursor_put(cur, &key, &val[0], flags);
}

int lmdbgo_mdb_cursor_get1(MDB_cursor *cur, void *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    key->mv_size = kn;
    key->mv_data = kdata;
    return mdb_cursor_get(cur, key, val, op);
}

int lmdbgo_mdb_cursor_get2(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    key->mv_size = kn;
    key->mv_data = kdata;
    val->mv_size = vn;
    val->mv_data = vdata;
    return mdb_cursor_get(cur, key, val, op);
}
