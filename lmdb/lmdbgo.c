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
    return lmdbgoMDBMsgFuncBridge(s, ctx);
}

int lmdbgo_mdb_get(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, MDB_val *val) {
    MDB_val key;
    key.mv_size = kn;
    key.mv_data = kdata;
    return mdb_get(txn, dbi, &key, val);
}

int lmdbgo_mdb_put(MDB_txn *txn, MDB_dbi dbi, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    key.mv_size = kn;
    key.mv_data = kdata;
    val.mv_size = vn;
    val.mv_data = vdata;
    return mdb_put(txn, dbi, &key, &val, flags);
}

int lmdbgo_mdb_cursor_put(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    key.mv_size = kn;
    key.mv_data = kdata;
    val.mv_size = vn;
    val.mv_data = vdata;
    return mdb_cursor_put(cur, &key, &val, flags);
}

int lmdbgo_mdb_cursor_get1(MDB_cursor *cur, void **kdata, size_t *kn, MDB_val *val, MDB_cursor_op op) {
    int rc;
    MDB_val key;
    key.mv_size = *kn;
    key.mv_data = *kdata;
    rc = mdb_cursor_put(cur, &key, val, op);
    *kdata = key.mv_data;
    *kn = key.mv_size;
    return rc;
}

int lmdbgo_mdb_cursor_get1a(MDB_cursor *cur, void *kdata, size_t kn, MDB_val *val, MDB_cursor_op op) {
    MDB_val key;
    key.mv_size = kn;
    key.mv_data = kdata;
    return mdb_cursor_put(cur, &key, val, op);
}

int lmdbgo_mdb_cursor_get2(MDB_cursor *cur, void **kdata, size_t *kn, void **vdata, size_t *vn, MDB_cursor_op op) {
    int rc;
    MDB_val key, val;
    key.mv_data = *kdata;
    key.mv_size = *kn;
    val.mv_data = *vdata;
    val.mv_size = *vn;
    rc = mdb_cursor_put(cur, &key, &val, op);
    *kdata = key.mv_data;
    *kn = key.mv_size;
    *vdata = val.mv_data;
    *vn = val.mv_size;
    return rc;
}

int lmdbgo_mdb_cursor_get2a(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, MDB_cursor_op op) {
    MDB_val key, val;
    key.mv_data = kdata;
    key.mv_size = kn;
    val.mv_data = vdata;
    val.mv_size = vn;
    return mdb_cursor_put(cur, &key, &val, op);
}

int lmdbgo_mdb_reader_list(MDB_env *env, void *ctx) {
    // list readers using a static proxy function that does dynamic dispatch on
    // ctx.
    return mdb_reader_list(env, &lmdbgo_mdb_msg_func_proxy, ctx);
}
