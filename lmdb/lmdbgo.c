/* lmdbgo.c
 * Helper utilities for github.com/bmatsuo/lmdb-go/lmdb
 * */
#include <stdlib.h>
#include <string.h>
#include "lmdb.h"
#include "lmdbgo.h"
#include "_cgo_export.h"

int lmdbgo_mdb_msg_func_proxy(const char *msg, void *ctx) {
    //  wrap msg and call the bridge function exported from lmdb.go.
    lmdbgo_ConstCString s;
    s.p = msg;
    return lmdbgoMDBMsgFuncBridge(s, ctx);
}

int lmdbgo_mdb_reader_list(MDB_env *env, void *ctx) {
    // list readers using a static proxy function that does dynamic dispatch on
    // ctx.
    return mdb_reader_list(env, &lmdbgo_mdb_msg_func_proxy, ctx);
}
