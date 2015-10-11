#include <stdlib.h>
#include <string.h>
#include "lmdb.h"
#include "lmdbgo.h"
#include "_cgo_export.h"

int lmdbgo_mdb_msg_func_proxy(const char *msg, void *ctx) {
	ConstCString s;
	s.p = msg;
	return lmdbgo_mdb_msg_func_bridge(s, ctx);
}

int lmdbgo_mdb_reader_list(MDB_env *env, void *ctx) {
    return mdb_reader_list(env, &lmdbgo_mdb_msg_func_proxy, ctx);
}
