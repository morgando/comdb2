#include "build/db.h"
#include "build/db_int.h"
#include "dbinc_auto/os_ext.h"

int __mempro_init(DB_ENV *dbenv) {
	int ret = 0;
	DB_MPRO *mp;
	ret = __os_calloc(dbenv, 1, sizeof(DB_MPRO), &mp);

	if (ret) {
		goto err;
	}

	/* Create transaction map */
	mp->transactions = hash_init_o(offsetof(UTXNID_TRACK, utxnid), sizeof(u_int64_t));
	if (mp->transactions == NULL) {
		ret = ENOMEM;
		goto err;
	}

	Pthread_mutex_init(&mp->mpro_mutexp, NULL);
	dbenv->mpro = mp;
err:
	logmsg(LOGMSG_ERROR, "Failed to initialize mempro\n");
	return ret;
}

int __mempro_destroy(DB_ENV *env) {
	if (env->mpro) {
		Pthread_mutex_destroy(&env->mpro->mpro_mutexp);
		__os_free(env, env->mpro);
	}
	return 0;
}

int __mempro_add_txn(DB_ENV *dbenv, u_int64_t utxnid, DB_LSN commit_lsn) {
	UTXNID_TRACK *txn;
	int ret = 0;
	if (IS_ZERO_LSN(commit_lsn)) {
		return 0;
	}
	ret = __os_malloc(dbenv, sizeof(UTXNID_TRACK), &txn);
	if (ret) {
		return ENOMEM;
	}

	txn->utxnid = utxnid;
	txn->commit_lsn = commit_lsn;
	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	hash_add(dbenv->mpro->transactions, txn);
	Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);

	return 0;
}
