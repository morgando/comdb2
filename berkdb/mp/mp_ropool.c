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
	return 0;
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

int __mempro_get_commit_lsn_for_txn(DB_ENV *dbenv, u_int64_t utxnid, DB_LSN *commit_lsn) {
	UTXNID_TRACK *txn;
	DB_MPRO *mpro = dbenv->mpro;
	int ret = 0;
	ZERO_LSN(*commit_lsn);

	txn = hash_find(dbenv->mpro->transactions, &utxnid);
	if (txn == NULL) {
		ret = DB_NOTFOUND;
	} else {
		*commit_lsn = txn->commit_lsn;
	}
	return ret;
}

int __mempro_remove_txn(DB_ENV *dbenv, u_int64_t utxnid) {
	UTXNID_TRACK *txn;
	int ret = 0;

	printf("Removing txn with id %"PRIx64" from map\n", utxnid);

	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	txn = hash_find(dbenv->mpro->transactions, &utxnid);
	if (txn) {
		hash_del(dbenv->mpro->transactions, txn);
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
		free(txn); // TODO: os_free
	} else {
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	}
	return ret;
}

int __mempro_add_txn_begin(DB_ENV *dbenv, u_int64_t utxnid) {
	UTXNID_TRACK *txn = malloc(sizeof(UTXNID_TRACK));
	// TODO: ret = __os_malloc(dbenv, sizeof(UTXNID_TRACK), txn);
	if (!txn) {
		return 1;
	}
	txn->utxnid = utxnid;
	txn->in_progress = 1;
	ZERO_LSN(txn->commit_lsn);
	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	hash_add(dbenv->mpro->transactions, txn);
	Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
	return 0;

}

int __mempro_add_txn_commit(DB_ENV *dbenv, u_int64_t utxnid, DB_LSN commit_lsn) {
	if (IS_ZERO_LSN(commit_lsn)) {
		return 0;
	}
	UTXNID_TRACK *txn;
	Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
	txn = hash_find(dbenv->mpro->transactions, &utxnid);
	if (txn) {
		txn->in_progress = 0;
		txn->commit_lsn = commit_lsn;
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
		return 0;
	} else {
		Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);
		return 1;
	}
}
